//! Durable admission and ordered execution for a gateway-owned session.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct GroupMember {
    pub(super) round: String,
    pub(super) name: String,
    pub(super) display: String,
    pub(super) title: String,
    pub(super) first: bool,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct ChannelExecution {
    pub tools: bool,
    pub interactive: bool,
    #[serde(skip)]
    pub model: Option<String>,
    #[serde(skip)]
    pub policy: hermes_engine::ChannelTurnPolicy,
    #[serde(skip)]
    pub approval_context: Option<messaging::ToolApprovalContext>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Receipt {
    pub(super) id: String,
    pub(super) input_id: Option<String>,
    pub(super) text: String,
    pub(super) attachments: Vec<Attachment>,
    pub(super) model: String,
    pub(super) options: GenerationOptions,
    sequence: u64,
    pub(super) generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_epoch: Option<u64>,
    pub(super) status: String,
    pub(super) outcome: Option<String>,
    pub(super) result: Option<Value>,
    pub(super) turn_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    legacy_turn_hint: Option<usize>,
    pub(super) admitted_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) committed_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) python_selection: Option<python_memory::PythonSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) python_delivery: Option<python_memory::PythonDelivery>,
    #[serde(default)]
    pub(super) unattended: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) channel: Option<ChannelExecution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) capture: Option<PendingMemoryCapture>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) ordinary_chat: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) group: Option<GroupMember>,
}

impl Receipt {
    pub(super) fn value(&self, profile: &str, sid: &str, authority_epoch: u64, include_result: bool) -> Value {
        // The claim remains attributable to its original owner after recovery.
        // Earlier Rust records used epoch 1 and did not serialize it.
        let epoch = self.owner_epoch.or((self.generation > 0).then_some(1)).unwrap_or(authority_epoch);
        let mut result = json!({"admission_id":self.id,"ref":{"profile_id":profile,"session_id":sid},
            "sequence":self.sequence,"status":self.status,"outcome":self.outcome,
            "authority_epoch":epoch,"execution_generation":self.generation});
        if include_result {
            result["result"] = json!(self.result);
            if let Some(delivery) = self.python_delivery {
                result["memory_delivery"] = json!(delivery);
            }
        }
        result
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Authority {
    version: u32,
    pub(super) generation: u64,
    pub(super) receipts: VecDeque<Receipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) source_rewinds: Vec<memory_capture::CommittedRewind>,
    #[serde(default)]
    imported_queue: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    staged: BTreeMap<String,Attachment>,
}

impl Default for Authority {
    fn default() -> Self {
        Self {
            version: 1,
            generation: 0,
            receipts: VecDeque::new(),
            source_rewinds: Vec::new(),
            imported_queue: false,
            staged: BTreeMap::new(),
        }
    }
}

impl Authority {
    pub(super) fn recover(
        writer: &ConversationWriter,
        dir: &Path,
        settings: &Settings,
        turns: usize,
    ) -> Result<Self> {
        let mut authority: Self = writer
            .authority()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        ensure!(
            authority.version == 1,
            "Unsupported gateway admission record"
        );
        let mut changed = false;
        authority.generation = authority
            .generation
            .max(settings.execution_generation)
            .max(turns as u64);
        for receipt in &mut authority.receipts {
            // Older migration records stored an unverified desktop index in the committed slot.
            if receipt.admitted_ns == 0 && receipt.turn_index.is_some()
                && receipt.input_id.as_deref().is_some_and(|id| id.starts_with("legacy-queue-v1-"))
                && (receipt.status == "unknown" || receipt.result.as_ref().is_some_and(|v| v["unknown_execution"] == true))
            {
                receipt.legacy_turn_hint = receipt.turn_index.take();
                changed = true;
            }
            if receipt.status == "started" {
                receipt.status = "unknown".into();
                changed = true;
            }
        }
        if !authority.imported_queue {
            match fs::read(dir.join("queued-prompts.json")) {
                Ok(bytes) => {
                    authority.import_legacy(serde_json::from_slice(&bytes)?, settings)?;
                    authority.imported_queue = true;
                    changed = true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if changed {
            authority.persist(writer, None)?;
        }
        Ok(authority)
    }

    fn import_legacy(&mut self, queue: LegacyQueue, settings: &Settings) -> Result<()> {
        let pending = queue
            .in_flight
            .into_iter()
            .map(|claim| (claim.entry, Some(claim.turn_index)))
            .chain(queue.entries.into_iter().map(|entry| (entry, None)));
        for (entry, turn_index) in pending {
            Attachment::validate_set(&entry.attachments)?;
            self.receipts.push_back(Receipt {
                id: new_id(),
                input_id: Some(format!("legacy-queue-v1-{}", entry.id)),
                sequence: self.receipts.len() as u64 + 1,
                text: entry.text,
                attachments: entry.attachments,
                options: settings.generation_options.clone(),
                model: settings.model.clone().unwrap_or_default(),
                generation: if turn_index.is_some() {
                    self.generation
                } else {
                    0
                },
                owner_epoch: turn_index.is_some().then_some(1),
                status: if turn_index.is_some() {
                    "unknown"
                } else {
                    "parked"
                }
                .into(),
                outcome: None,
                result: None,
                turn_index: None,
                legacy_turn_hint: turn_index,
                admitted_ns: 0,
                committed_ns: None,
                python_selection: None,
                python_delivery: None,
                unattended: false,
                channel: None,
                ordinary_chat: false,
                capture: None,
                group: None,
            });
        }
        Ok(())
    }

    pub(super) fn pending(&self, profile: &str, sid: &str, authority_epoch: u64) -> Value {
        json!(
            self.receipts
                .iter()
                .filter(|item| item.status != "terminal")
                .map(|item| {
                    let mut row = item.value(profile, sid, authority_epoch, false);
                    row["text"] = json!(item.text);
                    row["turn_index"] = json!(item.turn_index);
                    if let Some(hint) = item.legacy_turn_hint { row["legacy_turn_index"] = json!(hint); }
                    row["model"] = json!(item.model);
                    row["attachments"] = json!(item.attachments);
                    row["submission_id"] = json!(item.input_id);
                    row
                })
                .collect::<Vec<_>>()
        )
    }

    pub(super) fn persist(
        &self,
        writer: &ConversationWriter,
        history: Option<&History>,
    ) -> Result<()> {
        writer.commit(history, &serde_json::to_value(self)?)
    }

    pub(super) fn cancel_group(&mut self, round: &str) {
        for receipt in &mut self.receipts {
            if receipt.status == "queued" && receipt.group.as_ref().is_some_and(|group| group.round == round) {
                receipt.status = "terminal".into();
                receipt.outcome = Some("cancelled".into());
                receipt.result = Some(json!({"status":"cancelled","reason":"A prior group member did not complete"}));
            }
        }
    }

    fn claim(&mut self, turns: usize, authority_epoch: u64) -> Option<Receipt> {
        let head = self
            .receipts
            .iter_mut()
            .find(|item| item.status != "terminal")?;
        if head.status != "queued" {
            return None;
        }
        self.generation += 1;
        head.generation = self.generation;
        head.owner_epoch = Some(authority_epoch);
        head.status = "started".into();
        head.turn_index = Some(turns);
        if head.admitted_ns == 0 { head.admitted_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0); }
        Some(head.clone())
    }
}

#[derive(Deserialize)]
struct LegacyQueue {
    entries: Vec<LegacyEntry>,
    in_flight: Option<LegacyClaim>,
}
#[derive(Deserialize)]
struct LegacyClaim {
    entry: LegacyEntry,
    turn_index: usize,
}
#[derive(Deserialize)]
struct LegacyEntry {
    id: u64,
    text: String,
    #[serde(default)]
    attachments: Vec<Attachment>,
}

enum AdmissionOrigin {
    Chat,
    Background { unattended: bool },
    Channel,
    Telegram,
}

struct SubmissionFence {
    instance: String,
    revision: u64,
    generation: u64,
}

impl Shared {
    pub(super) async fn attach_file(&self, params: &Value, peer: &Peer) -> RpcResult {
        let session = self.attached(params, peer).await?;
        let name = required_string(params, "name")?.to_owned();
        let attachment = if let Some(descriptor) = params.get("staged_upload").filter(|v| !v.is_null()) {
            let source = upload::resolve_staged(self.library.root(),descriptor).map_err(|e| RpcError::invalid(e.to_string()))?;
            let mut snapshot = Attachment::capture(&session.dir,&source).map_err(|e| RpcError::invalid(e.to_string()))?;
            snapshot.name = name;
            snapshot
        } else {
            Attachment::capture_data_url(&session.dir,name,required_string(params,"data_url")?).map_err(|e| RpcError::invalid(e.to_string()))?
        };
        let path = attachment.snapshot_path(&session.dir).map_err(|e| RpcError::invalid(e.to_string()))?;
        let key = path.to_string_lossy().into_owned();
        let mut view = lock(&session.view);
        if view.authority.staged.get(&key) != Some(&attachment) {
            let mut next = view.authority.clone();
            next.staged.insert(key.clone(),attachment.clone());
            next.persist(&session.writer,None).map_err(RpcError::internal)?;
            view.authority = next;
        }
        Ok(json!({"attached":true,"uploaded":true,"path":path,"name":attachment.name,
            "ref_text":format!("@file:{key}"),"attachment":attachment}))
    }

    pub(super) async fn submit(self: &Arc<Self>, params: &Value, peer: &Peer) -> RpcResult {
        reject_options(
            params,
            &[
                "api_text",
                "model_override",
                "turn_author",
                "voice_context",
                "voice_turn",
                "truncate_before_row_id",
                "truncate_before_user_ordinal",
                "truncate_before_message_id",
                "confirm_truncate",
                "display_kind",
            ],
        )?;
        let text = required_string(params, "text")?.trim().to_owned();
        let session = self.attached(params,peer).await?;
        let mut attachments = Vec::new();
        {
            let view = lock(&session.view);
            if let Some(raw) = params.get("attachments").filter(|v| !v.is_null()) {
                for item in raw.as_array().ok_or_else(||RpcError::invalid("attachments must be an array"))? {
                    let attachment = if let Some(path) = item["path"].as_str() {
                        let staged = view.authority.staged.get(path).ok_or_else(||RpcError::invalid("File was not staged in this session"))?;
                        if item["mime"].as_str().is_some_and(|mime| mime!=staged.media_type) { return Err(RpcError::invalid("Mismatched attachment type")); }
                        staged.clone()
                    } else {
                        serde_json::from_value(item.clone()).map_err(|e| RpcError::invalid(e.to_string()))?
                    };
                    attachments.push(attachment);
                }
            }
            // Stock composers send ordinary files as standalone @file lines
            // and also send explicit {path,mime} references for images.
            for path in text.lines().filter_map(|line| line.trim().strip_prefix("@file:")) {
                if let Some(staged) = view.authority.staged.get(path) && !attachments.contains(staged) { attachments.push(staged.clone()); }
            }
        }
        Attachment::validate_set(&attachments).map_err(|e| RpcError::invalid(e.to_string()))?;
        if (text.is_empty() && attachments.is_empty()) || text.len() > MAX_PROMPT_BYTES {
            return Err(RpcError::invalid(
                "Enter a message or a staged attachment within the prompt limit",
            ));
        }
        let input = optional_string(params, "submission_id")?
            .or(optional_string(params, "input_id")?)
            .map(str::to_owned);
        if matches!(&input,Some(s) if s.is_empty() || s.len()>256) {
            return Err(RpcError::invalid(
                "submission identity must contain 1-256 bytes",
            ));
        }
        let queue = params["queued"].as_bool().unwrap_or(false);
        if queue && !peer.canonical {
            return Err(RpcError::unsupported(
                "Queueing requires the native gateway protocol",
            ));
        }
        lock(&session.peers).insert(peer.id.clone(), peer.clone());
        let fence = if let Some(instance) = params.get("expected_instance_id") {
            Some(SubmissionFence {
                instance: instance.as_str().filter(|s| !s.is_empty()).ok_or_else(|| RpcError::invalid("Invalid expected_instance_id"))?.into(),
                revision: params["expected_revision"].as_u64().ok_or_else(|| RpcError::invalid("expected_revision is required"))?,
                generation: params["expected_generation"].as_u64().ok_or_else(|| RpcError::invalid("expected_generation is required"))?,
            })
        } else { None };
        let receipt = self.admit(session, text, attachments, input, queue, AdmissionOrigin::Chat, fence).await?;
        if peer.canonical { Ok(receipt) } else { Ok(json!({"status":"streaming","admission_id":receipt["admission_id"]})) }
    }

    pub(crate) async fn submit_background(self: &Arc<Self>, session: Arc<LiveSession>, text: String, submission_id: String, unattended: bool) -> RpcResult {
        if text.trim().is_empty() || text.len() > MAX_PROMPT_BYTES || submission_id.is_empty() || submission_id.len() > 256 {
            return Err(RpcError::invalid("Invalid background submission"));
        }
        self.admit(session, text, Vec::new(), Some(submission_id), true, AdmissionOrigin::Background { unattended }, None).await
    }

    pub(super) async fn submit_channel(
        self: &Arc<Self>, session: Arc<LiveSession>, text: String, attachments: Vec<Attachment>, id: String,
    ) -> RpcResult {
        Attachment::validate_set(&attachments).map_err(|error| RpcError::invalid(error.to_string()))?;
        if (text.trim().is_empty() && attachments.is_empty()) || text.len() > MAX_PROMPT_BYTES {
            return Err(RpcError::invalid("Invalid channel input"));
        }
        self.admit(session, text, attachments, Some(id), true, AdmissionOrigin::Channel, None).await
    }

    pub(super) fn claim_channel(&self, session: &LiveSession, id: &str, permissions: ChannelExecution) -> Result<Option<Receipt>> {
        ensure!(lock(&session.settings).channel_route.is_some(), "Expected a channel session");
        self.claim_pending(session, Some((id, permissions)))
    }

    /// Settle a withdrawn channel input without claiming a turn. A running
    /// receipt must settle through its own worker.
    pub(super) fn retire_channel_submission(&self, session: &LiveSession, id: &str) -> Result<bool> {
        ensure!(lock(&session.settings).channel_route.is_some(), "Expected a channel session");
        let mut view = lock(&session.view);
        let mut next = view.authority.clone();
        let Some(receipt) = next.receipts.iter_mut().find(|receipt| receipt.input_id.as_deref() == Some(id)) else {
            return Ok(true);
        };
        match receipt.status.as_str() {
            "queued" => {
                receipt.status = "terminal".into();
                receipt.outcome = Some("cancelled".into());
                receipt.result = Some(json!({"status":"cancelled","reason":"Channel source authorization changed"}));
            }
            "terminal" => return Ok(true),
            _ => return Ok(false),
        }
        next.persist(&session.writer, None)?;
        view.authority = next;
        view.revision += 1;
        Ok(true)
    }

    pub(super) async fn submit_telegram(self: &Arc<Self>, session: Arc<LiveSession>, text: String, submission_id: String) -> RpcResult {
        if text.trim().is_empty() || text.len() > MAX_PROMPT_BYTES {
            return Err(RpcError::invalid("Invalid Telegram submission"));
        }
        self.admit(session, text, Vec::new(), Some(submission_id), true, AdmissionOrigin::Telegram, None).await
    }

    pub(super) fn claim_telegram_turn(&self, session: &LiveSession) -> Result<Option<Receipt>> {
        ensure!(lock(&session.settings).is_telegram_session(), "Expected a Telegram conversation");
        self.claim_pending(session, None)
    }

    pub(crate) fn submission_receipt(&self, session: &LiveSession, submission_id: &str) -> Option<Value> {
        let sid = lock(&session.summary).id.clone();
        lock(&session.view).authority.receipts.iter().find(|r|r.input_id.as_deref()==Some(submission_id)).map(|r|r.value(&self.profile_id, &sid, self.authority_epoch, true))
    }

    /// Atomically publish a completed producer turn and its source identity.
    /// Returns false when the conversation is busy. The callback runs once the
    /// turn can be written, so the producer can persist its destination first.
    pub(crate) fn try_publish_background_turn(
        &self,
        session: &LiveSession,
        source: &str,
        turn: &hermes_engine::Turn,
        before_publish: impl FnOnce() -> Result<()>,
    ) -> Result<bool> {
        let Ok(mut engine) = session.engine.try_lock() else { return Ok(false); };
        let mut view = lock(&session.view);
        let source = format!("producer-result:{source}");
        let payload = serde_json::to_value(turn)?;
        if let Some(existing) = view.authority.receipts.iter().find(|r| r.input_id.as_deref() == Some(&source)) {
            ensure!(existing.result.as_ref() == Some(&payload), "Producer identity reused for another result");
            before_publish()?;
            return Ok(true);
        }
        if view.running || view.authority.receipts.iter().any(|r| r.status != "terminal") { return Ok(false); }
        before_publish()?;
        let mut history = view.history.clone();
        let turn_index = history.turns.len();
        history.turns.push(turn.clone());
        let mut authority = view.authority.clone();
        authority.generation += 1;
        authority.receipts.push_back(Receipt {
            id: new_id(), input_id: Some(source), text: turn.prompt.clone(), attachments: turn.attachments.clone(),
            model: turn.model.clone(), options: GenerationOptions::default(), sequence: authority.receipts.len() as u64 + 1,
            generation: authority.generation, owner_epoch: Some(self.authority_epoch), status: "terminal".into(), outcome: Some("published".into()),
            result: Some(payload), turn_index: Some(turn_index), admitted_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0), unattended: true, channel: None,
            committed_ns: None, python_selection: None, python_delivery: None,
            capture: None, ordinary_chat: false, legacy_turn_hint: None, group: None,
        });
        authority.persist(&session.writer, Some(&history))?;
        engine.accept_committed_history(history.clone());
        view.history = history;
        view.generation = authority.generation;
        view.revision += 1;
        view.authority = authority;
        Ok(true)
    }

    async fn admit(self: &Arc<Self>, session: Arc<LiveSession>, text:String, attachments:Vec<Attachment>, input:Option<String>, queue: bool, origin: AdmissionOrigin, fence: Option<SubmissionFence>) -> RpcResult {
        let channel = matches!(origin, AdmissionOrigin::Channel);
        let telegram = matches!(origin, AdmissionOrigin::Telegram);
        let (session_is_channel, session_is_telegram) = {
            let settings = lock(&session.settings);
            (settings.channel_route.is_some(), settings.is_telegram_session())
        };
        if channel != session_is_channel {
            return Err(RpcError::invalid("This channel conversation accepts messages through its owning adapter"));
        }
        if telegram != session_is_telegram {
            return Err(RpcError::invalid("Telegram conversations only accept messages from their connector"));
        }
        let summary = lock(&session.summary).clone();
        let bot_names = match summary.bot.as_deref() {
            Some(name) if !name.starts_with("group:") => vec![name.to_owned()],
            Some(name) if name.starts_with("group:") => self.library.groups().map_err(RpcError::internal)?
                .into_iter().find(|group| group.id == name.trim_start_matches("group:"))
                .map(|group| group.members).unwrap_or_default(),
            _ => Vec::new(),
        };
        let _bot_guards = self.bot_read_guards(&bot_names).await;
        if let Some(name) = summary.bot.as_deref().filter(|name| !name.starts_with("group:"))
            && !self.library.bots().map_err(RpcError::internal)?.iter().any(|bot| bot.name == name)
        {
            return Err(RpcError::new(4004, "not_found", "This bot no longer exists"));
        }
        let sid = summary.id.clone();
        let config = lock(&session.config).clone();
        let (options, accepts_profile_memory) = {
            let settings = lock(&session.settings);
            (settings.generation_options.clone(), settings.accepts_profile_memory())
        };
        let ordinary_chat = matches!(origin, AdmissionOrigin::Chat) && summary.bot.is_none()
            && accepts_profile_memory && self.selected_memory().await.native();
        let python_selection = if matches!(origin, AdmissionOrigin::Chat)
            && attachments.is_empty()
            && hermes_engine::references::parse(&text).is_empty()
            && !hermes_engine::memory_capture::contains_inline_data(&text)
            && (config.api_key.is_empty() || !text.contains(&config.api_key))
            && self.python_source_is_active(&summary, &lock(&session.settings))
        {
            self.python_selection_at_admission()
        } else {
            None
        };
        let mut candidate = Receipt {
            id: new_id(),
            input_id: input,
            text,
            attachments,
            options,
            model: config.model.clone(),
            sequence: 0,
            generation: 0,
            owner_epoch: None,
            status: "queued".into(),
            outcome: None,
            result: None,
            turn_index: None,
            legacy_turn_hint: None,
            admitted_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            committed_ns: None,
            python_selection,
            python_delivery: None,
            unattended: telegram || matches!(origin, AdmissionOrigin::Background { unattended: true }),
            channel: None,
            // Freeze native memory admission at submission. REL access also checks the binding.
            ordinary_chat,
            capture: None,
            group: None,
        };
        let first = {
            let _admissions = lock(&self.admissions);
            let settings = lock(&session.settings);
            let config = lock(&session.config);
            let mut view = lock(&session.view);
            if let Some(input) = &candidate.input_id
                && let Some(existing) = view
                    .authority
                    .receipts
                    .iter()
                    .find(|r| r.input_id.as_ref() == Some(input))
            {
                if existing.text != candidate.text || existing.attachments != candidate.attachments || existing.unattended != candidate.unattended
                {
                    return Err(RpcError::new(
                        4001,
                        "admission_conflict",
                        "Submission identity was reused with a different payload",
                    ));
                }
                return Ok(existing.value(&self.profile_id, &sid, self.authority_epoch, false));
            }
            if self.shutdown.is_cancelled() {
                return Err(RpcError::new(4001, "shutdown", "Gateway is shutting down"));
            }
            if let Some(fence) = &fence {
                if fence.instance != self.instance_id {
                    return Err(RpcError::fenced("owner_changed", "The gateway restarted; reconnect before sending", &view));
                }
                if fence.revision != view.revision || fence.generation != view.generation {
                    return Err(RpcError::fenced("revision_conflict", "Session settings changed; reconnect before sending", &view));
                }
            }
            // Snapshot settings while holding the same locks as session.mutate.
            // A concurrent model change must not slip between fencing and admission.
            candidate.model = config.model.clone();
            candidate.options = settings.generation_options.clone();
            let pending = view
                .authority
                .receipts
                .iter()
                .filter(|r| r.status != "terminal")
                .count();
            if pending > 0 && !queue {
                return Err(RpcError::fenced(
                    "session_busy",
                    "Session has pending work",
                    &view,
                ));
            }
            let members = if let Some(id) = summary.bot.as_deref().and_then(|bot| bot.strip_prefix("group:")) {
                let group = self.library.groups().map_err(RpcError::internal)?.into_iter().find(|group| group.id == id)
                    .ok_or_else(|| RpcError::new(4004, "not_found", "Group no longer exists"))?;
                if group.members.is_empty() { return Err(RpcError::invalid("Group has no members")); }
                let bots = self.library.bots().map_err(RpcError::internal)?;
                let mut members = Vec::new();
                for name in group.members {
                    let bot = bots.iter().find(|bot| bot.name == name).ok_or_else(|| RpcError::invalid("Group member no longer exists"))?;
                    members.push((name, bot.display_name.clone()));
                }
                members
            } else { Vec::new() };
            if pending + members.len().max(1) > 51 {
                return Err(RpcError::new(4001, "queue_full", "Session queue is full"));
            }
            config
                .validate()
                .map_err(|e| RpcError::new(4001, "provider_configuration", e.to_string()))?;
            for file in &candidate.attachments {
                file.validate_snapshot(&session.dir)
                    .map_err(|e| RpcError::invalid(e.to_string()))?;
            }
            let mut next = view.authority.clone();
            candidate.sequence = next.receipts.len() as u64 + 1;
            if let Some((name, display)) = members.first() {
                candidate.group = Some(GroupMember { round: candidate.id.clone(), name: name.clone(), display: display.clone(), title: summary.title.clone(), first: true });
            }
            next.receipts.push_back(candidate.clone());
            for (name, display) in members.into_iter().skip(1) {
                let mut child = candidate.clone();
                child.id = new_id();
                child.input_id = Some(format!("group-child:{}:{}", candidate.id, next.receipts.len()));
                child.text = format!("\u{2063}{display}\u{2063}@{display} — your turn. Reply to the group's last message in your own voice; the earlier replies above are from the other members.");
                child.attachments.clear();
                child.sequence = next.receipts.len() as u64 + 1;
                child.group = Some(GroupMember { round: candidate.id.clone(), name, display, title: summary.title.clone(), first: false });
                next.receipts.push_back(child);
            }
            let claim = if view.running || channel || telegram {
                None
            } else {
                next.claim(view.history.turns.len(), self.authority_epoch)
            };
            next.persist(&session.writer, None)
                .map_err(RpcError::internal)?;
            view.authority = next;
            view.revision += 1;
            if let Some(receipt) = &claim {
                if receipt.id == candidate.id {
                    candidate = receipt.clone();
                }
                Self::begin(&mut view, receipt, self.shutdown.child_token());
            }
            claim
        };
        if let Some(receipt) = first {
            self.spawn_worker(session.clone(), receipt).await;
        } else {
            self.emit(&session, "session.info", self.info(&session));
        }
        Ok(candidate.value(&self.profile_id, &sid, self.authority_epoch, false))
    }

    fn begin(view: &mut View, receipt: &Receipt, cancel: CancellationToken) {
        view.generation = receipt.generation;
        view.running = true;
        view.started = Some(now());
        view.cancel = Some(cancel);
        view.current_admission = Some(receipt.id.clone());
        view.corrections = Some(async_channel::bounded(16));
        view.correction_receipts.clear();
    }

    pub(super) fn claim_next(&self, session: &LiveSession) -> Result<Option<Receipt>> {
        let connector_owned = {
            let settings = lock(&session.settings);
            settings.channel_route.is_some() || settings.is_telegram_session()
        };
        if connector_owned {
            return Ok(None);
        }
        self.claim_pending(session, None)
    }

    fn claim_pending(&self, session: &LiveSession, channel: Option<(&str, ChannelExecution)>) -> Result<Option<Receipt>> {
        let mut view = lock(&session.view);
        if view.running || self.shutdown.is_cancelled() {
            return Ok(None);
        }
        let mut next = view.authority.clone();
        if let Some((key, permissions)) = channel {
            let Some(head) = next.receipts.iter_mut().find(|receipt| receipt.status != "terminal") else {
                return Ok(None);
            };
            if head.status != "queued" || head.input_id.as_deref() != Some(key) {
                return Ok(None);
            }
            if let Some(model) = &permissions.model {
                head.model.clone_from(model);
            }
            head.channel = Some(permissions);
        }
        let Some(claim) = next.claim(view.history.turns.len(), self.authority_epoch) else {
            return Ok(None);
        };
        next.persist(&session.writer, None)?;
        view.authority = next;
        view.revision += 1;
        Self::begin(&mut view, &claim, self.shutdown.child_token());
        Ok(Some(claim))
    }

    pub(super) async fn spawn_worker(self: &Arc<Self>, session: Arc<LiveSession>, first: Receipt) {
        let mut tasks = self.turns.lock().await;
        tasks.retain(|t| !t.is_finished());
        let owner = self.clone();
        tasks.push(tokio::spawn(async move {
            let mut admission = first;
            loop {
                let config = lock(&session.config).clone();
                let cancel = lock(&session.view)
                    .cancel
                    .clone()
                    .expect("claimed turn has cancellation token");
                if !owner
                    .clone()
                    .run_turn(session.clone(), config, admission, cancel)
                    .await
                {
                    break;
                }
                match owner.claim_next(&session) {
                    Ok(Some(next)) => admission = next,
                    Ok(None) => break,
                    Err(error) => {
                        owner.emit(
                            &session,
                            "error",
                            json!({"message":format!("Could not claim queued turn: {error:#}")}),
                        );
                        break;
                    }
                }
            }
        }));
    }

    pub(super) async fn recover_queued(self: &Arc<Self>) -> Result<()> {
        for summary in self.library.authority_sessions()? {
            let settings = Settings::read(&summary.directory)?;
            if settings.channel_route.is_some() || settings.is_telegram_session() {
                continue;
            }
            // Only acquire the session lock for work owned by this gateway.
            let (_, authority) = hermes_engine::read_conversation(&summary.directory)?;
            let authority: Option<Authority> = authority.map(serde_json::from_value).transpose()?;
            if !authority.is_some_and(|a| {
                a.receipts
                    .iter()
                    .find(|r| r.status != "terminal")
                    .is_some_and(|r| r.status == "queued")
            }) {
                continue;
            }
            let session = self
                .session(&summary.id)
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?;
            if let Some(first) = self.claim_next(&session)? {
                self.spawn_worker(session, first).await;
            }
        }
        Ok(())
    }

    pub(super) async fn resume_parked(self: &Arc<Self>, params: &Value, peer: &Peer) -> RpcResult {
        if !peer.canonical {
            return Err(RpcError::unsupported("A native session is required"));
        }
        let session = self.attached(params, peer).await?;
        let summary = lock(&session.summary).clone();
        let bot_names = match summary.bot.as_deref() {
            Some(name) if !name.starts_with("group:") => vec![name.to_owned()],
            Some(name) if name.starts_with("group:") => self.library.groups().map_err(RpcError::internal)?
                .into_iter().find(|group| group.id == name.trim_start_matches("group:"))
                .map(|group| group.members).unwrap_or_default(),
            _ => Vec::new(),
        };
        let _bot_guards = self.bot_read_guards(&bot_names).await;
        if let Some(name) = summary.bot.as_deref().filter(|name| !name.starts_with("group:"))
            && !self.library.bots().map_err(RpcError::internal)?.iter().any(|bot| bot.name == name)
        {
            return Err(RpcError::new(4004, "not_found", "This bot no longer exists; the prompt remains parked"));
        }
        let sid = summary.id.clone();
        let id = required_string(params, "admission_id")?;
        let result = {
            let mut view = lock(&session.view);
            let mut next = view.authority.clone();
            let index = next.receipts.iter().position(|receipt| receipt.id == id)
                .ok_or_else(|| RpcError::new(4004, "not_found", "No such admission"))?;
            if next.receipts[index].status != "parked" {
                return Ok(next.receipts[index].value(&self.profile_id, &sid, self.authority_epoch, false));
            }
            let mut root = next.receipts[index].clone();
            root.status = "queued".into();
            if root.model.is_empty() { root.model = self.provider.model.clone(); }
            let mut children = Vec::new();
            if let Some(group_id) = summary.bot.as_deref().and_then(|bot| bot.strip_prefix("group:"))
                && root.group.is_none() {
                let group = self.library.groups().map_err(RpcError::internal)?.into_iter().find(|group| group.id == group_id)
                    .ok_or_else(|| RpcError::new(4004, "not_found", "Group no longer exists; the prompt remains parked"))?;
                if group.members.is_empty() { return Err(RpcError::invalid("Group has no members; the prompt remains parked")); }
                let bots = self.library.bots().map_err(RpcError::internal)?;
                let mut members = Vec::new();
                for name in group.members {
                    let bot = bots.iter().find(|bot| bot.name == name).ok_or_else(|| RpcError::invalid("Group member no longer exists; the prompt remains parked"))?;
                    members.push((name, bot.display_name.clone()));
                }
                let pending = next.receipts.iter().filter(|receipt| receipt.status != "terminal").count();
                if pending + members.len() - 1 > 51 { return Err(RpcError::new(4001, "queue_full", "Group round exceeds the queue limit; the prompt remains parked")); }
                if next.receipts.iter().skip(index + 1).any(|receipt| receipt.status == "terminal") {
                    return Err(RpcError::unsupported("Cannot expand this legacy group prompt without changing completed receipts; the prompt remains parked"));
                }
                let (name, display) = members.remove(0);
                root.group = Some(GroupMember { round: root.id.clone(), name, display, title: summary.title.clone(), first: true });
                for (offset, (name, display)) in members.into_iter().enumerate() {
                    let mut child = root.clone();
                    child.id = new_id();
                    child.input_id = Some(format!("group-child:{}:legacy-{}", root.id, offset + 1));
                    child.text = format!("\u{2063}{display}\u{2063}@{display} — your turn. Reply to the group's last message in your own voice; the earlier replies above are from the other members.");
                    child.attachments.clear();
                    child.group = Some(GroupMember { round: root.id.clone(), name, display, title: summary.title.clone(), first: false });
                    children.push(child);
                }
            }
            next.receipts[index] = root.clone();
            let expanded = !children.is_empty();
            for (offset, child) in children.into_iter().enumerate() { next.receipts.insert(index + 1 + offset, child); }
            // Pending positions may shift; completed receipt sequences stay fixed.
            if expanded {
                let mut sequence = root.sequence;
                for receipt in next.receipts.iter_mut().skip(index + 1) { sequence += 1; receipt.sequence = sequence; }
            }
            let result = root.value(&self.profile_id, &sid, self.authority_epoch, false);
            next.persist(&session.writer, None)
                .map_err(RpcError::internal)?;
            view.authority = next;
            view.revision += 1;
            result
        };
        if let Some(first) = self.claim_next(&session).map_err(RpcError::internal)? {
            self.spawn_worker(session.clone(), first).await;
        }
        self.emit(&session, "session.info", self.info(&session));
        Ok(result)
    }

    pub(super) async fn resolve_unknown(
        self: &Arc<Self>,
        params: &Value,
        peer: &Peer,
    ) -> RpcResult {
        if !peer.canonical {
            return Err(RpcError::unsupported(
                "Resolving unknown work requires a native session",
            ));
        }
        let session = self.attached(params, peer).await?;
        let sid = lock(&session.summary).id.clone();
        let id = required_string(params, "admission_id")?;
        let generation = params["execution_generation"]
            .as_u64()
            .ok_or_else(|| RpcError::invalid("execution_generation is required"))?;
        let result = {
            let mut view = lock(&session.view);
            let mut next = view.authority.clone();
            let receipt = next
                .receipts
                .iter_mut()
                .find(|r| r.id == id)
                .ok_or_else(|| RpcError::new(4004, "not_found", "No such admission"))?;
            if receipt.generation != generation {
                return Err(RpcError::fenced(
                    "stale_generation",
                    "Admission generation has changed",
                    &view,
                ));
            }
            if receipt.status == "terminal"
                && receipt
                    .result
                    .as_ref()
                    .is_some_and(|v| v["unknown_execution"] == true)
            {
                return Ok(receipt.value(&self.profile_id, &sid, self.authority_epoch, true));
            }
            if receipt.status != "unknown" {
                return Err(RpcError::new(
                    4001,
                    "invalid_state",
                    "The admission is not unknown",
                ));
            }
            let mut engine = session.engine.try_lock().map_err(|_| RpcError::fenced("session_busy","Owner is still using the engine",&view))?;
            let mut history = engine.history().clone();
            if let Some(turn) = receipt.turn_index.and_then(|n| history.turns.get_mut(n))
                && turn.status == Status::Running
            {
                turn.status = Status::Interrupted;
            }
            receipt.status = "terminal".into();
            receipt.outcome = Some("interrupted".into());
            receipt.result = Some(
                json!({"status":"interrupted","unknown_execution":true,"text":"","partial":true}),
            );
            let result = receipt.value(&self.profile_id, &sid, self.authority_epoch, true);
            let round = receipt.group.as_ref().map(|group| group.round.clone());
            if let Some(round) = round { next.cancel_group(&round); }
            next.persist(&session.writer, Some(&history))
                .map_err(RpcError::internal)?;
            engine.accept_committed_history(history.clone());
            view.history = history;
            view.authority = next;
            view.revision += 1;
            result
        };
        self.emit(&session, "session.info", self.info(&session));
        if let Some(first) = self.claim_next(&session).map_err(RpcError::internal)? {
            self.spawn_worker(session, first).await;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[tokio::test]
    async fn selected_model_admission_is_fenced_and_retryable() {
        let requests = Arc::new(StdMutex::new(Vec::<Value>::new()));
        let captured = requests.clone();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |Json(body): Json<Value>| {
                let captured = captured.clone();
                async move {
                    lock(&captured).push(body);
                    ([(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
                }
            }))).await.unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let gateway = Gateway::start(GatewayConfig::new(directory.path(), Config {
            base_url: format!("http://{address}/v1"), model: "first".into(), api_key: String::new(),
        })).await.unwrap();
        let (tx, _rx) = mpsc::channel(64);
        let peer = Peer { id: "settings-client".into(), tx, stop: CancellationToken::new(), canonical: true, local_native: true };
        let created = gateway.shared.create(&json!({"source":"gui"}), &peer).await.unwrap_or_else(|e| panic!("{}", e.message));
        let sid = created["session_id"].as_str().unwrap();
        let session = gateway.shared.session(sid).await.unwrap_or_else(|e| panic!("{}", e.message));
        lock(&session.peers).insert(peer.id.clone(), peer.clone());
        let before = gateway.shared.snapshot(&session, &peer, false);
        let changed = gateway.shared.mutate(&json!({"session_id":sid,"operation":"model","request_id":"select-second",
            "expected_revision":before["revision"],"expected_generation":before["execution_generation"],
            "payload":{"model":"second","generation_options":{"temperature":0.4,"reasoning_effort":"high"}}}), &peer, true)
            .await.unwrap_or_else(|e| panic!("{}", e.message));
        let mut prompt = json!({"session_id":sid,"text":"Use the selected settings.","submission_id":"selected-once",
            "expected_instance_id":gateway.shared.instance_id,"expected_revision":before["revision"],
            "expected_generation":before["execution_generation"]});
        let stale = gateway.shared.submit(&prompt, &peer).await.err().unwrap();
        assert_eq!(stale.data["reason"], "revision_conflict");
        prompt["expected_revision"] = changed["revision"].clone();
        prompt["expected_instance_id"] = json!("another-owner");
        let stale = gateway.shared.submit(&prompt, &peer).await.err().unwrap();
        assert_eq!(stale.data["reason"], "owner_changed");
        assert!(lock(&session.view).authority.receipts.is_empty());
        prompt["expected_instance_id"] = json!(gateway.shared.instance_id);
        let receipt = gateway.shared.submit(&prompt, &peer).await.unwrap_or_else(|e| panic!("{}", e.message));
        tokio::time::timeout(Duration::from_secs(10), async {
            while lock(&session.view).running { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        prompt["expected_instance_id"] = json!("previous-owner");
        let retry = gateway.shared.submit(&prompt, &peer).await.unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(retry["admission_id"], receipt["admission_id"]);
        assert_eq!(lock(&session.view).history.turns.len(), 1);
        {
            let sent = lock(&requests);
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0]["model"], "second");
            assert_eq!(sent[0]["temperature"], 0.4);
            assert_eq!(sent[0]["reasoning_effort"], "high");
        }
        gateway.shutdown().await.unwrap(); server.abort();
    }
    #[tokio::test]
    async fn legacy_parked_group_expands_atomically_at_resume() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let library = Library::open(dir.path()).unwrap();
        let ada = library.create_bot("Ada", "", "", "").unwrap();
        let grace = library.create_bot("Grace", "", "", "").unwrap();
        let group = library.create_group("Review", &[ada.name, grace.name]).unwrap();
        let chat = library.group_chat(&group.id).unwrap();
        std::fs::write(chat.directory.join("queued-prompts.json"), serde_json::to_vec(&json!({"entries":[{"id":2,"text":"legacy","attachments":[]}],"in_flight":{"entry":{"id":1,"text":"uncertain","attachments":[]},"turn_index":0}})).unwrap()).unwrap();
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config)).await.unwrap();
        let session = gateway.shared.session(&chat.id).await.unwrap();
        let id = lock(&session.view).authority.receipts[1].id.clone();
        let (tx, _rx) = mpsc::channel(4);
        let peer = Peer { id: "legacy-test".into(), tx, stop: CancellationToken::new(), canonical: true, local_native: true };
        lock(&session.peers).insert(peer.id.clone(), peer.clone());
        library.delete_bot("grace").unwrap();
        // A group saved by an earlier version can still name a bot that is gone.
        let groups = dir.path().join("groups.json");
        let mut saved: Value = serde_json::from_slice(&std::fs::read(&groups).unwrap()).unwrap();
        saved["groups"][0]["members"] = json!(["ada", "grace"]);
        std::fs::write(&groups, serde_json::to_vec(&saved).unwrap()).unwrap();
        assert!(gateway.shared.resume_parked(&json!({"session_id":chat.id,"admission_id":id}), &peer).await.is_err());
        assert_eq!(lock(&session.view).authority.receipts[1].status, "parked");
        assert_eq!(lock(&session.view).authority.receipts.len(), 2);
        library.create_bot("Grace", "", "", "").unwrap();
        let result = gateway.shared.resume_parked(&json!({"session_id":chat.id,"admission_id":id}), &peer).await.unwrap();
        assert_eq!(result["admission_id"], id);
        {
            let view = lock(&session.view);
            assert_eq!(view.authority.receipts.len(), 3);
            assert_eq!(view.authority.receipts[0].status, "unknown");
            assert_eq!(view.authority.receipts[1].id, id);
            assert_eq!(view.authority.receipts[1].group.as_ref().unwrap().name, "ada");
            assert_eq!(view.authority.receipts[2].group.as_ref().unwrap().name, "grace");
            assert_eq!(view.authority.receipts[2].group.as_ref().unwrap().round, id);
            assert!(view.history.turns.is_empty());
        }
        drop(session);
        gateway.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn producer_result_is_committed_once_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = || Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config())).await.unwrap();
        let chat = gateway.shared.library.create_session(None, None).unwrap();
        let session = gateway.shared.session(&chat.id).await.unwrap();
        let turn = hermes_engine::Turn { context: String::new(), attachments: vec![], prompt: "scheduled".into(), model: "fixture".into(), entries: vec![hermes_engine::Entry::Assistant { text: "done".into(), calls: vec![] }], status: hermes_engine::Status::Complete };
        assert!(gateway.shared.try_publish_background_turn(&session, "run-1", &turn, || Ok(())).unwrap());
        assert!(gateway.shared.try_publish_background_turn(&session, "run-1", &turn, || Ok(())).unwrap());
        assert_eq!(lock(&session.view).history.turns.len(), 1);
        drop(session);
        gateway.shutdown().await.unwrap();
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config())).await.unwrap();
        let session = gateway.shared.session(&chat.id).await.unwrap();
        assert!(gateway.shared.try_publish_background_turn(&session, "run-1", &turn, || Ok(())).unwrap());
        assert_eq!(lock(&session.view).history.turns.len(), 1);
        let mut changed = turn; changed.prompt = "different".into();
        assert!(gateway.shared.try_publish_background_turn(&session, "run-1", &changed, || Ok(())).is_err());
        drop(session);
        gateway.shutdown().await.unwrap();
    }
}
