//! Gateway-owned scheduler. Durable claim, authority admission and publication
//! are separately reconciled by the run identity after an owner restart.
use super::*;
use hermes_engine::cron::{self, Claim, Outcome, RunStatus, Store};
use std::collections::{BTreeMap, HashSet};
use tokio::sync::mpsc;

pub(super) fn start(shared: Arc<Shared>, store: Store) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        let mut active = HashSet::new();
        let mut workers: Vec<JoinHandle<()>> = Vec::new();
        loop {
            let claims = tokio::select! {
                _ = shared.shutdown.cancelled() => break,
                Some(id) = done_rx.recv() => { active.remove(&id); continue; },
                _ = shared.scheduler_notify.notified() => Vec::new(),
                _ = interval.tick() => {
                    let mut claims = match store.gateway_claims() { Ok(claims) => claims, Err(error) => { eprintln!("scheduler recovery: {error:#}"); vec![] } };
                    let _admissions = lock(&shared.admissions);
                    if !shared.shutdown.is_cancelled() {
                        // Script-only jobs never call a model, so a broken model setting must not stop them.
                        let model_ready = shared.provider.validate().is_ok();
                        let due = store.claim_due_matching(chrono::Utc::now(), |job| model_ready || job.no_agent());
                        match due { Ok(due) => claims.extend(due), Err(error) => eprintln!("scheduler claim: {error:#}") }
                    }
                    claims
                }
            };
            let mut claims = claims;
            if claims.is_empty() { claims = store.gateway_claims().unwrap_or_default(); }
            for claim in claims {
                if !active.insert(claim.run.id.clone()) { continue; }
                let shared = shared.clone(); let store = store.clone(); let done = done_tx.clone();
                workers.retain(|worker| !worker.is_finished());
                workers.push(tokio::spawn(async move {
                    let id = claim.run.id.clone();
                    if let Err(error) = run(shared, store, claim).await { eprintln!("scheduler {id}: {error:#}"); }
                    let _ = done.send(id);
                }));
            }
        }
        for worker in workers { let _ = worker.await; }
    })
}

async fn session_for(shared: &Arc<Shared>, store: &Store, claim: &Claim, workspace: &Path, model: &str) -> Result<Arc<LiveSession>> {
    let _guard = shared.creates.lock().await;
    let id = if let Some(id) = &claim.run.gateway_session { id.clone() } else {
        // A background chat: it never takes over the user's open "New chat" or the profile's
        // default project, and a retry after a crash finds the same chat by its run.
        let settings = Settings { source: "scheduled".into(), cwd: Some(workspace.to_owned()), model: Some(model.to_owned()), ..Default::default() };
        let summary = shared.library.create_background_session(&format!("Run · {}", claim.job.title()), &format!("scheduled-run:{}", claim.run.id),
            |summary| settings.save(&summary.directory))?;
        store.bind_gateway_session(&claim.run.id, &summary.id)?;
        summary.id
    };
    shared.session(&id).await.map_err(|error| anyhow::anyhow!(error.message))
}

fn prepare_workspace(store: &Store, claim: &Claim, config: &mut Config) -> Result<PathBuf> {
    let workspace = cron::prepare_job(store, claim, config)?;
    let workspace = workspace.canonicalize().context("Could not open the scheduled working directory")?;
    ensure!(workspace.is_dir(), "Scheduled working directory must be a folder");
    Ok(workspace)
}

struct SavedAdmission {
    receipt: Receipt,
    turn: Option<hermes_engine::Turn>,
}

fn saved_admission(shared: &Shared, claim: &Claim, submission: &str) -> Result<Option<SavedAdmission>> {
    let Some(id) = &claim.run.gateway_session else { return Ok(None); };
    let record = read_record(&shared.library.session_dir(id)?)?;
    Ok(record.authority.as_ref().and_then(|authority| {
        authority.receipts.iter().find(|receipt| receipt.input_id.as_deref() == Some(submission)).map(|receipt| {
            SavedAdmission {
                receipt: receipt.clone(),
                turn: receipt.turn_index.and_then(|index| record.history.turns.get(index).cloned()),
            }
        })
    }))
}

fn fail_unadmitted(store: &Store, claim: &Claim, error: impl std::fmt::Display) -> Result<()> {
    let outcome = if store.cancellation_requested(&claim.run.id)? {
        Outcome::Cancelled { reason: "Stopped by the user".into() }
    } else {
        Outcome::Failed { error: error.to_string() }
    };
    store.finish(claim, outcome, chrono::Utc::now())
}

async fn wait_for_turn(shared: &Arc<Shared>, store: &Store, claim: &Claim, session: &Arc<LiveSession>, submission: &str) -> Result<Option<SavedAdmission>> {
    loop {
        if shared.shutdown.is_cancelled() { return Ok(None); }
        let cancellation_requested = store.cancellation_requested(&claim.run.id).unwrap_or(true);
        let (finished, cancelled_queued) = {
            let mut view = lock(&session.view);
            let index = view.authority.receipts.iter().position(|receipt| receipt.input_id.as_deref() == Some(submission))
                .context("Scheduled admission is missing")?;
            let receipt = &view.authority.receipts[index];
            // A completed run can share its chat with later user work. Its
            // cancellation flag must never reach that work's token.
            let mut cancelled_queued = false;
            if cancellation_requested && matches!(receipt.status.as_str(), "queued" | "parked") {
                let mut next = view.authority.clone();
                next.receipts[index].status = "terminal".into();
                next.receipts[index].outcome = Some("cancelled".into());
                next.persist(&session.writer, None)?;
                view.authority = next;
                view.revision += 1;
                cancelled_queued = true;
            } else if cancellation_requested && receipt.status == "started"
                && view.current_admission.as_deref() == Some(receipt.id.as_str())
            {
                if let Some(cancel) = &view.cancel { cancel.cancel(); }
            }
            let receipt = &view.authority.receipts[index];
            let finished = (receipt.status == "terminal").then(|| SavedAdmission {
                receipt: receipt.clone(),
                turn: receipt.turn_index.and_then(|index| view.history.turns.get(index).cloned()),
            });
            (finished, cancelled_queued)
        };
        if cancelled_queued {
            shared.emit(session, "session.info", shared.info(session));
            if let Some(next) = shared.claim_next(session)? { shared.spawn_worker(session.clone(), next).await; }
        }
        if finished.is_some() { return Ok(finished); }
        // Unknown external effects remain fenced until explicitly resolved.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub(super) async fn routine_for(shared: &Arc<Shared>, job_id: &str, title: &str) -> Result<Arc<LiveSession>> {
    let _guard = shared.creates.lock().await;
    let path = shared.library.root().join("routine-chats.json");
    let mut index: BTreeMap<String, String> = match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
        Err(error) => return Err(error.into()),
    };
    let summary = match index.get(job_id).and_then(|id| shared.library.session(id).ok()).filter(|s| !s.archived) {
        Some(summary) => summary,
        None => {
            // Derive each successor from its predecessor so a retry finds the same replacement.
            let mut predecessor = index.get(job_id).cloned();
            let title = format!("Routine · {title}");
            let summary = loop {
                let key = match &predecessor {
                    Some(id) => format!("routine-chat:{job_id}:after:{id}"),
                    None => format!("routine-chat:{job_id}"),
                };
                let summary = shared.library.create_background_session(&title, &key, |_| Ok(()))?;
                if !summary.archived { break summary; }
                predecessor = Some(summary.id);
            };
            index.insert(job_id.to_owned(), summary.id.clone());
            let mut temp = NamedTempFile::new_in(shared.library.root())?;
            serde_json::to_writer_pretty(&mut temp, &index)?;
            temp.as_file().sync_all()?;
            temp.persist(&path)?;
            summary
        }
    };
    shared.session(&summary.id).await.map_err(|error| anyhow::anyhow!(error.message))
}

async fn publish_routine_turn(shared: &Arc<Shared>, store: &Store, claim: &Claim, turn: &hermes_engine::Turn) -> Result<bool> {
    let source = format!("cron:{}", claim.run.id);
    loop {
        if shared.shutdown.is_cancelled() { return Ok(false); }
        {
            // Select and publish together with respect to catalog archive and delete operations.
            let _catalog = shared.catalog_edits.lock().await;
            let routine = if let Some(id) = &claim.run.gateway_routine_session {
                match shared.session(id).await {
                    Ok(session) => session,
                    Err(error) if error.data["reason"] == "not_found" => return Ok(true),
                    Err(error) => anyhow::bail!(error.message),
                }
            } else {
                routine_for(shared, &claim.job.id, claim.job.title()).await?
            };
            let id = lock(&routine.summary).id.clone();
            if shared.try_publish_background_turn(&routine, &source, turn, || {
                // Save the chosen destination before writing the receipt so recovery follows it.
                if claim.run.gateway_routine_session.is_none() {
                    store.bind_gateway_routine(&claim.run.id, &id)?;
                }
                Ok(())
            })? {
                shared.emit(&routine, "session.info", shared.info(&routine));
                return Ok(true);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn run(shared: Arc<Shared>, store: Store, claim: Claim) -> Result<()> {
    let submission = format!("cron:{}", claim.run.id);
    // Once admitted, the durable receipt is authoritative even if the job or
    // provider configuration or working directory has changed before recovery.
    let saved = saved_admission(&shared, &claim, &submission)?;
    let finished = if saved.as_ref().is_some_and(|saved| saved.receipt.status == "terminal") {
        saved.expect("terminal admission")
    } else {
        let session = if saved.is_some() {
            // Opening a claimed admission may fail, but that is never permission
            // to resubmit or settle an uncertain external effect.
            shared.session(claim.run.gateway_session.as_deref().expect("saved session")).await
                .map_err(|error| anyhow::anyhow!(error.message))?
        } else {
            if store.cancellation_requested(&claim.run.id)? {
                store.finish(&claim, Outcome::Cancelled { reason: "Stopped by the user".into() }, chrono::Utc::now())?;
                return Ok(());
            }
            let outside_agent = cron::has_preflight(&claim.job);
            if outside_agent && claim.run.status != RunStatus::Claimed {
                if let Some(route) = claim.job.deliver.strip_prefix("channel:")
                    && let Some(output) = shared.messaging.notification_text(route, &("cron", &claim.run.id))?
                {
                    store.finish(&claim, Outcome::Completed { output }, chrono::Utc::now())?;
                    return Ok(());
                }
                // A started script can have effects without an agent receipt.
                // Preserve its run identity and never execute it again after recovery.
                fail_unadmitted(&store, &claim, "Hermes restarted while this job's script was running")?;
                return Ok(());
            }
            let mut prompt = claim.job.prompt.clone();
            if outside_agent {
                if let Err(error) = store.begin(&claim, chrono::Utc::now()) {
                    fail_unadmitted(&store, &claim, format!("{error:#}"))?;
                    return Ok(());
                }
                match cron::preflight_with_cancel(&store, &claim, &shared.shutdown).await {
                    Ok(cron::Preflight::Agent(text)) => prompt = text,
                    Ok(cron::Preflight::Done(output)) => {
                        if let Some(route) = claim.job.deliver.strip_prefix("channel:") {
                            shared.messaging.notify_route(route, &("cron", &claim.run.id), output.clone())?;
                        }
                        store.finish(&claim, Outcome::Completed { output }, chrono::Utc::now())?;
                        return Ok(());
                    }
                    Err(error) => {
                        if shared.shutdown.is_cancelled() { return Ok(()); }
                        fail_unadmitted(&store, &claim, format!("{error:#}"))?;
                        return Ok(());
                    }
                }
            }
            let mut config = shared.provider.clone();
            let workspace = match prepare_workspace(&store, &claim, &mut config) {
                Ok(workspace) => workspace,
                Err(error) => { fail_unadmitted(&store, &claim, format!("{error:#}"))?; return Ok(()); }
            };
            let session = match session_for(&shared, &store, &claim, &workspace, &config.model).await {
                Ok(session) => session,
                Err(error) => { fail_unadmitted(&store, &claim, format!("{error:#}"))?; return Ok(()); }
            };
            if claim.run.status == RunStatus::Claimed && !outside_agent
                && let Err(error) = store.begin(&claim, chrono::Utc::now())
            {
                fail_unadmitted(&store, &claim, format!("{error:#}"))?;
                return Ok(());
            }
            if let Err(error) = shared.submit_background(session.clone(), prompt, submission.clone(), !claim.run.manual).await {
                // A failed directory sync can follow a successful atomic write.
                // Re-read the receipt before declaring that no work was admitted.
                let record = read_record(&session.dir)?;
                if record.authority.is_some_and(|authority| authority.receipts.iter().any(|receipt| receipt.input_id.as_deref() == Some(&submission))) {
                    return Err(anyhow::anyhow!(error.message));
                }
                fail_unadmitted(&store, &claim, error.message)?;
                return Ok(());
            }
            session
        };
        let finished = wait_for_turn(&shared, &store, &claim, &session, &submission).await?;
        if shared.shutdown.is_cancelled() { return Ok(()); }
        finished.context("Scheduled admission did not finish")?
    };
    let turn = finished.turn;
    if let Some(turn) = &turn
        && !publish_routine_turn(&shared, &store, &claim, turn).await?
    {
        return Ok(());
    }
    let outcome = match turn {
        Some(turn) => match turn.status {
            hermes_engine::Status::Complete => Outcome::Completed { output: turn.entries.iter().filter_map(|entry| if let hermes_engine::Entry::Assistant { text, .. } = entry { Some(text.as_str()) } else { None }).collect::<Vec<_>>().join("\n") },
            hermes_engine::Status::Interrupted => Outcome::Cancelled { reason: "Stopped by the user".into() },
            hermes_engine::Status::Failed(error) => Outcome::Failed { error },
            hermes_engine::Status::Running => Outcome::Failed { error: "Run ended without a terminal status".into() },
        },
        None if finished.receipt.outcome.as_deref() == Some("cancelled") => Outcome::Cancelled { reason: "Stopped by the user".into() },
        None => Outcome::Failed { error: "Run ended without a conversation turn".into() },
    };
    if let Outcome::Completed { output } = &outcome
        && let Some(route) = claim.job.deliver.strip_prefix("channel:")
    {
        shared.messaging.notify_route(route, &("cron", &claim.run.id), output.clone())?;
    }
    store.finish(&claim, outcome, chrono::Utc::now())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, body::{Body, Bytes}, http::header, response::{IntoResponse, Response}, routing::post};
    use futures_util::{stream, StreamExt};
    use std::convert::Infallible;
    use tokio::{net::TcpListener, time::timeout};

    async fn model(Json(request): Json<Value>) -> Response {
        let last = request["messages"].as_array().unwrap().last().unwrap();
        if last["content"] == "slow" {
            let chunk = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Waiting\"},\"finish_reason\":null}]}\n\n";
            let body = stream::once(async move { Ok::<_, Infallible>(Bytes::from(chunk)) }).chain(stream::pending());
            return ([(header::CONTENT_TYPE, "text/event-stream")], Body::from_stream(body)).into_response();
        }
        ([(header::CONTENT_TYPE, "text/event-stream")], "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n").into_response()
    }

    async fn isolated_gateway(root: &Path) -> (RunningGateway, JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/v1/chat/completions", post(model))).await.unwrap();
        });
        let config = Config { base_url: base, model: "fixture".into(), api_key: String::new() };
        let mut gateway = Gateway::start(GatewayConfig::new(root, config)).await.unwrap();
        let scheduler = gateway.scheduler_task.take().unwrap();
        scheduler.abort();
        let _ = scheduler.await;
        (gateway, server)
    }

    async fn bound_claim(gateway: &RunningGateway) -> (Claim, Arc<LiveSession>) {
        bound_claim_for(gateway, cron::Draft { prompt: "scheduled".into(), paused: true, ..Default::default() }).await
    }

    async fn bound_claim_for(gateway: &RunningGateway, draft: cron::Draft) -> (Claim, Arc<LiveSession>) {
        let store = &gateway.shared.scheduler;
        let job = store.create(draft, chrono::Utc::now()).unwrap();
        let claim = store.claim_gateway_now(&job.id, "manual", chrono::Utc::now()).unwrap();
        let mut config = gateway.shared.provider.clone();
        let workspace = prepare_workspace(store, &claim, &mut config).unwrap();
        let session = session_for(&gateway.shared, store, &claim, &workspace, &config.model).await.unwrap();
        store.begin(&claim, chrono::Utc::now()).unwrap();
        let claim = store.gateway_claims().unwrap().into_iter().find(|saved| saved.run.id == claim.run.id).unwrap();
        (claim, session)
    }

    #[tokio::test]
    async fn a_scheduled_run_keeps_its_own_folder_and_leaves_the_users_new_chat_alone() {
        let dir = tempfile::tempdir().unwrap();
        let (project, job_folder) = (dir.path().join("project-a"), dir.path().join("job-b"));
        fs::create_dir(&project).unwrap();
        fs::create_dir(&job_folder).unwrap();
        fs::write(dir.path().join("agent-settings.json"), serde_json::json!({"default_directory": project}).to_string()).unwrap();
        let library = Library::open(dir.path()).unwrap();
        let blank = library.create_session(None, None).unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let (claim, session) = bound_claim_for(&gateway, cron::Draft {
            prompt: "scheduled".into(), workdir: Some(job_folder.clone()), paused: true, ..Default::default()
        }).await;
        let run_chat = claim.run.gateway_session.clone().unwrap();
        assert_ne!(run_chat, blank.id, "the run has a chat of its own");
        assert_eq!(library.active_session().unwrap().map(|s| s.id), Some(blank.id.clone()), "the user's selection is unchanged");
        assert_eq!(library.session(&blank.id).unwrap().title, "New chat");
        assert_eq!(library.session(&run_chat).unwrap().project_id, None, "not filed under the default project");
        assert_eq!(gateway.shared.library.workspace_for_session(&run_chat).unwrap(), None);
        assert_eq!(session.settings.lock().unwrap().cwd.as_deref(), Some(job_folder.canonicalize().unwrap().as_path()));
        server.abort();
    }

    #[tokio::test]
    async fn a_routine_run_after_archiving_uses_a_new_conversation() {
        let dir = tempfile::tempdir().unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let store = &gateway.shared.scheduler;
        let job = store.create(cron::Draft {
            prompt: "scheduled".into(), paused: true, ..Default::default()
        }, chrono::Utc::now()).unwrap();
        let first = store.claim_gateway_now(&job.id, "before-archive", chrono::Utc::now()).unwrap();
        run(gateway.shared.clone(), store.clone(), first).await.unwrap();
        let old = routine_for(&gateway.shared, &job.id, job.title()).await.unwrap();
        let old_id = lock(&old.summary).id.clone();
        assert_eq!(read_record(&old.dir).unwrap().history.turns.len(), 1);
        gateway.shared.library.set_archived(&old_id, true).unwrap();

        let second = store.claim_gateway_now(&job.id, "after-archive", chrono::Utc::now()).unwrap();
        run(gateway.shared.clone(), store.clone(), second).await.unwrap();
        let current = routine_for(&gateway.shared, &job.id, job.title()).await.unwrap();
        let current_id = lock(&current.summary).id.clone();
        assert_ne!(current_id, old_id);
        assert_eq!(read_record(&current.dir).unwrap().history.turns.len(), 1);
        assert!(gateway.shared.library.session(&old_id).unwrap().archived);
        assert_eq!(read_record(&old.dir).unwrap().history.turns.len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn recovering_a_published_run_keeps_its_routine_destination() {
        let dir = tempfile::tempdir().unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let store = &gateway.shared.scheduler;
        let (claim, _) = bound_claim(&gateway).await;
        let turn = hermes_engine::Turn {
            prompt: "scheduled".into(), context: String::new(), attachments: vec![], model: "fixture".into(),
            entries: vec![hermes_engine::Entry::Assistant { text: "done".into(), calls: vec![] }],
            status: hermes_engine::Status::Complete,
        };
        assert!(publish_routine_turn(&gateway.shared, store, &claim, &turn).await.unwrap());
        let saved = store.gateway_claims().unwrap().into_iter()
            .find(|saved| saved.run.id == claim.run.id).unwrap();
        let archived_id = saved.run.gateway_routine_session.as_deref().unwrap();
        let archived = gateway.shared.library.set_archived(archived_id, true).unwrap();
        let replacement = routine_for(&gateway.shared, &claim.job.id, claim.job.title()).await.unwrap();
        assert_ne!(lock(&replacement.summary).id, archived_id);

        assert!(publish_routine_turn(&gateway.shared, store, &saved, &turn).await.unwrap());
        assert_eq!(read_record(&archived.directory).unwrap().history.turns.len(), 1);
        assert!(read_record(&replacement.dir).unwrap().history.turns.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn scheduled_runs_appear_as_cron_chats_for_stock_clients() {
        let dir = tempfile::tempdir().unwrap();
        let library = Library::open(dir.path()).unwrap();
        let run = library.create_session(None, Some("Run · nightly")).unwrap();
        Settings { source: "scheduled".into(), ..Default::default() }.save(&run.directory).unwrap();
        let mut history = hermes_engine::History::default();
        history.turns.push(hermes_engine::Turn {
            prompt: "scheduled".into(), context: String::new(), attachments: vec![], model: "fixture".into(),
            entries: vec![hermes_engine::Entry::Assistant { text: "done".into(), calls: vec![] }], status: hermes_engine::Status::Complete,
        });
        hermes_engine::restore_conversation(&run.directory, serde_json::to_value(&history).unwrap()).unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let sidebar: Value = reqwest::Client::new()
            .get(format!("{}/api/profiles/sessions/sidebar?recents_profile=default&recents_exclude=cron", gateway.http_url()))
            .bearer_auth(gateway.token()).send().await.unwrap().json().await.unwrap();
        let ids = |list: &Value| list["sessions"].as_array().unwrap().iter().map(|s| s["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
        assert_eq!(ids(&sidebar["cron"]), [run.id.clone()], "{sidebar}");
        assert!(!ids(&sidebar["recents"]).contains(&run.id), "{sidebar}");
        server.abort();
    }

    async fn wait_for_receipt(shared: &Shared, session: &LiveSession, input: &str, status: &str) {
        timeout(Duration::from_secs(5), async {
            while shared.submission_receipt(session, input).is_none_or(|receipt| receipt["status"] != status) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn missing_workdirs_settle_new_and_previously_bound_claims() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("removed-workspace");
        fs::create_dir(&workspace).unwrap();
        let library = Library::open(dir.path()).unwrap();
        let store = Store::open_gateway(dir.path()).unwrap();
        let mut run_ids = Vec::new();
        let mut unbound_id = None;
        for bound in [false, true] {
            let job = store.create(cron::Draft {
                prompt: "must not execute".into(), workdir: Some(workspace.clone()), paused: true,
                ..Default::default()
            }, chrono::Utc::now()).unwrap();
            let claim = store.claim_gateway_now(&job.id, &format!("missing-{bound}"), chrono::Utc::now()).unwrap();
            if bound {
                let summary = library.create_session(None, None).unwrap();
                Settings { source: "scheduled".into(), cwd: Some(workspace.clone()), model: Some("fixture".into()), ..Default::default() }
                    .save(&summary.directory).unwrap();
                store.bind_gateway_session(&claim.run.id, &summary.id).unwrap();
                store.begin(&claim, chrono::Utc::now()).unwrap();
            } else { unbound_id = Some(claim.run.id.clone()); }
            run_ids.push((job.id, claim.run.id));
        }
        fs::remove_dir(&workspace).unwrap();
        drop(store);
        let view = Store::open_view(dir.path()).unwrap();
        let config = Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let mut gateway = Gateway::start(GatewayConfig::new(dir.path(), config)).await.unwrap();
        timeout(Duration::from_secs(5), async {
            loop {
                if run_ids.iter().all(|(job, id)| view.runs(job).unwrap().iter().any(|run| run.id == *id && run.status == RunStatus::Failed)) { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        let scheduler = gateway.scheduler_task.take().unwrap();
        scheduler.abort();
        let _ = scheduler.await;
        assert!(gateway.shared.scheduler.gateway_claims().unwrap().is_empty());
        for (job, _) in &run_ids {
            let run = view.runs(job).unwrap().into_iter().next().unwrap();
            assert!(run.error.unwrap().contains("working directory"));
            if Some(&run.id) == unbound_id.as_ref() { assert!(run.gateway_session.is_none()); }
        }
        let job = view.create(cron::Draft { prompt: "next job".into(), schedule: "every 1m".into(), ..Default::default() }, chrono::Utc::now()).unwrap();
        let due = cron::instant(job.next_run_at.as_deref().unwrap()).unwrap();
        assert_eq!(gateway.shared.scheduler.claim_due(due).unwrap().len(), 1, "failed workspaces must release scheduler capacity");
        gateway.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_active_run_does_not_interrupt_its_followup() {
        let dir = tempfile::tempdir().unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let (claim, session) = bound_claim(&gateway).await;
        let submission = format!("cron:{}", claim.run.id);
        gateway.shared.submit_background(session.clone(), "slow".into(), submission.clone(), false).await.unwrap();
        gateway.shared.submit_background(session.clone(), "slow".into(), "followup".into(), false).await.unwrap();
        wait_for_receipt(&gateway.shared, &session, &submission, "started").await;
        wait_for_receipt(&gateway.shared, &session, "followup", "queued").await;
        gateway.shared.scheduler.cancel(&claim.run.id).unwrap();
        timeout(Duration::from_secs(5), run(gateway.shared.clone(), gateway.shared.scheduler.clone(), claim.clone())).await.unwrap().unwrap();
        wait_for_receipt(&gateway.shared, &session, "followup", "started").await;
        {
            let view = lock(&session.view);
            assert!(!view.cancel.as_ref().unwrap().is_cancelled(), "a later admission owns this cancellation token");
            assert_eq!(view.authority.receipts.iter().find(|receipt| receipt.input_id.as_deref() == Some("followup")).unwrap().status, "started");
        }
        assert_eq!(gateway.shared.scheduler.runs(&claim.job.id).unwrap()[0].status, RunStatus::Cancelled);
        drop(session);
        gateway.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn cancelling_queued_run_does_not_interrupt_the_active_user_turn() {
        let dir = tempfile::tempdir().unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let (claim, session) = bound_claim(&gateway).await;
        let submission = format!("cron:{}", claim.run.id);
        gateway.shared.submit_background(session.clone(), "slow".into(), "active-user".into(), false).await.unwrap();
        gateway.shared.submit_background(session.clone(), "scheduled".into(), submission.clone(), false).await.unwrap();
        gateway.shared.scheduler.cancel(&claim.run.id).unwrap();
        timeout(Duration::from_secs(5), run(gateway.shared.clone(), gateway.shared.scheduler.clone(), claim.clone())).await.unwrap().unwrap();
        {
            let view = lock(&session.view);
            assert!(!view.cancel.as_ref().unwrap().is_cancelled());
            assert_eq!(view.authority.receipts.iter().find(|receipt| receipt.input_id.as_deref() == Some(&submission)).unwrap().outcome.as_deref(), Some("cancelled"));
        }
        assert_eq!(gateway.shared.scheduler.runs(&claim.job.id).unwrap()[0].status, RunStatus::Cancelled);
        drop(session);
        gateway.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn missing_workdir_does_not_resubmit_or_settle_an_uncertain_admission() {
        let dir = tempfile::tempdir().unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let (claim, session) = bound_claim(&gateway).await;
        let submission = format!("cron:{}", claim.run.id);
        gateway.shared.submit_background(session.clone(), "scheduled".into(), submission.clone(), false).await.unwrap();
        wait_for_receipt(&gateway.shared, &session, &submission, "terminal").await;
        for worker in std::mem::take(&mut *gateway.shared.turns.lock().await) { worker.await.unwrap(); }
        let id = lock(&session.summary).id.clone();
        let workspace = lock(&session.settings).cwd.clone().unwrap();
        let path = session.dir.clone();
        gateway.shared.sessions.lock().await.remove(&id);
        drop(session);
        let mut record = hermes_engine::conversation_json(&path).unwrap().unwrap();
        record["authority"]["receipts"][0]["status"] = json!("started");
        record["authority"]["receipts"][0]["outcome"] = Value::Null;
        record["authority"]["receipts"][0]["result"] = Value::Null;
        record["turns"][0]["status"] = json!({"state":"running"});
        hermes_engine::restore_conversation(&path, record.clone()).unwrap();
        fs::remove_dir(&workspace).unwrap();

        assert!(run(gateway.shared.clone(), gateway.shared.scheduler.clone(), claim.clone()).await.is_err());
        assert_eq!(hermes_engine::conversation_json(&path).unwrap().unwrap(), record, "an uncertain admission must retain its original identity and disposition");
        assert_eq!(gateway.shared.scheduler.runs(&claim.job.id).unwrap()[0].status, RunStatus::Running);
        gateway.shared.scheduler.finish(&claim, Outcome::Cancelled { reason: "test complete".into() }, chrono::Utc::now()).unwrap();
        gateway.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn completed_run_publishes_after_restart_without_its_provider_or_workspace() {
        use hermes_engine::cron::{Draft, RunStatus};
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, Router::new().route("/v1/chat/completions", post(model))).await.unwrap(); });
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("removed-workspace");
        fs::create_dir(&workspace).unwrap();
        let view = Store::open_view(dir.path()).unwrap();
        let job = view.create(Draft { name: "Recovery".into(), prompt: "scheduled".into(), workdir: Some(workspace.clone()), paused: true, ..Draft::default() }, chrono::Utc::now()).unwrap();
        let config = Config { base_url: base, model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config)).await.unwrap();
        let routine = routine_for(&gateway.shared, &job.id, job.title()).await.unwrap();
        let routine_id = lock(&routine.summary).id.clone();
        gateway.shared.submit_background(routine.clone(), "slow".into(), "block-routine".into(), false).await.unwrap();
        let claim = gateway.shared.scheduler.claim_gateway_now(&job.id, "manual-once", chrono::Utc::now()).unwrap();
        gateway.shared.scheduler_notify.notify_one();
        timeout(Duration::from_secs(20), async { loop {
            let run = view.runs(&job.id).unwrap().into_iter().find(|run| run.id == claim.run.id).unwrap();
            if let Some(id) = run.gateway_session {
                let session = gateway.shared.session(&id).await.unwrap();
                if gateway.shared.submission_receipt(&session, &format!("cron:{}", run.id)).is_some_and(|receipt| receipt["status"] == "terminal") { break; }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }}).await.unwrap();
        assert_eq!(view.runs(&job.id).unwrap()[0].status, RunStatus::Running);
        drop(routine);
        gateway.shutdown().await.unwrap();
        fs::remove_dir(&workspace).unwrap();
        let invalid = Config { base_url: String::new(), model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), invalid)).await.unwrap();
        timeout(Duration::from_secs(20), async { loop {
            if view.runs(&job.id).unwrap().iter().any(|run| run.status == RunStatus::Completed) { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }}).await.unwrap();
        let routine = gateway.shared.session(&routine_id).await.unwrap();
        let history = lock(&routine.view).history.clone();
        assert_eq!(history.turns.iter().filter(|turn| turn.prompt == "scheduled").count(), 1);
        assert_eq!(view.runs(&job.id).unwrap().len(), 1);
        drop(routine);
        gateway.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn a_script_only_job_finishes_without_the_model() {
        use hermes_engine::cron::{Draft, RunStatus};
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("scripts")).unwrap();
        fs::write(dir.path().join("scripts/report.py"), "print('disk ok')").unwrap();
        let view = Store::open_view(dir.path()).unwrap();
        let job = view.create(Draft { script: "report.py".into(), no_agent: true, paused: true, ..Draft::default() }, chrono::Utc::now()).unwrap();
        assert_eq!(job.title(), "report.py");
        let draft = job.draft();
        assert!(draft.no_agent && draft.script == "report.py");
        // Nothing listens here: a model request would fail the run.
        let config = Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config)).await.unwrap();
        gateway.shared.scheduler.claim_gateway_now(&job.id, "manual-script", chrono::Utc::now()).unwrap();
        gateway.shared.scheduler_notify.notify_one();
        let run = timeout(Duration::from_secs(30), async { loop {
            let run = view.runs(&job.id).unwrap().remove(0);
            if !run.status.live() { break run; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }}).await.unwrap();
        assert_eq!(run.status, RunStatus::Completed, "{}", run.output);
        assert_eq!(run.output, "disk ok");
        gateway.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn an_admitted_script_job_recovers_without_repeating_preflight() {
        let dir = tempfile::tempdir().unwrap();
        let (gateway, server) = isolated_gateway(dir.path()).await;
        let (claim, session) = bound_claim_for(&gateway, cron::Draft {
            prompt: "scheduled".into(), script: "missing-after-restart.py".into(), paused: true,
            ..Default::default()
        }).await;
        let submission = format!("cron:{}", claim.run.id);
        gateway.shared.submit_background(session.clone(), "scheduled".into(), submission.clone(), false).await.unwrap();
        wait_for_receipt(&gateway.shared, &session, &submission, "terminal").await;
        run(gateway.shared.clone(), gateway.shared.scheduler.clone(), claim.clone()).await.unwrap();
        assert_eq!(gateway.shared.scheduler.runs(&claim.job.id).unwrap()[0].status, RunStatus::Completed);
        assert_eq!(lock(&session.view).history.turns.len(), 1);
        drop(session);
        gateway.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn shutdown_stops_preflight_and_restart_does_not_repeat_its_effects() {
        let dir = tempfile::tempdir().unwrap();
        let scripts = dir.path().join("scripts");
        fs::create_dir(&scripts).unwrap();
        fs::write(scripts.join("slow.py"), "from pathlib import Path\nimport time\np = Path(__file__).with_name('starts')\np.write_text(str(int(p.read_text()) + 1) if p.exists() else '1')\ntime.sleep(30)\n").unwrap();
        let view = Store::open_view(dir.path()).unwrap();
        let job = view.create(cron::Draft {
            script: "slow.py".into(), no_agent: true, paused: true, ..Default::default()
        }, chrono::Utc::now()).unwrap();
        let config = Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config.clone())).await.unwrap();
        gateway.shared.scheduler.claim_gateway_now(&job.id, "manual-script", chrono::Utc::now()).unwrap();
        gateway.shared.scheduler_notify.notify_one();
        timeout(Duration::from_secs(10), async {
            while fs::read_to_string(scripts.join("starts")).ok().as_deref() != Some("1") {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        timeout(Duration::from_secs(5), gateway.shutdown()).await.unwrap().unwrap();
        assert_eq!(view.runs(&job.id).unwrap()[0].status, RunStatus::Running);
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config)).await.unwrap();
        timeout(Duration::from_secs(5), async {
            while view.runs(&job.id).unwrap()[0].status.live() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        assert_eq!(view.runs(&job.id).unwrap()[0].status, RunStatus::Failed);
        assert_eq!(fs::read_to_string(scripts.join("starts")).unwrap(), "1");
        gateway.shutdown().await.unwrap();
    }
}
