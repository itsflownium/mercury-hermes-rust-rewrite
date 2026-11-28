use super::*;
use serde_json::{Value, json};

#[derive(Default)]
pub(super) struct View {
    connection: Option<Arc<remote::Connection>>,
    sid: Option<String>,
    pub detach: Option<CancellationToken>,
    pub ready: bool,
    generation: u64,
    pub pending_queue: Vec<Queued>,
    pending: Option<(String, Value)>,
    pub submitting: bool,
    pub settings_explicit: bool,
    settings_revision: u64,
    creating: bool,
    create_params: Option<Value>,
    quick_prompt: Option<String>,
    terminal_status: Option<(u64, hermes_engine::Status)>,
    pub tasks: Vec<Task<Message>>,
    pending_plugin: Option<PendingPluginCommand>,
}
struct PendingPluginCommand { connection: Arc<remote::Connection>, name:String, args:String, epoch:u64, session_id:Option<String> }
pub(super) struct Queued {
    pub id: String,
    pub status: String,
    pub text: String,
    pub generation: u64,
}
#[derive(Clone, Copy)]
pub(super) enum QueueAction {
    Resume,
    Cancel,
    MarkInterrupted(u64),
}
impl Drop for View {
    fn drop(&mut self) {
        if let Some(detach) = self.detach.take() {
            detach.cancel();
        }
    }
}
#[cfg(test)]
impl View {
    pub fn has_startup_provider(&self, provider: &Config) -> bool {
        self.connection.as_ref().is_some_and(|connection| connection.has_startup_provider(provider))
    }
}

impl Hermes {
    pub(super) fn sync_selected_settings(&mut self) -> Task<Message> {
        self.chat.settings_explicit = true;
        if self.connections.config.mode != connections::Mode::Local || self.chat.submitting {
            return Task::none();
        }
        let Some(sid) = self.chat.sid.clone() else { return Task::none(); };
        let conn = match self.chat_connection() {
            Ok(conn) => conn,
            Err(error) => { self.error(error); return Task::none(); }
        };
        self.chat.settings_revision = self.chat.settings_revision.wrapping_add(1);
        let revision = self.chat.settings_revision;
        let selection = conn.select_settings();
        let provider = self.config.clone();
        let options = self.models.options.clone();
        let epoch = self.run_id;
        self.note("Applying chat settings…");
        Task::perform(async move { conn.configure_session(&sid, &provider, &options, selection).await },
            move |result| Message::Gateway(epoch, remote::Update::Configured(revision, result)))
    }
    pub fn restore_gateway_chat(&mut self, existing: Option<SessionSummary>) {
        let conn = match self.chat_connection() {
            Ok(c) => c,
            Err(e) => {
                self.error(e);
                return;
            }
        };
        if let remote::Route::Remote { origin, .. } = &conn.route {
            let Some(library) = &self.library else {
                return;
            };
            let root = library.root();
            let dir = remote::cache(&root.join("gateway-views"), origin);
            let selected = secondary_session().or_else(|| {
                self.appearance
                    .preferences
                    .reopen_last_chat
                    .then(|| std::fs::read_to_string(dir.join("last-session")).ok())
                    .flatten()
            });
            let prior = existing.filter(|s| {
                std::fs::read(s.directory.join("remote-session.json"))
                    .ok()
                    .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                    .is_some_and(|v| v["url"].as_str() == Some(origin))
            });
            self.tabs = tabs::State::default();
            self.sessions.clear();
            if let Some(summary) = selected
                .and_then(|id| remote::remote_summary(root, origin, &json!({"id":id})))
                .or(prior)
            {
                self.open_gateway_chat(summary);
            } else {
                self.create_gateway_chat();
            }
        } else if let Some(summary) = existing {
            self.open_gateway_chat(summary);
        } else {
            self.create_chat();
        }
    }
    pub fn refresh_gateway_route(&mut self) {
        let Some(root) = self.library.as_ref().map(|l| l.root().to_owned()) else {
            return;
        };
        let route = remote::Route::selected(&root, &self.connections);
        if let Ok(route) = &route
            && self
                .chat
                .connection
                .as_ref()
                .is_some_and(|c| c.matches(&root, route))
        {
            return;
        }
        if let Err(e) = self.save_draft() {
            self.error(e);
            return;
        }
        self.chat = View::default();
        self.run_id = self.run_id.wrapping_add(1);
        self.current = None;
        self.session = None;
        self.approvals.clear();
        self.running = false;
        self.history = History::default();
        self.rebuild_markdown();
        self.sync_terminal_context();
        if let Err(e) = route {
            self.error(e);
            return;
        }
        if self.connections.config.mode == connections::Mode::Local {
            if let Some(library) = &self.library { self.tabs = tabs::State::load(library); }
            self.refresh_sessions();
        }
        let prior = self
            .library
            .as_ref()
            .and_then(|l| l.active_session().ok())
            .flatten();
        self.restore_gateway_chat(prior);
    }
    pub fn quick_chat(&mut self, prompt: String, new: bool) -> Task<Message> {
        if new || self.current.is_none() {
            self.create_chat();
        }
        if self.chat.creating || !self.chat.ready {
            self.chat.quick_prompt = Some(prompt);
            return Task::none();
        }
        self.composer = text_editor::Content::with_text(&prompt);
        self.send()
    }
    pub(super) fn chat_connection(&mut self) -> Result<Arc<remote::Connection>, String> {
        let root = self
            .library
            .as_ref()
            .ok_or("Open a profile first")?
            .root()
            .to_owned();
        let route = remote::Route::selected(&root, &self.connections)?;
        if let Some(conn) = &self.chat.connection
            && conn.matches(&root, &route)
        {
            return Ok(conn.clone());
        }
        if let Some(detach) = self.chat.detach.take() {
            detach.cancel();
        }
        let conn = remote::Connection::new(root, route, self.config.clone());
        if self.chat.pending_plugin.as_ref().is_some_and(|pending|!Arc::ptr_eq(&pending.connection,&conn)){self.chat.pending_plugin=None;}
        self.chat.connection = Some(conn.clone());
        self.chat.ready = false;
        Ok(conn)
    }

    pub(super) fn run_plugin_command(&mut self,name:String,args:String)->Task<Message>{
        let connection=match self.chat_connection(){Ok(conn)=>conn,Err(error)=>{self.error(error);return Task::none()}};
        let epoch=self.run_id;
        if let Some(session_id)=self.chat.sid.clone().filter(|_|self.chat.ready){
            return dispatch_plugin_command(connection,session_id,epoch,name,args);
        }
        let session_id=self.chat.sid.clone();
        self.chat.pending_plugin=Some(PendingPluginCommand{connection,name,args,epoch,session_id:session_id.clone()});
        if session_id.is_some(){Task::none()}else{self.update(Message::NewChat)}
    }

    pub(super) fn plugin_target_is_current(&self,connection:&Arc<remote::Connection>,session_id:&str,epoch:u64)->bool{
        epoch==self.run_id && self.chat.sid.as_deref()==Some(session_id) && self.chat.ready
            && self.chat.connection.as_ref().is_some_and(|current|Arc::ptr_eq(current,connection))
    }

    fn dispatch_pending_plugin(&mut self,session_id:&str)->Task<Message>{
        let Some(pending)=self.chat.pending_plugin.take() else{return Task::none()};
        if pending.epoch!=self.run_id || pending.session_id.as_deref()!=Some(session_id)
            || self.chat.connection.as_ref().is_none_or(|conn|!Arc::ptr_eq(conn,&pending.connection)) { return Task::none(); }
        dispatch_plugin_command(pending.connection,session_id.to_owned(),pending.epoch,pending.name,pending.args)
    }
    pub fn find_chat(&self, id: &str) -> Option<SessionSummary> {
        if let Some(remote::Route::Remote { origin, .. }) = self.chat.connection.as_ref().map(|conn| &conn.route) {
            return self.sessions.iter().find(|summary| summary.id == id).cloned().or_else(|| {
                let library = self.library.as_ref()?;
                let title = self.tabs.entries.iter().find(|tab| tab.id == id).map(|tab| tab.title.as_str()).unwrap_or("New chat");
                remote::remote_summary(library.root(), origin, &json!({"id":id,"title":title}))
            });
        }
        self.sessions
            .iter()
            .find(|s| s.id == id)
            .cloned()
            .or_else(|| self.library.as_ref()?.session(id).ok())
    }
    pub fn open_gateway_chat(&mut self, summary: SessionSummary) {
        let conn = match self.chat_connection() {
            Ok(c) => c,
            Err(e) => {
                self.error(e);
                return;
            }
        };
        if self.chat.pending_plugin.as_ref().is_some_and(|pending|pending.session_id.as_deref()!=Some(summary.id.as_str()) || !Arc::ptr_eq(&pending.connection,&conn)) {self.chat.pending_plugin=None;}
        if self.current.as_ref().is_some_and(|s| s.id == summary.id) && self.chat.ready {
            self.screen = Screen::Chat;
            return;
        }
        if let Err(e) = self.save_draft() {
            self.error(e);
            return;
        }
        if let Err(e) = std::fs::create_dir_all(&summary.directory) {
            self.error(format!("Could not open the draft folder: {e}"));
            return;
        }
        let sid = match &conn.route {
            remote::Route::Local => summary.id.clone(),
            remote::Route::Remote { origin, .. } => {
                // Existing releases recorded their remote binding alongside a local transcript.
                let binding = std::fs::read(summary.directory.join("remote-session.json"))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
                binding
                    .as_ref()
                    .filter(|v| v["url"].as_str() == Some(origin))
                    .and_then(|v| v["session_id"].as_str())
                    .unwrap_or(&summary.id)
                    .to_owned()
            }
        };
        self.voice.stop_all();
        if let Some(detach) = self.chat.detach.take() {
            detach.cancel();
        }
        self.run_id = self.run_id.wrapping_add(1);
        self.chat.ready = false;
        self.chat.generation = 0;
        self.chat.terminal_status = None;
        self.chat.submitting = false;
        self.chat.pending = None;
        self.chat.creating = false;
        self.chat.create_params = None;
        self.running = false;
        self.stopping = false;
        self.approvals.clear();
        self.subagents.clear();
        self.session = None;
        self.history = History::default();
        self.rebuild_markdown();
        self.session_menu = None;
        self.rename = None;
        self.follow_stream = true;
        let draft =
            std::fs::read_to_string(summary.directory.join("draft.txt")).unwrap_or_default();
        self.composer = text_editor::Content::with_text(&draft);
        self.draft_files = match attachments::DraftFiles::load(&summary.directory) {
            Ok(files) => files,
            Err(e) => {
                self.error(e);
                attachments::DraftFiles::default()
            }
        };
        if let Some(library) = &self.library {
            match &conn.route {
                remote::Route::Local => {
                    if let Err(e) = library.select_session(&summary.id) {
                        self.error(e.to_string());
                    }
                }
                remote::Route::Remote { origin, .. } => {
                    let dir = remote::cache(&library.root().join("gateway-views"), origin);
                    if let Err(e) = std::fs::write(dir.join("last-session"), &sid) {
                        self.error(e.to_string());
                    }
                }
            }
        }
        self.fresh_draft = summary.is_empty_draft();
        self.tabs.activate(&summary, self.fresh_draft);
        self.current = Some(summary);
        self.save_tabs();
        let _ = self.files.reset(None);
        let _ = self.git.reset(None);
        self.sync_terminal_context();
        self.screen = Screen::Chat;
        let detach = CancellationToken::new();
        self.chat.detach = Some(detach.clone());
        self.chat.sid = Some(sid.clone());
        self.chat.tasks.push(conn.watch(sid, self.run_id, detach));
        self.list_gateway_chats();
    }
    pub fn create_gateway_chat(&mut self) {
        if self.chat.creating {
            return;
        }
        let conn = match self.chat_connection() {
            Ok(c) => c,
            Err(e) => {
                self.error(e);
                return;
            }
        };
        let mut params = json!({"source":"gui","title":"New chat"});
        if conn.route == remote::Route::Local {
            if let Some(project) = &self.project {
                params["project_id"] = json!(project.id);
                params["cwd"] = json!(project.path);
            }
            if !self.config.model.trim().is_empty() {
                params["model"] = json!(self.config.model);
            }
            params["generation_options"] = json!(self.models.options);
        }
        let retry = self.chat.create_params.as_ref().filter(|old| {
            let mut old = (*old).clone();
            old.as_object_mut().unwrap().remove("request_id");
            old == params
        });
        if let Some(old) = retry {
            params = old.clone();
        } else {
            params["request_id"] = json!(uuid::Uuid::new_v4().to_string());
        }
        self.chat.create_params = Some(params.clone());
        self.chat.creating = true;
        let epoch = self.run_id;
        self.chat.tasks.push(Task::perform(
            async move {
                let created = conn.request("session.create", params).await?;
                created["session_id"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "The gateway returned no session ID".into())
            },
            move |result| Message::Gateway(epoch, remote::Update::Created(result)),
        ));
    }
    pub fn list_gateway_chats(&mut self) {
        let Some(conn) = self.chat.connection.clone() else {
            return;
        };
        let query = json!({"scope":"stored","query":self.search,"limit":1000});
        let epoch = self.run_id;
        self.chat.tasks.push(Task::perform(
            async move { conn.request("session.list", query).await },
            move |result| Message::Gateway(epoch, remote::Update::Listed(result)),
        ));
    }
    fn gateway_control(&self, method: &'static str, mut params: Value) -> Task<Message> {
        let (Some(conn), Some(sid)) = (self.chat.connection.clone(), self.chat.sid.as_ref()) else {
            return Task::none();
        };
        params["session_id"] = json!(sid);
        let epoch = self.run_id;
        Task::perform(
            async move { conn.request(method, params).await },
            move |result| Message::Gateway(epoch, remote::Update::Controlled(result)),
        )
    }
    pub fn rename_gateway(&self, title: String, epoch: u64) -> Result<Task<Message>, String> {
        let (Some(conn), Some(sid)) = (self.chat.connection.clone(), self.chat.sid.clone()) else {
            return Err("The chat is not connected to its profile gateway.".into());
        };
        let params = json!({"session_id":sid,"title":title.clone()});
        Ok(Task::perform(
            async move { conn.request("session.title", params).await.map(|_| title) },
            move |result| Message::Gateway(epoch, remote::Update::Renamed(result)),
        ))
    }
    pub fn set_gateway_pin(&mut self, id: String, pinned: bool) -> Result<Task<Message>, String> {
        let conn = self.chat_connection()?;
        let message_id = id.clone();
        Ok(Task::perform(
            async move {
                conn.request("catalog.session.set", json!({"session_id":id,"pinned":pinned})).await
            },
            move |result| Message::CatalogPinned(message_id, pinned, result),
        ))
    }
    pub fn set_gateway_archive(&mut self, id: String, archived: bool) -> Result<Task<Message>, String> {
        let conn = self.chat_connection()?;
        let message_id = id.clone();
        Ok(Task::perform(
            async move {
                conn.request("catalog.session.set", json!({"session_id":id,"archived":archived})).await
            },
            move |result| Message::CatalogSessionChanged(message_id, archived, result),
        ))
    }
    pub fn interrupt_gateway(&mut self) -> Task<Message> {
        if !self.running || !self.chat.ready {
            return Task::none();
        }
        self.stopping = true;
        self.gateway_control(
            "session.interrupt",
            json!({"execution_generation":self.chat.generation}),
        )
    }
    pub fn change_pending(&self, sid: String, id: String, action: QueueAction) -> Task<Message> {
        if self.current.as_ref().is_none_or(|c| c.id != sid) || !self.chat.ready {
            return Task::none();
        }
        let Some(entry) = self.chat.pending_queue.iter().find(|entry| entry.id == id) else {
            return Task::none();
        };
        match action {
            QueueAction::Resume if entry.status == "parked" => {
                self.gateway_control("prompt.resume_parked", json!({"admission_id":id}))
            }
            QueueAction::Cancel if matches!(entry.status.as_str(), "queued" | "parked") => {
                self.gateway_control("prompt.cancel", json!({"admission_id":id}))
            }
            QueueAction::MarkInterrupted(generation)
                if entry.status == "unknown" && entry.generation == generation =>
            {
                self.gateway_control(
                    "prompt.resolve_unknown",
                    json!({"admission_id":id,"execution_generation":generation}),
                )
            }
            _ => Task::none(),
        }
    }
    pub fn answer_gateway(&self, prompt: String, generation: u64, yes: bool) -> Task<Message> {
        if !self
            .approvals
            .iter()
            .any(|a| a.call.id == prompt && a.generation == generation)
        {
            return Task::none();
        }
        self.gateway_control(
            "approval.respond",
            json!({"prompt_id":prompt, "request_id":prompt,
            "execution_generation":generation, "choice":if yes { "once" } else { "deny" }}),
        )
    }
    pub fn submit_gateway(&mut self, prompt: String) -> Task<Message> {
        if self.chat.submitting {
            return Task::none();
        }
        if !self.chat.ready {
            self.error("Wait for the chat to connect before sending.");
            return Task::none();
        }
        let (Some(conn), Some(sid)) = (self.chat.connection.clone(), self.chat.sid.as_ref()) else {
            return Task::none();
        };
        let mut params = json!({"session_id":sid,"text":prompt});
        if !self.draft_files.files.is_empty() {
            params["attachments"] = json!(self.draft_files.files);
        }
        let local = conn.route == remote::Route::Local;
        let provider = self.config.clone();
        let options = self.models.options.clone();
        let explicit = self.chat.settings_explicit;
        // Retrying text after changing settings is a new intentional submission.
        // These client fields never enter the gateway wire payload.
        if local {
            params["client_model"] = json!(provider.model);
            params["client_options"] = json!(options);
            params["client_provider"] = json!(provider.base_url);
        }
        let id = self
            .chat
            .pending
            .as_ref()
            .filter(|(_, previous)| *previous == params)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        self.chat.pending = Some((id.clone(), params.clone()));
        self.chat.submitting = true;
        let selection = conn.select_settings();
        let sid = sid.clone();
        if local {
            params.as_object_mut().unwrap().remove("client_model");
            params.as_object_mut().unwrap().remove("client_options");
            params.as_object_mut().unwrap().remove("client_provider");
        }
        params["submission_id"] = json!(id);
        if self.running {
            params["queued"] = json!(true);
        }
        let files = self.draft_files.files.clone();
        let directory = self.current.as_ref().unwrap().directory.clone();
        let epoch = self.run_id;
        Task::perform(
            async move {
                let result = async {
                if matches!(&conn.route, remote::Route::Remote { .. }) && !files.is_empty() {
                    use base64::{Engine, engine::general_purpose::STANDARD};
                    let mut refs = Vec::with_capacity(files.len());
                    for file in files {
                        let bytes = file.data(&directory).map_err(|e| format!("Could not stage {}: {e}", file.name))?;
                        let upload = conn.request("file.attach", json!({"session_id":params["session_id"],
                            "name":file.name, "data_url":format!("data:{};base64,{}", file.media_type, STANDARD.encode(bytes))})).await?;
                        refs.push(upload.get("attachment").cloned().ok_or("Gateway returned no staged attachment reference")?);
                    }
                    params["attachments"] = Value::Array(refs);
                }
                if local { conn.submit_selected(&sid, params, &provider, &options, selection, explicit).await }
                else { conn.request("prompt.submit", params).await }
            }.await;
                (id, result)
            },
            move |(id, result)| Message::Gateway(epoch, remote::Update::Submitted(id, result)),
        )
    }
    pub fn gateway_update(&mut self, update: remote::Update) -> Task<Message> {
        use remote::Update;
        let mut followup = Task::none();
        match update {
            Update::Configured(revision, result) => {
                if revision != self.chat.settings_revision { return Task::none(); }
                match result {
                    Ok(_) => self.note("Chat settings applied"),
                    Err(error) => self.error(error),
                }
            }
            Update::Created(result) => {
                self.chat.creating = false;
                match result {
                    Err(e) => {if self.chat.pending_plugin.as_ref().is_some_and(|pending|pending.epoch==self.run_id){self.chat.pending_plugin=None;}self.error(e)},
                    Ok(id) => {
                        if let Some(pending)=self.chat.pending_plugin.as_mut(){
                            if pending.epoch==self.run_id && self.chat.connection.as_ref().is_some_and(|conn|Arc::ptr_eq(conn,&pending.connection)) {pending.session_id=Some(id.clone());}
                            else {self.chat.pending_plugin=None;}
                        }
                        let Some(library) = &self.library else {
                            return Task::none();
                        };
                        let summary = match self.chat.connection.as_ref().map(|c| &c.route) {
                            Some(remote::Route::Remote { origin, .. }) => {
                                remote::remote_summary(library.root(), origin, &json!({"id":id}))
                            }
                            _ => library.session(&id).ok(),
                        };
                        if let Some(summary) = summary {
                            self.open_gateway_chat(summary);
                        } else {
                            self.error("The new session is missing from the selected profile.");
                        }
                    }
                }
            }
            Update::Listed(result) => {
                let Ok(rows) = result else {
                    if let Err(e) = result {
                        self.error(e);
                    }
                    return Task::none();
                };
                let Some(library) = &self.library else {
                    return Task::none();
                };
                if let Some(remote::Route::Remote { origin, .. }) =
                    self.chat.connection.as_ref().map(|c| &c.route)
                {
                    self.sessions = rows["sessions"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|row| remote::remote_summary(library.root(), origin, row))
                        .collect();
                }
            }
            Update::Snapshot(snapshot) => {
                let was_running = self.running;
                let mut history = match remote::history(&snapshot) {
                    Ok(h) => h,
                    Err(e) => {
                        self.error(e);
                        return Task::none();
                    }
                };
                self.chat.ready = true;
                self.chat.generation = snapshot["execution_generation"]
                    .as_u64()
                    .or(snapshot["info"]["execution_generation"].as_u64())
                    .unwrap_or_default();
                self.running = snapshot["running"].as_bool().unwrap_or(false);
                let pending = snapshot["pending"].as_array().into_iter().flatten();
                self.chat.pending_queue = pending
                    .clone()
                    .filter_map(|p| {
                        let status = p["status"].as_str()?;
                        if status == "started" {
                            return None;
                        }
                        Some(Queued {
                            id: p["admission_id"].as_str()?.into(),
                            status: status.into(),
                            text: p["text"].as_str().unwrap_or_default().into(),
                            generation: p["execution_generation"].as_u64().unwrap_or(0),
                        })
                    })
                    .collect();
                if let Some(active) = pending.into_iter().find(|p| p["status"] == "started")
                    && active["turn_index"].as_u64() == Some(history.turns.len() as u64)
                    && let Some(prompt) = active["text"].as_str()
                {
                    history.turns.push(hermes_engine::Turn {
                        prompt: prompt.into(),
                        model: active["model"].as_str().unwrap_or_default().into(),
                        context: String::new(),
                        attachments: serde_json::from_value(active["attachments"].clone())
                            .unwrap_or_default(),
                        entries: vec![],
                        status: hermes_engine::Status::Running,
                    });
                }
                if snapshot.get("native_history").is_none()
                    && !self.running
                    && let Some((gen_id, status)) = &self.chat.terminal_status
                    && *gen_id == self.chat.generation
                    && let Some(last) = history.turns.last_mut()
                {
                    last.status = status.clone();
                }
                if self.running && !was_running {
                    self.statusbar.turn_started();
                }
                if !self.running {
                    self.stopping = false;
                    if was_running {
                        self.statusbar.turn_finished();
                        self.refresh_sessions();
                    }
                }
                self.accept_snapshot(history);
                let plugin= self.chat.sid.clone().map(|sid|self.dispatch_pending_plugin(&sid)).unwrap_or_else(Task::none);
                let reply = if was_running && !self.running { self.read_finished_reply() } else { Task::none() };
                followup = Task::batch([plugin, super::slash::refresh(self), reply]);
                self.approvals = snapshot["prompts"]
                    .as_array()
                    .map(|v| v.iter().collect::<Vec<_>>())
                    .unwrap_or_else(|| snapshot.get("pending_approval").into_iter().collect())
                    .into_iter()
                    .filter_map(|p| {
                        let id = p["prompt_id"].as_str().or(p["request_id"].as_str())?;
                        Some(Approval {
                            generation: p["execution_generation"]
                                .as_u64()
                                .unwrap_or(self.chat.generation),
                            call: ToolCall {
                                id: id.into(),
                                name: p["tool_name"].as_str().unwrap_or("tool").into(),
                                arguments: p["command"]
                                    .as_str()
                                    .or(p["description"].as_str())
                                    .unwrap_or_default()
                                    .into(),
                            },
                        })
                    })
                    .collect();
                self.fresh_draft = self.history.turns.is_empty() && !self.running;
                if let Some(prompt) = self.chat.quick_prompt.take() {
                    self.composer = text_editor::Content::with_text(&prompt);
                    return self.send();
                }
            }
            Update::Event(frame) => {
                let p = &frame["payload"];
                match frame["type"].as_str().unwrap_or_default() {
                    "message.complete" => {
                        if let Some(status) = remote::terminal_status(p) {
                            self.chat.terminal_status = Some((
                                frame["execution_generation"]
                                    .as_u64()
                                    .unwrap_or(self.chat.generation),
                                status.clone(),
                            ));
                            if let Some(turn) = self.history.turns.last_mut() {
                                turn.status = status.clone();
                            }
                            if let hermes_engine::Status::Failed(e) = status {
                                self.error(e);
                            }
                        }
                    }
                    "message.error" | "error" => {
                        self.error(p["message"].as_str().unwrap_or("Gateway request failed"))
                    }
                    "message.warning" => {
                        if let Some(message) = p["message"].as_str() {
                            self.note(message);
                        }
                    }
                    "message.delta" => {
                        let delta = p["text"].as_str().unwrap_or_default();
                        if let Some(turn) = self
                            .history
                            .turns
                            .last_mut()
                            .filter(|t| t.status == hermes_engine::Status::Running)
                        {
                            if !matches!(turn.entries.last(), Some(Entry::Assistant {calls,..}) if calls.is_empty())
                            {
                                turn.entries.push(Entry::Assistant {
                                    text: String::new(),
                                    calls: vec![],
                                });
                                if let Some(items) = self.rendered.last_mut() {
                                    items.push(Some(markdown::Content::parse("")));
                                }
                            }
                            if let Some(Entry::Assistant { text, .. }) = turn.entries.last_mut() {
                                if p["offset"]
                                    .as_u64()
                                    .is_none_or(|n| n == text.encode_utf16().count() as u64)
                                {
                                    text.push_str(delta);
                                    if let Some(Some(content)) =
                                        self.rendered.last_mut().and_then(|items| items.last_mut())
                                    {
                                        content.push_str(delta);
                                    }
                                }
                            }
                            self.statusbar.streamed(delta.len());
                        }
                    }
                    "memory.recall" => {
                        if let Ok(status) = serde_json::from_value(p.clone()) {
                            self.show_memory_recall(status);
                        }
                    }
                    "subagent.progress" => {
                        if let Ok(progress) =
                            serde_json::from_value::<hermes_engine::subagents::Progress>(p.clone())
                        {
                            if let Some(old) =
                                self.subagents.iter_mut().find(|s| s.id == progress.id)
                            {
                                *old = progress;
                            } else {
                                self.subagents.push(progress);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Update::Offline(e) => {
                self.chat.ready = false;
                self.error(e);
            }
            Update::Renamed(result) => {
                match result {
                    Ok(title) => {
                        if let Some(current) = &mut self.current { current.title = title.clone(); }
                        for summary in &mut self.sessions {
                            if self.current.as_ref().is_some_and(|current| current.id == summary.id) {
                                summary.title = title.clone();
                            }
                        }
                        self.rename = None;
                        self.refresh_sessions();
                        self.note("Chat renamed");
                    }
                    Err(error) => self.error(format!("Could not rename chat: {error}")),
                }
            }
            Update::Controlled(result) => {
                if let Err(e) = result {
                    self.stopping = false;
                    self.error(e);
                }
            }
            Update::Submitted(id, result) => {
                self.chat.submitting = false;
                if let Err(e) = result {
                    self.error(e);
                    return Task::none();
                }
                if let Some((key, params)) = self.chat.pending.take()
                    && key == id
                {
                    let same_files = params
                        .get("attachments")
                        .cloned()
                        .unwrap_or_else(|| json!([]))
                        == json!(self.draft_files.files);
                    if params["text"].as_str() == Some(self.composer.text().trim()) && same_files {
                        self.composer = text_editor::Content::new();
                        self.clear_draft_files();
                        if let Err(e) = self.save_draft() {
                            self.error(e);
                        }
                    }
                    self.fresh_draft = false;
                    if let Some(current) = &self.current {
                        self.tabs.committed(&current.id);
                    }
                    self.save_tabs();
                }
            }
        }
        followup
    }
}

fn dispatch_plugin_command(connection:Arc<remote::Connection>,session_id:String,epoch:u64,name:String,args:String)->Task<Message>{
    let target=connection.clone();let target_session=session_id.clone();
    Task::perform(async move{connection.request("slash.exec",json!({"session_id":session_id,"command":format!("/{name} {args}").trim_end()})).await},
        move |result|Message::Slash(super::slash::Action::PluginFinished(target,target_session,epoch,result)))
}
