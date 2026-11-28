//! A window subscribes to the selected gateway even when another client owns the active turn.
use super::{Message, connections};
use hermes_engine::{CancellationToken, Config, Entry, History, Status, ToolCall, Turn};
use hermes_gateway::client::{ClientError, ClientMessage, ConnectionState, GatewayClient};
use iced::{Task, futures::SinkExt};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}},
    time::Duration,
};

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Route {
    Local,
    Remote { origin: String, token: String },
}

impl Route {
    pub fn selected(root: &Path, state: &connections::State) -> Result<Self, String> {
        match state.config.mode {
            connections::Mode::Local => Ok(Self::Local),
            connections::Mode::Ssh => state
                .tunnel
                .as_ref()
                .map(|t| Self::Remote {
                    origin: t.url.clone(),
                    token: t.token.clone(),
                })
                .ok_or_else(|| "Connect the selected SSH tunnel before opening a chat.".into()),
            connections::Mode::Remote | connections::Mode::Cloud => {
                let origin = state.config.url.trim().trim_end_matches('/').to_owned();
                if origin.is_empty() {
                    return Err("Select a gateway URL before opening a chat.".into());
                }
                let token = connections::saved_token(root)?;
                Ok(Self::Remote { origin, token })
            }
        }
    }
}

pub(super) struct Connection {
    pub route: Route,
    root: PathBuf,
    provider: Mutex<Config>,
    client: GatewayClient,
    connecting: tokio::sync::Mutex<()>,
    configuration: tokio::sync::Mutex<()>,
    owner_instance: Mutex<Option<String>>,
    selection: AtomicU64,
}

impl Connection {
    pub fn new(root: PathBuf, route: Route, provider: Config) -> Arc<Self> {
        Arc::new(Self {
            root,
            route,
            provider: Mutex::new(provider),
            client: GatewayClient::new(Default::default()),
            connecting: Default::default(),
            configuration: Default::default(),
            owner_instance: Mutex::new(None),
            selection: AtomicU64::new(0),
        })
    }
    pub fn matches(&self, root: &Path, route: &Route) -> bool {
        self.root == root && self.route == *route
    }
    #[cfg(test)]
    pub fn has_startup_provider(&self, provider: &Config) -> bool {
        let captured = self.provider.lock().unwrap();
        captured.base_url == provider.base_url && captured.api_key == provider.api_key && captured.model == provider.model
    }
    pub fn select_settings(&self) -> u64 {
        self.selection.fetch_add(1, Ordering::SeqCst) + 1
    }
    fn selection_is_current(&self, selection: u64) -> Result<(), String> {
        if self.selection.load(Ordering::SeqCst) == selection { Ok(()) }
        else { Err("The selected settings changed before they were applied".into()) }
    }
    async fn connect(&self) -> Result<(), String> {
        let _guard = self.connecting.lock().await;
        if self.client.connection_state() == ConnectionState::Open {
            return Ok(());
        }
        match &self.route {
            Route::Local => {
                let provider = self.provider.lock().unwrap().clone();
                let owner = hermes_gateway::owner::ensure_local(&self.root, &provider)
                    .await
                    .map_err(|e| format!("{e:#}"))?;
                owner
                    .connect(&self.client)
                    .await
                    .map_err(|e| format!("Could not attach to the local gateway: {e}"))?;
                *self.owner_instance.lock().unwrap() = Some(owner.instance_id().to_owned());
                Ok(())
            }
            Route::Remote { origin, token } => {
                let bearer = if token.is_empty() {
                    super::gateway_oauth::access_token(&self.root, origin).await?
                } else {
                    token.clone()
                };
                self.client
                    .connect_remote(origin, &bearer)
                    .await
                    .map_err(|e| e.to_string())
            }
        }
    }
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        self.connect().await?;
        self.client
            .request(method, params)
            .await
            .map_err(|e| format!("{method}: {e}"))
    }
    async fn selected_settings(
        &self, sid: &str, provider: &Config, options: &hermes_engine::models::GenerationOptions, selection: u64,
    ) -> Result<Value, String> {
        self.selection_is_current(selection)?;
        let owner = hermes_gateway::owner::ensure_configured(&self.root, provider)
            .await.map_err(|e| format!("Could not apply provider settings: {e:#}"))?;
        self.selection_is_current(selection)?;
        let same_owner = self.owner_instance.lock().unwrap().as_deref() == Some(owner.instance_id());
        if !same_owner || self.client.connection_state() != ConnectionState::Open {
            owner.connect(&self.client).await.map_err(|e| e.to_string())?;
            *self.owner_instance.lock().unwrap() = Some(owner.instance_id().to_owned());
        }
        *self.provider.lock().unwrap() = provider.clone();
        let desired = json!({"model":provider.model,"generation_options":options});
        for _ in 0..3 {
            let mut snapshot = self.resume(sid, false).await?;
            self.selection_is_current(selection)?;
            if snapshot["info"]["model"] != desired["model"]
                || snapshot["info"]["generation_options"] != desired["generation_options"] {
                let mutation = json!({"session_id":sid,"operation":"model","payload":desired,
                    "request_id":uuid::Uuid::new_v4().to_string(),
                    "expected_revision":snapshot["revision"],"expected_generation":snapshot["execution_generation"]});
                match self.client.request("session.mutate", mutation).await {
                    Ok(receipt) => {
                        snapshot["revision"] = receipt["revision"].clone();
                        snapshot["execution_generation"] = receipt["execution_generation"].clone();
                        snapshot["info"]["model"] = desired["model"].clone();
                        snapshot["info"]["generation_options"] = desired["generation_options"].clone();
                    }
                    Err(error) if matches!(error.reason(), Some("revision_conflict" | "stale_generation")) => continue,
                    Err(error) => return Err(format!("Could not apply session model settings: {error}")),
                }
            }
            snapshot["expected_instance_id"] = json!(owner.instance_id());
            self.selection_is_current(selection)?;
            return Ok(snapshot);
        }
        Err("Session settings changed in another client; retry your selection".into())
    }
    pub async fn configure_session(
        &self, sid: &str, provider: &Config, options: &hermes_engine::models::GenerationOptions, selection: u64,
    ) -> Result<Value, String> {
        let _configuration = self.configuration.lock().await;
        let _connecting = self.connecting.lock().await;
        self.selected_settings(sid, provider, options, selection).await
    }
    pub async fn submit_selected(
        &self, sid: &str, mut params: Value, provider: &Config, options: &hermes_engine::models::GenerationOptions, selection: u64,
        explicit: bool,
    ) -> Result<Value, String> {
        let _configuration = self.configuration.lock().await;
        let _connecting = self.connecting.lock().await;
        self.selection_is_current(selection)?;
        let owner = hermes_gateway::owner::ensure_local(&self.root, provider).await.map_err(|e| format!("{e:#}"))?;
        if !owner.is_native() {
            if explicit { return Err("Apply model and provider changes through this gateway's own controls".into()); }
            let same_owner = self.owner_instance.lock().unwrap().as_deref() == Some(owner.instance_id());
            if !same_owner || self.client.connection_state() != ConnectionState::Open {
                owner.connect(&self.client).await.map_err(|e| e.to_string())?;
                *self.owner_instance.lock().unwrap() = Some(owner.instance_id().to_owned());
            }
            self.selection_is_current(selection)?;
            self.resume(sid, false).await?;
            return self.client.request("prompt.submit", params).await.map_err(|e| format!("prompt.submit: {e}"));
        }
        let snapshot = self.selected_settings(sid, provider, options, selection).await?;
        params["expected_instance_id"] = snapshot["expected_instance_id"].clone();
        params["expected_revision"] = snapshot["revision"].clone();
        params["expected_generation"] = snapshot["execution_generation"].clone();
        // No reconnect between the acknowledged settings and admission. The
        // server also fences this request against another client's mutation.
        self.client.request("prompt.submit", params).await.map_err(|e| format!("prompt.submit: {e}"))
    }
    async fn resume(&self, sid: &str, detach_others: bool) -> Result<Value, String> {
        if let Some(barrier) = self.client.session_replay_barrier(sid)
            && !barrier.wait().await
        {
            return Err("Gateway disconnected during replay".into());
        }
        if detach_others {
            let reply = self.client.request("session.resume", json!({"session_id":sid,"detach_others":true})).await;
            // Older gateways may reject the optional parameter.
            if !matches!(&reply, Err(ClientError::Rpc { code: Some(-32602), .. })) {
                return reply.map_err(|e| e.to_string());
            }
        }
        self.client
            .request("session.resume", json!({"session_id":sid}))
            .await
            .map_err(|e| e.to_string())
    }
    pub fn watch(
        self: Arc<Self>,
        sid: String,
        epoch: u64,
        detach: CancellationToken,
    ) -> Task<Message> {
        let stream = iced::stream::channel::<Update>(64, async move |mut out| {
            let mut delay = 250;
            loop {
                let mut events = self.client.subscribe();
                let attach = async {
                    self.connect().await?;
                    self.resume(&sid, true).await
                };
                let result =
                    tokio::select! { _ = detach.cancelled() => return, result = attach => result };
                let error = match result {
                    Err(error) => error,
                    Ok(snap) => {
                        delay = 250;
                        let mut watermark = snap["last_sequence"].as_u64().unwrap_or(0);
                        if out.send(Update::Snapshot(snap)).await.is_err() {
                            return;
                        }
                        loop {
                            let message = tokio::select! { _ = detach.cancelled() => return, event = events.recv() => event };
                            let fetch = match message {
                                Ok(ClientMessage::Event(frame)) => {
                                    let event = frame.get("params").unwrap_or(&frame);
                                    if event["session_id"].as_str() != Some(&sid) {
                                        continue;
                                    }
                                    let seq = event["seq"].as_u64();
                                    if seq.is_some_and(|s| s <= watermark) {
                                        continue;
                                    }
                                    let gap =
                                        seq.is_some_and(|s| watermark != 0 && s > watermark + 1);
                                    if let Some(seq) = seq {
                                        watermark = seq;
                                    }
                                    let kind = event["type"].as_str().unwrap_or_default();
                                    if !gap
                                        && matches!(
                                            kind,
                                            "message.delta"
                                                | "message.complete"
                                                | "error"
                                                | "message.error"
                                                | "message.warning"
                                                | "subagent.progress"
                                        )
                                    {
                                        if out.send(Update::Event(event.clone())).await.is_err() {
                                            return;
                                        }
                                    }
                                    gap || matches!(
                                        kind,
                                        "message.start"
                                            | "session.info"
                                            | "session.updated"
                                            | "message.complete"
                                            | "tool.start"
                                            | "tool.complete"
                                            | "approval.request"
                                            | "approval.settled"
                                            | "approval.cancelled"
                                            | "session.replay_gap"
                                            | "session.snapshot_required"
                                            | "replay.snapshot_required"
                                    )
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => true,
                                Ok(ClientMessage::State(
                                    ConnectionState::Closed | ConnectionState::Error,
                                ))
                                | Err(_) => break "Gateway disconnected".into(),
                                _ => continue,
                            };
                            if fetch {
                                let result = tokio::select! { _ = detach.cancelled() => return, result = self.resume(&sid, false) => result };
                                match result {
                                    Ok(snap) => {
                                        watermark =
                                            snap["last_sequence"].as_u64().unwrap_or(watermark);
                                        if out.send(Update::Snapshot(snap)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) => break error,
                                }
                            }
                        }
                    }
                };
                if out.send(Update::Offline(error)).await.is_err() {
                    return;
                }
                tokio::select! { _ = detach.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(delay)) => () }
                delay = (delay * 2).min(5000);
            }
        });
        Task::run(stream, move |update| Message::Gateway(epoch, update))
    }
}

#[derive(Clone)]
pub(super) enum Update {
    Snapshot(Value),
    Event(Value),
    Offline(String),
    Created(Result<String, String>),
    Submitted(String, Result<Value, String>),
    Renamed(Result<String, String>),
    Controlled(Result<Value, String>),
    Listed(Result<Value, String>),
    Configured(u64, Result<Value, String>),
}

// Only UI state lives under this directory. Remote strings never become literal paths.
pub fn cache(root: &Path, key: &str) -> PathBuf {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut path = root.to_owned();
    let encoded = URL_SAFE_NO_PAD.encode(key);
    for part in encoded.as_bytes().chunks(64) {
        path.push(format!(
            "k-{}",
            std::str::from_utf8(part).expect("base64 is ASCII")
        ));
    }
    path
}

pub fn remote_summary(
    root: &Path,
    origin: &str,
    row: &Value,
) -> Option<hermes_library::SessionSummary> {
    let id = row["session_id"].as_str().or_else(|| row["id"].as_str())?;
    if id.is_empty() || id.len() > 256 {
        return None;
    }
    let profile = cache(&root.join("gateway-views"), origin);
    let directory = cache(&profile.join("sessions"), id);
    let title = row["title"].as_str().unwrap_or("New chat").to_owned();
    let date = row["started_at"]
        .as_i64()
        .and_then(|t| chrono::DateTime::from_timestamp(t, 0))
        .map(|t| t.to_rfc3339())
        .unwrap_or_default();
    Some(hermes_library::SessionSummary {
        id: id.into(),
        title,
        project_id: row["project_id"].as_str().map(Into::into),
        directory,
        created_at: date.clone(),
        updated_at: date,
        last_active: None,
        bot: None,
        source: hermes_library::SessionSource::Native,
        archived: false,
        pinned: false,
    })
}

pub(super) fn history(snapshot: &Value) -> Result<History, String> {
    if let Some(native) = snapshot.get("native_history") {
        return serde_json::from_value(native.clone())
            .map_err(|e| format!("Invalid gateway history: {e}"));
    }
    let rows = snapshot["messages"]
        .as_array()
        .ok_or("The gateway returned no session history")?;
    let mut history = History::default();
    let model = snapshot["info"]["model"].as_str().unwrap_or_default();
    for row in rows {
        let text = row["text"]
            .as_str()
            .or_else(|| row["content"].as_str())
            .unwrap_or_default()
            .to_owned();
        match row["role"].as_str() {
            Some("user") => history.turns.push(Turn {
                context: String::new(),
                attachments: vec![],
                prompt: text,
                model: row["model"].as_str().unwrap_or(model).into(),
                entries: vec![],
                status: Status::Complete,
            }),
            Some("assistant") => {
                if let Some(turn) = history.turns.last_mut() {
                    let calls = row["tool_calls"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|call| ToolCall {
                            id: call["id"].as_str().unwrap_or_default().into(),
                            name: call["function"]["name"].as_str().unwrap_or_default().into(),
                            arguments: call["function"]["arguments"]
                                .as_str()
                                .unwrap_or_default()
                                .into(),
                        })
                        .collect();
                    turn.entries.push(Entry::Assistant { text, calls });
                }
            }
            Some("tool") => {
                if let Some(turn) = history.turns.last_mut() {
                    turn.entries.push(Entry::Tool {
                        id: row["tool_call_id"].as_str().unwrap_or_default().into(),
                        name: row["name"].as_str().unwrap_or_default().into(),
                        approved: row["approved"].as_bool().unwrap_or(true),
                        result: text,
                    });
                }
            }
            _ => {}
        }
    }
    if let Some(last) = history.turns.last_mut() {
        if snapshot["running"] == true {
            last.status = Status::Running;
        } else if let Some(status) = terminal_status(&snapshot["latest_outcome"]) {
            last.status = status;
        }
    }
    Ok(history)
}

pub(super) fn terminal_status(payload: &Value) -> Option<Status> {
    Some(match payload["status"].as_str()? {
        "complete" | "completed" => Status::Complete,
        "interrupted" | "cancelled" => Status::Interrupted,
        "failed" | "error" => Status::Failed(
            payload["error"]
                .as_str()
                .or_else(|| payload["message"].as_str())
                .unwrap_or("The gateway turn failed")
                .into(),
        ),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_and_interrupted_completions_keep_their_outcomes() {
        assert_eq!(
            terminal_status(&json!({"status":"error","error":"provider offline"})),
            Some(Status::Failed("provider offline".into()))
        );
        assert_eq!(
            terminal_status(&json!({"status":"interrupted"})),
            Some(Status::Interrupted)
        );
    }
    #[test]
    fn stock_snapshot_preserves_tool_identity() {
        let h = history(&json!({"messages":[{"role":"user","content":"clock"},{"role":"assistant","tool_calls":[{"id":"x","function":{"name":"time","arguments":"{}"}}]},{"role":"tool","tool_call_id":"x","content":"ok"}]})).unwrap();
        assert!(
            matches!(&h.turns[0].entries[0], Entry::Assistant {calls,..} if calls[0].id == "x")
        );
        assert!(matches!(&h.turns[0].entries[1], Entry::Tool{id,..} if id == "x"));
    }
}
