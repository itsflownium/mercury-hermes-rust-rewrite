//! Authenticated loopback WebSocket gateway for the native Rust engine.
//!
//! This is a documented subset of Hermes' legacy and `hermes-gateway-v1` wires,
//! not an implementation of the entire upstream dashboard or session authority.

pub mod client;
pub mod messaging;
pub mod owner;
pub mod replay;
pub mod stock_plugins;
mod memory_capture;
mod profile_memory;
mod python_history;
mod python_memory;
mod slash_commands;
mod ego_commands;
mod methods;

mod auth;
mod rest;
mod shell_exec;
mod upload;
mod webhooks;
mod terminal;
mod authority;
mod scheduler;
mod catalog;
mod turn_control;
use authority::{Authority, Receipt};
mod telegram_host;
use hermes_engine::{
    Attachment, ConversationWriter,
    memory_capture::{CaptureBinding, PendingMemoryCapture},
    models::GenerationOptions,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        ConnectInfo, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use hermes_engine::{CancellationToken, Config, Entry, Event, History, Session, Status};
use hermes_library::{Library, SessionSummary};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tempfile::NamedTempFile;
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock, mpsc, OwnedRwLockReadGuard, OwnedRwLockWriteGuard},
    task::JoinHandle,
};
use uuid::Uuid;

const PROTOCOL: &str = "hermes-gateway-v1";
const RUNTIME_CAPABILITIES: &[&str] = &["session-authority-v1", "rust-engine-subset-v1", "event-replay-v1"];
const TICKET_PREFIX: &str = "hermes-gateway-ticket.";
const TICKET_TTL: Duration = Duration::from_secs(30);
const MAX_SESSIONS: usize = 64;
const MAX_CONNECTIONS: usize = 32;
const MAX_REPLAY_EVENTS: usize = 512;
const MAX_REPLAY_BYTES: usize = 524_288;
const MAX_PROMPT_BYTES: usize = 262_144;

/// Methods implemented by this crate. Other methods return JSON-RPC -32601.
pub const METHODS: &[&str] = &[
    "ping",
    "gateway.ping",
    "rpc.discover",
    "runtime.describe",
    "gateway.capabilities",
    "client.capabilities",
    "session.create",
    "session.resume",
    "session.activate",
    "session.list",
    "session.active_list",
    "session.history",
    "session.info",
    "session.status",
    "session.events.since",
    "session.interrupt",
    "session.close",
    "session.title",
    "session.mutate",
    "cron.chat",
    "cron.trigger",
    "prompt.submit",
    "prompt.receipt",
    "prompt.cancel",
    "prompt.replace_queued",
    "session.steer",
    "session.redirect",
    "clarify.respond",
    "catalog.session.set",
    "catalog.session.delete",
    "catalog.bot.delete",
    "catalog.archive_inactive",
    "plugins.memory.sync",
    "plugins.memory.archive_search",
    "commands.catalog",
    "slash.exec",
    "memory.providers",
    "memory.provider.set",
    "process.list",
    "process.kill",
    "process.stop",
    "prompt.resolve_unknown",
    "prompt.resume_parked",
    "file.attach",
    "shell.exec",
    "approval.pending",
    "approval.received",
    "approval.respond",
    "telegram.status",
    "telegram.connect",
    "telegram.disconnect",
    "telegram.send",
    "telegram.resolve_delivery",
    "messaging.status",
    "messaging.resolve_delivery",
    "messaging.restore_route",
    "messaging.bind_conversation",
    "messaging.bind_provisioned_destination",
    "messaging.wake_target",
    "messaging.send",
    "messaging.profile.settings",
    "messaging.profile.enrollment_begin",
    "messaging.profile.enrollment_complete",
    "messaging.profile.enable",
    "messaging.profile.policy",
    "messaging.profile.pair",
    "messaging.profile.unpair",
];

/// Gateway settings. The provider key stays in memory and is never sent to clients.
pub struct GatewayConfig {
    pub bind: SocketAddr,
    pub library_root: PathBuf,
    /// Empty generates a random 256-bit token. Supply a saved token for native clients.
    pub token: String,
    pub provider: Config,
    pub adapters: Vec<messaging::AdapterConfig>,
    pub messaging_audio: Option<Arc<dyn messaging::AudioService>>,
    #[cfg(test)]
    telegram_transport: Option<Arc<dyn hermes_telegram::Transport>>,
}

impl GatewayConfig {
    pub fn new(library_root: impl Into<PathBuf>, provider: Config) -> Self {
        Self {
            bind: ([127, 0, 0, 1], 0).into(),
            library_root: library_root.into(),
            token: String::new(),
            provider,
            adapters: Vec::new(),
            messaging_audio: None,
            #[cfg(test)]
            telegram_transport: None,
        }
    }
}

pub struct Gateway;

/// A running listener. Drop cancels its listener and active turns.
pub struct RunningGateway {
    address: SocketAddr,
    shared: Arc<Shared>,
    task: Option<JoinHandle<Result<()>>>,
    scheduler_task: Option<JoinHandle<()>>,
}

impl RunningGateway {
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn token(&self) -> &str {
        &self.shared.token
    }
    pub fn http_url(&self) -> String {
        format!("http://{}", self.address)
    }
    pub fn websocket_url(&self) -> String {
        format!("ws://{}/api/ws", self.address)
    }
    pub async fn configure_adapter(&self, config: messaging::AdapterConfig) -> Result<()> {
        self.shared.messaging.configure(&self.shared, config).await
    }
    pub async fn set_adapter_policy(&self, name: &str, policy: messaging::AccessPolicy) -> Result<()> {
        self.shared.messaging.policy(name, policy).await
    }
    pub async fn stop_adapter(&self, name: &str) -> bool {
        self.shared.messaging.stop(name).await
    }
    pub async fn adapter_settings(&self) -> Result<Vec<messaging::ConnectionSettings>> {
        self.shared.messaging.settings().await
    }
    pub async fn begin_adapter_enrollment(&self, name: &str, ttl: Duration) -> Result<messaging::EnrollmentLease> {
        self.shared.messaging.begin_enrollment(name, ttl.as_millis().try_into()?).await
    }
    pub async fn complete_adapter_enrollment(
        &self, name: &str, lease: &str, credential: messaging::EnrollmentCredential,
    ) -> Result<()> {
        self.shared.messaging.complete_enrollment(&self.shared, name, lease, credential).await
    }
    pub async fn enable_adapter(&self, name: &str, enabled: bool) -> Result<()> {
        self.shared.messaging.enable(&self.shared, name, enabled).await
    }
    pub async fn bind_adapter_receiver(&self, receiver: &str, names: &[String]) -> Result<messaging::SharedReceiver> {
        self.shared.messaging.bind_receiver(self.shared.library.root(), receiver, names).await
    }
    pub async fn messaging_status(&self) -> Result<Value> {
        self.shared.messaging.status(&self.shared.profile_id).await
    }
    pub async fn resolve_delivery(
        &self, connection: String, delivery: String, part: u32, attempt: u32,
        resolution: messaging::DeliveryResolution,
    ) -> Result<bool> {
        self.shared.messaging.resolve(connection, delivery, part, attempt, resolution).await
    }
    pub async fn bind_messaging_conversation(&self, destination: &str, binding: &str) -> Result<()> {
        self.shared.messaging.bind_conversation(destination,binding).await
    }
    pub async fn bind_messaging_provisioned_destination(
        &self,
        connection: &str,
        delivery: &str,
        part: u32,
        attempt: u32,
    ) -> Result<String> {
        self.shared.messaging.bind_provisioned_destination(
            self.shared.library.root(), connection, delivery, part, attempt,
        ).await
    }
    pub async fn set_messaging_wake_target(&self, connection: &str, target: messaging::WakeTarget) -> Result<()> {
        self.shared.messaging.set_wake_target(connection,target).await
    }
    pub fn send_messaging_notification(&self, route: &str, request: &str, text: String) -> Result<String> {
        self.shared.messaging.manual_notification(route,request,text)
    }
    /// Publish a committed native reply with its source and audience retained.
    pub fn publish_messaging_result(&self, source: &str, route: &str, request: &str) -> Result<String> {
        self.shared.messaging.attributed_result(source, route, request)
    }
    pub async fn restore_messaging_route(&self, route: String) -> Result<bool> {
        self.shared.messaging.restore_route(route).await
    }
    pub async fn shutdown(mut self) -> Result<()> {
        self.shared.shutdown.cancel();
        self.shared.stop_turns().await;
        if let Some(task) = self.scheduler_task.take() { let _ = task.await; }
        if let Some(task) = self.task.take() {
            task.await.context("Gateway task stopped unexpectedly")??;
        }
        Ok(())
    }
}

impl Drop for RunningGateway {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
    }
}

impl Gateway {
    pub async fn start(config: GatewayConfig) -> Result<RunningGateway> {
        fs::create_dir_all(&config.library_root)?;
        let lock = owner::try_profile_lock(&config.library_root)?.context("An owner is running for this profile")?;
        let name = hermes_library::profiles::name_for_home(&config.library_root).to_owned();
        Self::start_profile(config, "default".into(), name, lock).await
    }
    async fn start_profile(mut config: GatewayConfig, profile_id: String, profile_name: String, owner_lock: File) -> Result<RunningGateway> {
        ensure!(
            config.bind.ip().is_loopback(),
            "The Rust gateway only binds to a loopback address"
        );
        if config.token.is_empty() {
            config.token = format!("{}{}", new_id(), new_id());
        }
        ensure!(
            config.token.len() >= 32
                && config.token.len() <= 512
                && config.token.bytes().all(|b| b.is_ascii_graphic()),
            "Gateway token must be 32-512 printable ASCII characters without spaces"
        );
        let mut names = std::collections::HashSet::new();
        for adapter in &config.adapters {
            messaging::Host::validate(adapter)?;
            ensure!(names.insert(adapter.name.clone()), "Duplicate messaging connection name");
        }
        let library = Library::open(&config.library_root)?;
        let messaging = messaging::Host::open(library.root(), config.messaging_audio)?;
        let scheduler_store = hermes_engine::cron::Store::open_gateway(library.root())?;
        let listener = TcpListener::bind(config.bind)
            .await
            .context("Could not bind gateway")?;
        let address = listener.local_addr()?;
        let auth = auth::Auth::load(library.root(), &config.token, address)?;
        let shutdown = CancellationToken::new();
        #[cfg(not(test))]
        let telegram = telegram_host::Host::new(library.root(), shutdown.clone());
        #[cfg(test)]
        let telegram = match config.telegram_transport.take() {
            Some(transport) => telegram_host::Host::with_transport(library.root(), shutdown.clone(), transport),
            None => telegram_host::Host::new(library.root(), shutdown.clone()),
        };
        let authority_epoch = owner::advance_authority_epoch(library.root())?;
        let shared = Arc::new(Shared {
            _owner_lock: owner_lock,
            authority_epoch,
            profile_id,
            memory: profile_memory::NativeSlot::new(library.clone(), &profile_name, config.provider.clone()),
            plugins: hermes_engine::plugins::PluginRuntime::for_profile(
                library.root().to_owned(), profile_name.clone(), Some(config.provider.clone()),
            ),
            other_plugin_profiles: Mutex::new(HashMap::new()),
            python_memory: python_memory::PythonSlot::default(),
            profile_name,
            memory_task: Mutex::new(None),
            library,
            provider: config.provider,
            token: config.token,
            address,
            epoch: new_id(),
            instance_id: new_id(),
            shutdown,
            sessions: Mutex::new(HashMap::new()),
            bot_lifecycle_locks: Mutex::new(HashMap::new()),
            tickets: auth::Tickets::default(),
            auth,
            terminal: terminal::Host::new(),
            telegram,
            messaging,
            creates: Mutex::new(HashMap::new()),
            catalog_edits: Mutex::new(()),
            admissions: StdMutex::new(()),
            turns: Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
            gateway_tasks: tokio_util::task::TaskTracker::new(),
            scheduler: scheduler_store.clone(),
            scheduler_notify: tokio::sync::Notify::new(),
            webhook_watches: Mutex::new(std::collections::HashSet::new()),
        });
        shared.recover_queued().await?;
        shared.start_python_memory().await;
        webhooks::recover(&shared).await?;
        shared.telegram.restore(Some(Arc::downgrade(&shared)));
        for config in config.adapters {
            shared.messaging.restore(&shared, config).await?;
        }
        let scheduler_task = scheduler::start(shared.clone(), scheduler_store);
        if stock_plugins::has_memory_plugin() {
            let owner = shared.clone();
            *shared.memory_task.lock().await = Some(tokio::spawn(owner.run_memory_capture()));
            shared.selected_memory().await;
        }
        let router = Router::new()
            .route("/health", get(health))
            .route("/", get(auth::landing))
            .route("/api/status", get(auth::status))
            .route("/api/chat/file-upload", post(upload::receive).layer(axum::extract::DefaultBodyLimit::max(upload::MAX_BYTES + 65536)))
            .route("/api/auth/ws-ticket", post(mint_ticket))
            .route("/api/auth/providers", get(auth::providers))
            .route("/api/auth/me", get(auth::me))
            .route("/login", get(auth::login))
            .route("/auth/password-login", post(auth::password_login).layer(axum::extract::DefaultBodyLimit::max(16_384)))
            .route("/auth/native/authorize", get(auth::native_authorize))
            .route("/auth/native/token", post(auth::native_token).layer(axum::extract::DefaultBodyLimit::max(16_384)))
            .route("/auth/native/refresh", post(auth::native_refresh).layer(axum::extract::DefaultBodyLimit::max(16_384)))
            .route("/auth/logout", post(auth::logout))
            .route("/api/profiles", get(rest::profiles))
            .route("/api/profiles/active", get(rest::active))
            .route("/api/profiles/sessions", get(rest::profile_sessions))
            .route("/api/profiles/sessions/sidebar", get(rest::sidebar))
            .route("/api/sessions", get(rest::sessions))
            .route("/api/sessions/{session_id}/messages", get(rest::history))
            .route("/api/ws", get(upgrade))
            .route("/api/webapp/socket-ticket", post(terminal::ticket))
            .route("/api/host-terminal", get(terminal::upgrade))
            .route("/webhooks/{name}", post(webhooks::receive))
            .with_state(shared.clone());
        let stop = shared.shutdown.clone();
        let on_stop = shared.clone();
        let task = tokio::spawn(async move {
            let result = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .context("Gateway listener failed");
            on_stop.telegram.shutdown().await;
            on_stop.messaging.shutdown().await;
            on_stop.stop_turns().await;
            on_stop.gateway_tasks.close();
            on_stop.gateway_tasks.wait().await;
            result
        });
        Ok(RunningGateway {
            address,
            shared,
            task: Some(task),
            scheduler_task: Some(scheduler_task),
        })
    }
}

fn new_id() -> String {
    Uuid::new_v4().simple().to_string()
}
fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}
fn now() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}
fn epoch_time(value: &str) -> f64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|v| v.timestamp_millis() as f64 / 1000.0)
        .unwrap_or(0.0)
}
fn same_secret(left: &str, right: &str) -> bool {
    let mut difference = left.len() ^ right.len();
    for (a, b) in left.bytes().zip(right.bytes()) {
        difference |= (a ^ b) as usize;
    }
    difference == 0
}

struct ListFilter {
    limit: usize,
    title: Option<String>,
    query: Option<String>,
    workspace: Option<PathBuf>,
    live_only: bool,
    canonical: bool,
    compact: bool,
}

struct Shared {
    authority_epoch: u64,
    profile_id: String,
    profile_name: String,
    memory: Arc<profile_memory::NativeSlot>,
    plugins: Arc<hermes_engine::plugins::PluginRuntime>,
    other_plugin_profiles: Mutex<HashMap<PathBuf, Arc<hermes_engine::plugins::PluginRuntime>>>,
    python_memory: python_memory::PythonSlot,
    memory_task: Mutex<Option<JoinHandle<()>>>,
    library: Library,
    provider: Config,
    token: String,
    address: SocketAddr,
    epoch: String,
    instance_id: String,
    shutdown: CancellationToken,
    sessions: Mutex<HashMap<String, Arc<LiveSession>>>,
    bot_lifecycle_locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
    tickets: auth::Tickets,
    auth: auth::Auth,
    terminal: terminal::Host,
    telegram: Arc<telegram_host::Host>,
    messaging: messaging::Host,
    creates: Mutex<HashMap<String, (String, String)>>,
    catalog_edits: Mutex<()>,
    // Serialize the final admission commit with an idle provider restart.
    admissions: StdMutex<()>,
    turns: Mutex<Vec<JoinHandle<()>>>,
    connections: AtomicUsize,
    gateway_tasks: tokio_util::task::TaskTracker,
    scheduler: hermes_engine::cron::Store,
    scheduler_notify: tokio::sync::Notify,
    webhook_watches: Mutex<std::collections::HashSet<(String, String)>>,
    // Drop the scheduler and other owned resources before the profile becomes available.
    _owner_lock: File,
}

#[derive(Clone)]
struct Peer {
    id: String,
    tx: mpsc::Sender<Value>,
    stop: CancellationToken,
    canonical: bool,
    // A private local control-socket grant establishes native caller provenance.
    pub(crate) local_native: bool,
}

const TELEGRAM_SOURCE: &str = "telegram";
const TELEGRAM_ROUTE_PREFIX: &str = "telegram-route:";

#[derive(Clone, Default, Serialize, Deserialize)]
struct Settings {
    #[serde(default)]
    source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channel_route: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    creation_key: Option<String>,
    #[serde(default)]
    creation_fingerprint: Option<String>,
    #[serde(default)]
    metadata_revision: u64,
    #[serde(default)]
    execution_generation: u64,
    #[serde(default)]
    generation_options: GenerationOptions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_binding: Option<CaptureBinding>,
    #[serde(default)]
    shell_policy: Option<shell_exec::Policy>,
}

impl Settings {
    fn accepts_profile_memory(&self) -> bool {
        !matches!(self.source.as_str(), "subagent" | "recovered" | "import")
    }

    fn is_telegram_session(&self) -> bool {
        self.source == TELEGRAM_SOURCE && self.creation_fingerprint.is_none()
            && self.creation_key.as_deref().is_some_and(|key| key.starts_with(TELEGRAM_ROUTE_PREFIX))
    }

    fn read(dir: &Path) -> Result<Self> {
        match File::open(dir.join("gateway.json")) {
            Ok(file) => serde_json::from_reader(file).context("Invalid gateway session settings"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
    fn save(&self, dir: &Path) -> Result<()> {
        let mut file = NamedTempFile::new_in(dir)?;
        serde_json::to_writer_pretty(&mut file, self)?;
        file.write_all(b"\n")?;
        file.as_file().sync_all()?;
        file.persist(dir.join("gateway.json"))
            .context("Could not save gateway settings")?;
        Ok(())
    }
}

struct LiveSession {
    summary: StdMutex<SessionSummary>,
    dir: PathBuf,
    settings: StdMutex<Settings>,
    config: StdMutex<Config>,
    engine: Mutex<Session>,
    processes: hermes_engine::ProcessControls,
    member_engines: Mutex<HashMap<String, Session>>,
    member_controls: StdMutex<HashMap<String, hermes_engine::ProcessControls>>,
    writer: Arc<ConversationWriter>,
    view: StdMutex<View>,
    peers: StdMutex<HashMap<String, Peer>>,
}

struct View {
    history: History,
    running: bool,
    generation: u64,
    revision: u64,
    started: Option<f64>,
    cancel: Option<CancellationToken>,
    current_admission: Option<String>,
    corrections: Option<(async_channel::Sender<hermes_engine::TurnCorrection>, async_channel::Receiver<hermes_engine::TurnCorrection>)>,
    correction_receipts: VecDeque<(String, ring::digest::Digest)>,
    clarifications: BTreeMap<String, Clarification>,
    approvals: BTreeMap<String, Approval>,
    authority: Authority,
    sequence: u64,
    replay: VecDeque<Value>,
    replay_bytes: usize,
    evicted_through: u64,
    mutations: VecDeque<(String, String, Value)>,
}

struct Clarification {
    payload: Value,
    expires_ms: i64,
    answer: async_channel::Sender<String>,
}
struct Approval {
    payload: Value,
    action: ApprovalAction,
    tool: Option<String>,
    expires_ms: i64,
    timeout: CancellationToken,
}

enum ApprovalAction {
    Tool(async_channel::Sender<bool>),
    Identity { peer: String, apply: Box<dyn FnOnce() -> Result<()> + Send> },
}

impl Approval {
    fn tool_is_live(&self) -> bool {
        matches!(&self.action, ApprovalAction::Tool(answer) if !answer.is_closed())
            && self.expires_ms > chrono::Utc::now().timestamp_millis()
    }

    fn decline(&self) {
        if let ApprovalAction::Tool(answer) = &self.action {
            let _ = answer.try_send(false);
        }
    }
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
    data: Value,
}
type RpcResult = std::result::Result<Value, RpcError>;
impl RpcError {
    fn new(code: i64, reason: &str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: json!({"reason":reason}),
        }
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new(-32602, "invalid_params", message)
    }
    fn unsupported(message: impl Into<String>) -> Self {
        Self::new(4001, "unsupported_control", message)
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(-32603, "internal_error", error.to_string())
    }
    fn fenced(reason: &str, message: &str, view: &View) -> Self {
        Self {
            code: 4001,
            message: message.into(),
            data: json!({"reason":reason,"revision":view.revision,"execution_generation":view.generation}),
        }
    }
    fn value(self) -> Value {
        json!({"code":self.code,"message":self.message,"data":self.data})
    }
}

impl Shared {
    async fn bot_lifecycle_lock(&self, name: &str) -> Arc<RwLock<()>> {
        self.bot_lifecycle_locks.lock().await
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    async fn bot_read_guard(&self, name: &str) -> OwnedRwLockReadGuard<()> {
        self.bot_lifecycle_lock(name).await.read_owned().await
    }

    async fn try_bot_read_guard(&self, name: &str) -> Option<OwnedRwLockReadGuard<()>> {
        self.bot_lifecycle_lock(name).await.try_read_owned().ok()
    }

    async fn bot_read_guards(&self, names: &[String]) -> Vec<OwnedRwLockReadGuard<()>> {
        let mut names = names.to_vec();
        names.sort();
        names.dedup();
        let mut guards = Vec::with_capacity(names.len());
        for name in names {
            guards.push(self.bot_read_guard(&name).await);
        }
        guards
    }

    async fn bot_write_guard(&self, name: &str) -> OwnedRwLockWriteGuard<()> {
        self.bot_lifecycle_lock(name).await.write_owned().await
    }

    async fn stop_turns(&self) {
        for session in self.sessions.lock().await.values() {
            if let Some(cancel) = &lock(&session.view).cancel {
                cancel.cancel();
            }
            for peer in lock(&session.peers).values() {
                peer.stop.cancel();
            }
        }
        let tasks = std::mem::take(&mut *self.turns.lock().await);
        for task in tasks {
            let _ = task.await;
        }
        if let Some(task) = self.memory_task.lock().await.take() {
            let _ = task.await;
        }
        self.stop_python_memory().await;
        let _ = self.plugins.close().await;
        // A concurrent bot deletion can still find each profile's workers
        // here until their exits have been confirmed.
        let profiles: Vec<_> = self.other_plugin_profiles.lock().await.values().cloned().collect();
        for runtime in profiles {
            let _ = runtime.close().await;
        }
    }

    /// Stop plugins of bots that were deleted, so their folders can be removed, and disabled ones.
    async fn retire_other_plugin_profiles(&self) {
        let roster = self.library.bots().ok().map(|bots| {
            bots.iter().filter_map(|bot| self.library.bot_profile(&bot.name).ok()).collect::<std::collections::HashSet<_>>()
        });
        let mut live = Vec::new();
        let mut gone = Vec::new();
        // An unreadable roster says nothing about which bots exist.
        self.other_plugin_profiles.lock().await.retain(|path, runtime| {
            let keep = roster.as_ref().is_none_or(|roster| roster.contains(path));
            if keep { live.push(runtime.clone()) } else { gone.push(runtime.clone()) }
            keep
        });
        for runtime in gone { runtime.unload().await; }
        for runtime in live { runtime.retire_stale().await; }
    }

    async fn plugin_runtime(&self, profile: &Path) -> Arc<hermes_engine::plugins::PluginRuntime> {
        if profile == self.library.root() {
            return self.plugins.clone();
        }
        self.other_plugin_profiles.lock().await.entry(profile.to_owned()).or_insert_with(|| {
            hermes_engine::plugins::PluginRuntime::new(profile.to_owned(), Some(self.provider.clone()))
        }).clone()
    }

    fn has_session_capacity(sessions: &mut HashMap<String, Arc<LiveSession>>) -> bool {
        if sessions.len() >= MAX_SESSIONS {
            sessions.retain(|_, session| Arc::strong_count(session) > 1 || !Self::detached_idle(session));
        }
        sessions.len() < MAX_SESSIONS
    }

    fn detached_idle(session: &LiveSession) -> bool {
        !lock(&session.view).running && lock(&session.peers).is_empty() && !session.processes.has_running_jobs()
            && !lock(&session.member_controls).values().any(|controls| controls.has_running_jobs())
    }

    async fn session(&self, id: &str) -> std::result::Result<Arc<LiveSession>, RpcError> {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get(id) {
            let bot = lock(&session.summary).bot.clone().filter(|name| !name.starts_with("group:"));
            let _bot_guard = if let Some(name) = bot.as_deref() {
                Some(self.try_bot_read_guard(name).await.ok_or_else(|| RpcError::new(
                    4001, "bot_deleting", "This bot is being deleted; retry after the operation completes",
                ))?)
            } else { None };
            return Ok(session.clone());
        }
        if !Self::has_session_capacity(&mut sessions) {
            return Err(RpcError::new(
                4001,
                "session_limit",
                "Close an idle session before opening another (limit 64)",
            ));
        }
        let summary = self.library.session(id).map_err(|error|
            RpcError::new(4004, "not_found", format!("Cannot open session: {error:#}")))?;
        let bot = summary.bot.as_deref().filter(|name| !name.starts_with("group:"));
        let _bot_guard = if let Some(name) = bot {
            let guard = self.try_bot_read_guard(name).await.ok_or_else(|| RpcError::new(
                4001, "bot_deleting", "This bot is being deleted; retry after the operation completes",
            ))?;
            if !self.library.bots().map_err(RpcError::internal)?.iter().any(|bot| bot.name == name) {
                return Err(RpcError::new(4004, "not_found", "This bot no longer exists"));
            }
            Some(guard)
        } else { None };
        let dir = self.library.session_dir(id).map_err(|error| {
            if error.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) {
                return RpcError::new(4004, "not_found", "The conversation directory is missing");
            }
            RpcError::internal(error)
        })?;
        let mut settings = Settings::read(&dir).map_err(RpcError::internal)?;
        // A scheduled job names its own working directory; a project the chat belongs to must not
        // move its tools somewhere else.
        let workspace = match settings.cwd.clone().filter(|_| settings.source == "scheduled") {
            Some(cwd) => Some(cwd),
            None => self.library.workspace_for_session(id).map_err(RpcError::internal)?.or_else(|| settings.cwd.clone()),
        };
        let actual_workspace = workspace.clone().unwrap_or_else(|| dir.join("workspace"));
        let profile = match summary.bot.as_deref() {
            Some(name) if !name.starts_with("group:") => self.library.bot_profile(name).map_err(RpcError::internal)?,
            _ => self.library.root().to_owned(),
        };
        let engine = if settings.channel_route.is_some() {
            if !hermes_engine::has_conversation(&dir).map_err(RpcError::internal)? {
                return Err(RpcError::new(4004, "not_found", "The channel conversation is missing"));
            }
            Session::open_channel(&dir, &profile)
        } else if settings.is_telegram_session() {
            Session::open_text_only(&dir, workspace.as_deref(), &profile)
        } else if settings.source == "scheduled" {
            Session::open_scheduled(&dir, &actual_workspace, &profile)
        } else {
            let enrolled = profile == self.library.root() && self.selected_memory().await.native();
            Session::open_for_profile_with_enrollment(&dir, workspace.as_deref(), &profile, enrolled)
        };
        let mut engine = engine.map_err(|e| {
                RpcError::new(
                    4001,
                    "session_busy",
                    format!("Could not open session: {e:#}"),
                )
            })?;
        let processes = engine.enable_local_processes(id).map_err(RpcError::internal)?;
        if !settings.is_telegram_session() {
            engine.set_plugin_runtime(self.plugin_runtime(&profile).await);
        }
        settings.cwd = Some(actual_workspace);
        if settings.source.is_empty() {
            settings.source = "rust".into();
        }
        let history = engine.history().clone();
        let writer = engine.writer();
        let authority = Authority::recover(&writer, &dir, &settings, history.turns.len()).map_err(RpcError::internal)?;
        let generation = authority.generation.max(history.turns.len() as u64).max(settings.execution_generation);
        let mut config = self.provider.clone();
        if let Some(model) = &settings.model {
            config.model.clone_from(model);
        }
        let session = Arc::new(LiveSession {
            summary: StdMutex::new(summary),
            dir,
            settings: StdMutex::new(settings.clone()),
            config: StdMutex::new(config),
            writer,
            engine: Mutex::new(engine),
            processes,
            member_engines: Mutex::new(HashMap::new()),
            member_controls: StdMutex::new(HashMap::new()),
            peers: StdMutex::new(HashMap::new()),
            view: StdMutex::new(View {
                history,
                running: false,
                generation,
                revision: generation * 2 + settings.metadata_revision,
                started: None,
                cancel: None,
                current_admission: None,
                corrections: None,
                correction_receipts: VecDeque::new(),
                clarifications: BTreeMap::new(),
                approvals: BTreeMap::new(),
                authority,
                sequence: 0,
                replay: VecDeque::new(),
                replay_bytes: 0,
                evicted_through: 0,
                mutations: VecDeque::new(),
            }),
        });
        sessions.insert(id.into(), session.clone());
        Ok(session)
    }

    fn emit(&self, session: &LiveSession, kind: &str, payload: Value) {
        let id = lock(&session.summary).id.clone();
        let mut view = lock(&session.view);
        self.emit_locked(session, &id, &mut view, kind, payload);
    }

    fn emit_locked(&self, session: &LiveSession, id: &str, view: &mut View, kind: &str, payload: Value) {
        // Every way a native request ends is announced here; a remote prompt for it must end too.
        let ended: Vec<String> = match kind {
            "approval.settled" | "clarify.settled" => payload["request_id"].as_str().map(str::to_owned).into_iter().collect(),
            "approval.cancelled" => payload["request_ids"].as_array().into_iter().flatten()
                .filter_map(|id| id.as_str().map(str::to_owned)).collect(),
            _ => Vec::new(),
        };
        self.messaging.close_requests(id, &ended);
        view.sequence += 1;
        let mut params = json!({"type":kind,"session_id":id,"payload":payload,
            "seq":view.sequence,"replay_epoch":self.epoch,"authority_epoch":self.authority_epoch,"execution_generation":view.generation});
        if let Some(admission) = &view.current_admission {
            params["admission_id"] = json!(admission);
        }
        let size = params.to_string().len();
        if size <= MAX_REPLAY_BYTES {
            view.replay_bytes += size;
            view.replay.push_back(params.clone());
        } else {
            view.evicted_through = view.sequence;
        }
        while view.replay.len() > MAX_REPLAY_EVENTS || view.replay_bytes > MAX_REPLAY_BYTES {
            if let Some(event) = view.replay.pop_front() {
                view.replay_bytes -= event.to_string().len();
                view.evicted_through = view.evicted_through.max(event["seq"].as_u64().unwrap_or(0));
            }
        }
        let frame = json!({"jsonrpc":"2.0","method":"event","params":params});
        // Keep the view lock until queued, preserving sequence order across concurrent controls.
        lock(&session.peers).retain(|_, peer| {
            if peer.tx.try_send(frame.clone()).is_err() {
                peer.stop.cancel();
                false
            } else {
                true
            }
        });
    }

    fn info(&self, session: &LiveSession) -> Value {
        let summary = lock(&session.summary);
        let settings = lock(&session.settings);
        let config = lock(&session.config);
        let view = lock(&session.view);
        json!({ "desktop_protocol":PROTOCOL,"version":concat!(env!("CARGO_PKG_VERSION"),"-rust"),
            "model":config.model,"provider":provider_label(&config),"profile_id":self.profile_id,"profile_name":self.profile_name,
            "title":summary.title,"source":settings.source,"stored_session_id":summary.id,
            "project_id":summary.project_id,"generation_options":settings.generation_options,"cwd":settings.cwd.as_ref().map(|p|p.to_string_lossy().to_string()).unwrap_or_default(),
            "running":view.running,"pending":view.authority.pending(&self.profile_id, &summary.id, self.authority_epoch),"execution_generation":view.generation,"revision":view.revision,
            "turn_started_at":view.started,"started_at":epoch_time(&summary.created_at),
            "approval_mode":"manual","yolo":false,"tools":{"rust":if settings.is_telegram_session() { vec![] } else { tool_names() }},"skills":{},"mcp_servers":[],
            "terminal_backend":"rust","replay_epoch":self.epoch,"last_sequence":view.sequence,
            "credential_warning": if config.api_key.is_empty() && config.base_url.contains("openrouter.ai") { Some("Set HERMES_RUST_API_KEY or OPENROUTER_API_KEY before starting the gateway") } else { None }
        })
    }

    fn snapshot(&self, session: &LiveSession, peer: &Peer, omit: bool) -> Value {
        let info = self.info(session);
        let id = lock(&session.summary).id.clone();
        let view = lock(&session.view);
        let messages = transcript(&id, &view.history, Some(&view.authority));
        let prompts: Vec<_> = view.approvals.values().map(|p| p.payload.clone()).collect();
        let mut result = json!({"session_id":id,"stored_session_id":id,"message_count":messages.len(),
            "messages":if omit {vec![]} else {messages},"info":info,"running":view.running,
            "messages_omitted":omit,"open_requests":view.clarifications.values().map(|question|&question.payload).collect::<Vec<_>>(),"pending_approval":prompts.first(),
            "turn_started_at":view.started});
        if peer.canonical {
            result["authority_epoch"] = json!(self.authority_epoch);
            result["replay_epoch"] = json!(self.epoch);
            result["last_sequence"] = json!(view.sequence);
            result["subscription_id"] = json!(format!("{}:{id}", peer.id));
            result["revision"] = json!(view.revision);
            result["execution_generation"] = json!(view.generation);
            result["pending"] = view.authority.pending(&self.profile_id, &id, self.authority_epoch);
            result["latest_receipt"] = json!(view.authority.receipts.iter().rev().find(|r| r.status == "terminal").map(|r| r.value(&self.profile_id, &id, self.authority_epoch, true)));
            if !omit {
                let mut history = view.history.clone();
                for turn in &mut history.turns { turn.context.clear(); }
                result["native_history"] = json!(history);
            }
            result["prompts"] = json!(prompts);
        }
        result
    }

    async fn dispatch(self: &Arc<Self>, method: &str, params: &Value, peer: &Peer) -> RpcResult {
        if method == "shell.exec" {
            return shell_exec::exec(self, params, peer).await;
        }
        params
            .as_object()
            .ok_or_else(|| RpcError::invalid("params must be an object"))?;
        let label = optional_string(params, "profile")?;
        let explicit = optional_string(params, "profile_id")?;
        let nested = optional_string(&params["ref"], "profile_id")?;
        if label.is_some_and(|p| p != self.profile_name && p != self.profile_id)
            || [explicit, nested].into_iter().flatten().any(|p| p != self.profile_id)
        {
            return Err(RpcError::new(4001, "unsupported_profile", "This gateway serves a different profile"));
        }
        match method {
            "ping" => Ok(json!({"pong":true})),
            "gateway.ping" => Ok(json!({"ok":true})),
            "runtime.describe" => Ok(
                json!({"instance_id":self.instance_id,"profile_id":self.profile_id,"authority_epoch":self.authority_epoch,"capabilities":RUNTIME_CAPABILITIES,"session_create":{"sources":["cli","tui","gui","acp","tool","oneshot"],"parameters":["request_id","source","cwd","model","provider","title","cols"]},"skin":{}}),
            ),
            "rpc.discover" => Ok(
                json!({"implementation":"hermes-rust","version":env!("CARGO_PKG_VERSION"),"protocols":["legacy",PROTOCOL],"methods":METHODS.iter().chain(methods::METHODS).collect::<Vec<_>>(),"limitations":["one running turn per session","event replay is bounded to the current process"]}),
            ),
            "gateway.capabilities" => Ok(json!({"per_session_exclusive_submit":true})),
            "client.capabilities" => Ok(
                json!({"server_requests":[],"declines_not_shown":false,"canonical_clarify_batch":false}),
            ),
            "cron.chat" => {
                if !peer.canonical { return Err(RpcError::unsupported("A native session is required")); }
                let id = required_string(params, "job_id")?;
                let job = self.scheduler.jobs().map_err(RpcError::internal)?.into_iter().find(|job| job.id == id)
                    .ok_or_else(|| RpcError::new(4004, "not_found", "Scheduled job not found"))?;
                let session = scheduler::routine_for(self, &job.id, job.title()).await.map_err(RpcError::internal)?;
                let sid = lock(&session.summary).id.clone();
                Ok(json!({"session_id":sid}))
            }
            "cron.trigger" => {
                if !peer.canonical { return Err(RpcError::unsupported("A native session is required")); }
                let id = required_string(params, "job_id")?;
                let request_id = required_string(params, "request_id")?;
                let claim = {
                    let _admissions = lock(&self.admissions);
                    if self.shutdown.is_cancelled() { return Err(RpcError::new(4001, "shutdown", "Gateway is shutting down")); }
                    self.scheduler.claim_gateway_now(id, request_id, chrono::Utc::now()).map_err(RpcError::internal)?
                };
                self.scheduler_notify.notify_one();
                let run = tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let run = self.scheduler.runs(id)?.into_iter().find(|run| run.id == claim.run.id).context("Run no longer exists")?;
                        if run.gateway_session.is_some() || !run.status.live() { return Ok::<_, anyhow::Error>(run); }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }).await.map_err(|_| RpcError::internal("Scheduler has not bound this run yet; retry with the same request_id"))?.map_err(RpcError::internal)?;
                Ok(json!({"run_id":run.id,"session_id":run.gateway_session,"status":run.status,"error":run.error}))
            }
            "session.create" => self.create(params, peer).await,
            "session.list" | "session.active_list" => {
                self.list(params, peer, method == "session.active_list")
                    .await
            }
            "session.resume" | "session.activate" => {
                let id = required_string(params, "session_id")?;
                if params["detach_others"] == true {
                    for (previous, session) in self.sessions.lock().await.iter() {
                        if previous != id {
                            lock(&session.peers).remove(&peer.id);
                        }
                    }
                }
                let session = self.session(id).await?;
                lock(&session.peers).insert(peer.id.clone(), peer.clone());
                Ok(self.snapshot(
                    &session,
                    peer,
                    params["omit_messages"].as_bool().unwrap_or(false),
                ))
            }
            "session.history" => {
                let session = self.resolve(params).await?;
                let sid = lock(&session.summary).id.clone();
                let view = lock(&session.view);
                let messages = transcript(&sid, &view.history, Some(&view.authority));
                Ok(json!({"count":messages.len(),"messages":messages}))
            }
            "session.info" => {
                let session = self.resolve(params).await?;
                Ok(self.info(&session))
            }
            "session.status" => {
                let session = self.resolve(params).await?;
                let view = lock(&session.view);
                Ok(json!({"output":if view.running {"Running"} else {"Idle"}}))
            }
            "session.events.since" => self.replay(params, peer).await,
            "file.attach" => self.attach_file(params,peer).await,
            "prompt.submit" => self.submit(params, peer).await,
            "prompt.resume_parked" => self.resume_parked(params, peer).await,
            "prompt.resolve_unknown" => self.resolve_unknown(params, peer).await,
            "prompt.replace_queued" | "session.steer" | "session.redirect" | "clarify.respond" => {
                self.native_turn_control(method, params, peer).await
            }
            "prompt.receipt" => {
                let session = self.attached(params, peer).await?;
                let id = required_string(params, "admission_id")?;
                let sid = lock(&session.summary).id.clone();
                let view = lock(&session.view);
                let receipt = view.authority.receipts.iter().find(|r| r.id == id).ok_or_else(|| {
                    RpcError::new(
                        4004,
                        "not_found",
                        "Admission not retained; refetch session history",
                    )
                })?;
                Ok(receipt.value(&self.profile_id, &sid, self.authority_epoch, params["include_result"].as_bool().unwrap_or(false)))
            }
            "session.interrupt" | "prompt.cancel" => {
                self.interrupt(params, peer, method == "prompt.cancel")
                    .await
            }
            "catalog.session.set" | "catalog.session.delete" | "catalog.archive_inactive" | "catalog.bot.delete" => self.catalog_edit(method,params,peer).await,
            "plugins.memory.sync" => self.sync_memory_plugin(peer).await,
            "plugins.memory.archive_search" => self.user_archive_search(params, peer).await,
            "commands.catalog" => self.commands_catalog(params, peer).await,
            "slash.exec" => self.slash_exec(params, peer).await,
            "memory.providers" => self.python_memory_providers(peer).await,
            "memory.provider.set" => self.set_python_memory_provider(params, peer).await,
            "process.list" | "process.kill" | "process.stop" => {
                let session = self.attached(params, peer).await?;
                let mut controls = vec![session.processes.clone()];
                controls.extend(lock(&session.member_controls).values().cloned());
                match method {
                    "process.list" => {
                        let mut rows: Vec<Value> = Vec::new();
                        for control in controls {
                            for row in control.list().await.map_err(RpcError::internal)? {
                                if !rows.iter().any(|existing| existing["session_id"] == row["session_id"]) { rows.push(row); }
                            }
                        }
                        Ok(json!({"processes":rows}))
                    }
                    "process.stop" => {
                        let mut ids = std::collections::BTreeSet::new();
                        for control in &controls {
                            for row in control.list().await.map_err(RpcError::internal)? {
                                if row["status"] == "running" && let Some(id) = row["session_id"].as_str() { ids.insert(id.to_owned()); }
                            }
                        }
                        let mut killed = 0;
                        for id in ids {
                            for control in &controls {
                                if let Some(result) = control.kill(&id).await.map_err(RpcError::internal)? {
                                    if result["status"] == "killed" { killed += 1; }
                                    break;
                                }
                            }
                        }
                        Ok(json!({"killed":killed,"delegations":0}))
                    }
                    _ => {
                        let id = required_string(params, "process_id")?;
                        for control in controls {
                            if let Some(result) = control.kill(id).await.map_err(RpcError::internal)? { return Ok(result); }
                        }
                        Err(RpcError::new(4044, "not_found", "No such process in this conversation"))
                    }
                }
            }
            "approval.pending" => {
                let session = self.attached(params, peer).await?;
                let approvals: Vec<_> = lock(&session.view)
                    .approvals
                    .values()
                    .map(|a| a.payload.clone())
                    .collect();
                Ok(json!({"approvals":approvals}))
            }
            "approval.received" => {
                let session = self.attached(params, peer).await?;
                let id = required_string(params, "request_id")?;
                Ok(json!({"acknowledged":lock(&session.view).approvals.contains_key(id)}))
            }
            "approval.respond" => self.respond(params, peer).await,
            method if method.starts_with("messaging.") && !peer.local_native => {
                Err(RpcError::new(4403, "permission_denied", "Messaging operations require the private profile client"))
            }
            "messaging.profile.settings" => Ok(json!({"connections":self.messaging.settings().await.map_err(RpcError::internal)?})),
            "messaging.profile.enrollment_begin" => {
                let name = required_string(params, "name")?;
                let ttl = params["ttl_ms"].as_i64().unwrap_or(300_000);
                Ok(json!(self.messaging.begin_enrollment(name, ttl).await.map_err(RpcError::internal)?))
            }
            "messaging.profile.enrollment_complete" => {
                let name = required_string(params, "name")?;
                let lease = required_string(params, "lease")?;
                let credential = messaging::EnrollmentCredential::new(required_string(params, "credential")?.to_owned())
                    .map_err(|_| RpcError::invalid("Invalid adapter credential"))?;
                self.messaging.complete_enrollment(self, name, lease, credential).await.map_err(RpcError::internal)?;
                Ok(json!({"verified":true}))
            }
            "messaging.profile.enable" => {
                let name = required_string(params, "name")?;
                let enabled = params["enabled"].as_bool().ok_or_else(|| RpcError::invalid("enabled is required"))?;
                self.messaging.enable(self, name, enabled).await.map_err(RpcError::internal)?;
                Ok(json!({"enabled":enabled}))
            }
            "messaging.profile.policy" => {
                let name = required_string(params, "name")?;
                let policy = serde_json::from_value(params["policy"].clone())
                    .map_err(|_| RpcError::invalid("Invalid adapter policy"))?;
                self.messaging.policy(name, policy).await.map_err(RpcError::internal)?;
                Ok(json!({"saved":true}))
            }
            "messaging.profile.pair" => {
                let paired = self.messaging.pairing(required_string(params, "name")?,
                    required_string(params, "code")?, None).await.map_err(RpcError::internal)?;
                Ok(json!({"paired":paired}))
            }
            "messaging.profile.unpair" => {
                let removed = self.messaging.pairing(required_string(params, "name")?,
                    required_string(params, "actor")?, Some(required_string(params, "room")?))
                    .await.map_err(RpcError::internal)?;
                Ok(json!({"removed":removed}))
            }
            "messaging.status" => self.messaging.status(&self.profile_id).await.map_err(RpcError::internal),
            "messaging.bind_conversation" => {
                self.messaging.bind_conversation(required_string(params,"destination")?, required_string(params,"binding")?)
                    .await.map_err(RpcError::internal)?;
                Ok(json!({"bound":true}))
            }
            "messaging.bind_provisioned_destination" => {
                let id = |name| params[name].as_u64().and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| RpcError::invalid(format!("{name} is required")));
                let route = self.messaging.bind_provisioned_destination(
                    self.library.root(), required_string(params, "connection")?,
                    required_string(params, "delivery")?, id("part")?, id("attempt")?,
                ).await.map_err(RpcError::internal)?;
                Ok(json!({"route":route}))
            }
            "messaging.wake_target" => {
                let target = serde_json::from_value(params["target"].clone())
                    .map_err(|error| RpcError::invalid(error.to_string()))?;
                self.messaging.set_wake_target(required_string(params,"connection")?, target)
                    .await.map_err(RpcError::internal)?;
                Ok(json!({"configured":true}))
            }
            "messaging.send" => {
                let id = self.messaging.manual_notification(required_string(params,"route")?,
                    required_string(params,"request_id")?, required_string(params,"text")?.to_owned())
                    .map_err(RpcError::internal)?;
                Ok(json!({"delivery_id":id}))
            }
            "messaging.restore_route" => {
                let route = required_string(params, "route")?.to_owned();
                let restored = self.messaging.restore_route(route).await.map_err(RpcError::internal)?;
                Ok(json!({"restored":restored}))
            }
            "messaging.resolve_delivery" => {
                let connection = required_string(params, "connection")?.to_owned();
                let delivery = required_string(params, "delivery")?.to_owned();
                let id = |name| params[name].as_u64().and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| RpcError::invalid(format!("{name} is required")));
                let resolution = match required_string(params, "resolution")? {
                    "delivered" => messaging::DeliveryResolution::Delivered,
                    "discard" => messaging::DeliveryResolution::Discard,
                    _ => return Err(RpcError::invalid("resolution must be delivered or discard")),
                };
                let resolved = self.messaging.resolve(connection, delivery, id("part")?, id("attempt")?, resolution)
                    .await.map_err(RpcError::internal)?;
                Ok(json!({"resolved":resolved}))
            }
            "telegram.status" | "telegram.connect" | "telegram.disconnect" | "telegram.send" | "telegram.resolve_delivery" if !peer.local_native => {
                Err(RpcError::new(4403, "permission_denied", "Telegram setup and manual replies require the private profile client"))
            }
            "telegram.status" => {
                let mut status = self.telegram.status().await.map_err(RpcError::internal)?;
                if let Some(bot) = status["bot"]["id"].as_i64()
                    && let Some(issues) = status["work"]["issues"].as_array_mut()
                {
                    for issue in issues.iter_mut().filter(|issue| issue["stage"] == "turn_unknown") {
                        if let (Some(session), Some(update), Some(message)) = (issue["session_id"].as_str(), issue["update_id"].as_i64(), issue["message"].as_str())
                            && let Ok(session) = self.session(session).await
                            && let Some(receipt) = self.submission_receipt(&session, &telegram_host::submission_key(bot, update, message))
                                .or_else(|| self.submission_receipt(&session, &telegram_host::legacy_submission_key(bot, update)))
                        {
                            issue["admission_id"] = receipt["admission_id"].clone();
                            issue["execution_generation"] = receipt["execution_generation"].clone();
                        }
                    }
                }
                Ok(status)
            }
            "telegram.connect" => self.telegram.connect(Some(Arc::downgrade(self))).await.map_err(RpcError::internal),
            "telegram.disconnect" => self.telegram.disconnect().await.map_err(RpcError::internal),
            "telegram.resolve_delivery" => {
                let bot_id = params["bot_id"].as_i64().filter(|id| *id > 0)
                    .ok_or_else(|| RpcError::invalid("A positive bot_id is required"))?;
                let update_id = params["update_id"].as_i64().filter(|id| *id >= 0)
                    .ok_or_else(|| RpcError::invalid("A nonnegative update_id is required"))?;
                let attempt = params["attempt"].as_i64().filter(|n| *n > 0)
                    .ok_or_else(|| RpcError::invalid("A positive attempt is required"))?;
                let resolution = match required_string(params, "resolution")? {
                    "delivered" => hermes_telegram::DeliveryResolution::Delivered,
                    "discard" => hermes_telegram::DeliveryResolution::Discard,
                    _ => return Err(RpcError::invalid("resolution must be delivered or discard")),
                };
                if !self.telegram.resolve_delivery(bot_id, update_id, attempt, resolution).await.map_err(RpcError::internal)? {
                    return Err(RpcError::new(4001, "stale_delivery", "No uncertain Telegram delivery matches this attempt"));
                }
                Ok(json!({"resolved":true}))
            }
            "telegram.send" => {
                let user_id = params["user_id"].as_i64().filter(|id| *id > 0)
                    .ok_or_else(|| RpcError::invalid("user_id must be a positive Telegram user ID"))?;
                let text = required_string(params, "text")?;
                self.telegram.send(user_id, text.to_owned()).await.map_err(RpcError::internal)
            },
            "session.title" | "session.mutate" => {
                self.mutate(params, peer, method == "session.mutate").await
            }
            "session.close" => {
                let id = required_string(params, "session_id")?;
                let session = self.attached(params, peer).await?;
                let mut sessions = self.sessions.lock().await;
                lock(&session.peers).remove(&peer.id);
                if Arc::strong_count(&session) == 2 && Self::detached_idle(&session) {
                    sessions.remove(id);
                }
                Ok(json!({"closed":true}))
            }
            _ => self.profile_method(method, params, peer).await,
        }
    }

    async fn resolve(&self, params: &Value) -> std::result::Result<Arc<LiveSession>, RpcError> {
        let id = required_string(params, "session_id")?;
        self.session(id).await
    }
    async fn attached(
        &self,
        params: &Value,
        peer: &Peer,
    ) -> std::result::Result<Arc<LiveSession>, RpcError> {
        let session = self.resolve(params).await?;
        if peer.canonical && !lock(&session.peers).contains_key(&peer.id) {
            return Err(RpcError::new(
                4001,
                "not_attached",
                "Resume the session on this socket before controlling it",
            ));
        }
        Ok(session)
    }

    async fn create(self: &Arc<Self>, params: &Value, peer: &Peer) -> RpcResult {
        reject_options(
            params,
            &[
                "messages",
                "parent_session_id",
                "reasoning",
                "toolsets",
                "skills",
                "base_url",
                "api_key",
                "max_turns",
                "client_env",
                "editor",
                "run_budget",
                "row_cwd",
            ],
        )?;
        for name in [
            "hidden",
            "room_plumbing",
            "follow_profile_config",
            "yolo",
            "safe_mode",
            "ignore_rules",
            "ignore_user_config",
            "checkpoints",
            "accept_hooks",
            "pass_session_id",
            "close_on_disconnect",
        ] {
            if params[name].as_bool() == Some(true) {
                return Err(RpcError::unsupported(format!(
                    "session.create option {name} is not supported"
                )));
            }
        }
        if params["fast"].as_bool() == Some(true)
            || optional_string(params, "service_tier")?.is_some()
        {
            return Err(RpcError::unsupported(
                "Service tier selection is not implemented by this provider runtime",
            ));
        }
        if let Some(provider) = optional_string(params, "provider")?
            && !provider.is_empty()
            && provider != provider_label(&self.provider)
        {
            return Err(RpcError::unsupported(
                "Only the gateway's configured provider is available; select its model instead",
            ));
        }
        let title = optional_string(params, "title")?
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("New conversation");
        if title.chars().count() > 200 {
            return Err(RpcError::invalid("title is limited to 200 characters"));
        }
        let model = optional_string(params, "model")?.map(str::to_owned);
        if matches!(&model,Some(s) if s.trim().is_empty() || s.len()>256) {
            return Err(RpcError::invalid("model must contain 1-256 bytes"));
        }
        let cwd = optional_string(params, "cwd")?
            .filter(|s| !s.is_empty())
            .map(|s| {
                let path = fs::canonicalize(s)
                    .map_err(|e| RpcError::invalid(format!("cwd cannot be opened: {e}")))?;
                if !path.is_dir() {
                    return Err(RpcError::invalid("cwd must be an existing directory"));
                }
                Ok(path)
            })
            .transpose()?;
        let source = optional_string(params, "source")?.unwrap_or(if peer.canonical {
            "gui"
        } else {
            "desktop"
        });
        if source.len() > 64 {
            return Err(RpcError::invalid("source is limited to 64 bytes"));
        }
        if source == "channel" {
            return Err(RpcError::invalid("Channel sessions are created by the messaging gateway"));
        }
        let key = optional_string(params, "request_id")?
            .or(optional_string(params, "idempotency_key")?)
            .map(str::to_owned);
        if key.as_deref().is_some_and(|id| id.starts_with("channel-session-")) {
            return Err(RpcError::invalid("Request identity is reserved by the messaging gateway"));
        }
        if matches!(&key,Some(k) if k.is_empty() || k.len()>256) {
            return Err(RpcError::invalid(
                "request identity must contain 1-256 bytes",
            ));
        }
        if source == TELEGRAM_SOURCE || key.as_deref().is_some_and(|key| key.starts_with(TELEGRAM_ROUTE_PREFIX)) {
            return Err(RpcError::invalid("Telegram route identities are owned by the connector"));
        }
        let project = optional_string(params, "project_id")?;
        let mut options: GenerationOptions = match params.get("generation_options") {
            Some(value) => serde_json::from_value(value.clone()).map_err(|e| RpcError::invalid(e.to_string()))?,
            None => GenerationOptions::default(),
        };
        if let Some(effort) = optional_string(params, "reasoning_effort")? { options.reasoning_effort = Some(effort.into()); }
        options.validate().map_err(|e| RpcError::invalid(e.to_string()))?;
        let mut creation = json!({"title":title,"model":model,"cwd":cwd,"source":source});
        if let Some(project) = project { creation["project_id"] = json!(project); }
        if params.get("generation_options").is_some() || params.get("reasoning_effort").is_some() { creation["generation_options"] = json!(options); }
        let fingerprint = creation.to_string();
        // One admission lock covers durable lookup and creation, including reconnect retries.
        let mut creates = self.creates.lock().await;
        let mut existing = None;
        if let Some(key) = &key {
            if let Some((old_fingerprint, id)) = creates.get(key) {
                if old_fingerprint != &fingerprint {
                    return Err(RpcError::new(
                        4001,
                        "request_conflict",
                        "Creation request identity was reused with different options",
                    ));
                }
                existing = Some(id.clone());
            } else {
                for summary in self.library.sessions(None).map_err(RpcError::internal)? {
                    // An unreadable file cannot prove a match; the catalog's key still
                    // fences a duplicate, and the create below then reports the damage.
                    let Ok(settings) = Settings::read(&summary.directory) else {
                        continue;
                    };
                    if settings.creation_key.as_ref() == Some(key) {
                        if settings.creation_fingerprint.as_ref() != Some(&fingerprint) {
                            return Err(RpcError::new(
                                4001,
                                "request_conflict",
                                "Creation request identity was reused with different options",
                            ));
                        }
                        existing = Some(summary.id);
                        break;
                    }
                }
            }
        }
        let id = if let Some(id) = existing {
            id
        } else {
            if !Self::has_session_capacity(&mut *self.sessions.lock().await) {
                return Err(RpcError::new(
                    4001,
                    "session_limit",
                    "Close an idle session before creating another",
                ));
            }
            let mut prepared = Settings {
                source: source.into(), channel_route: None, model, cwd, creation_key: key.clone(),
                creation_fingerprint: Some(fingerprint.clone()), metadata_revision: 0,
                execution_generation: 0, generation_options: options, memory_binding: None, shell_policy: None,
            };
            let default_project = if project.is_none() {
                self.library.default_project().map_err(RpcError::internal)?
            } else {
                None
            };
            let project = project.or_else(|| default_project.as_ref().map(|project| project.id.as_str()));
            let selected_memory = self.refresh_memory().await.map_err(RpcError::internal)?;
            let local_backend = (peer.local_native && matches!(source, "cli" | "tui" | "gui"))
                .then(|| shell_exec::backend(self.library.root()));
            let summary = self.library.create_initialized_session_with_workspace(project, Some(title), key.as_deref(), |summary, project_workspace| {
                if summary.directory.join("gateway.json").exists() {
                    let existing = Settings::read(&summary.directory)?;
                    ensure!(existing.creation_key == prepared.creation_key && existing.creation_fingerprint == prepared.creation_fingerprint,
                        "Creation identity was previously used with different options");
                } else {
                    if prepared.accepts_profile_memory() {
                        prepared.memory_binding = selected_memory.binding(summary)?;
                    }
                    if let Some(backend) = local_backend {
                        let cwd = project_workspace.map(Path::to_owned)
                            .or_else(|| prepared.cwd.clone())
                            .unwrap_or_else(|| summary.directory.join("workspace"));
                        prepared.shell_policy = Some(shell_exec::Policy { cwd, backend });
                    }
                    prepared.save(&summary.directory)?;
                }
                Ok(())
            }).map_err(RpcError::internal)?;
            summary.id
        };
        if let Some(key) = key {
            if creates.len() >= 1024 {
                creates.clear();
            }
            creates.insert(key, (fingerprint, id.clone()));
        }
        let session = self.session(&id).await?;
        lock(&session.peers).insert(peer.id.clone(), peer.clone());
        Ok(self.snapshot(&session, peer, false))
    }

    async fn list(self: &Arc<Self>, params: &Value, peer: &Peer, active: bool) -> RpcResult {
        if params["cursor"].is_string() {
            return Err(RpcError::unsupported(
                "Cursor pagination is not implemented; use limit",
            ));
        }
        let limit = params["limit"].as_u64().unwrap_or(200).min(1000) as usize;
        let title = optional_string(params, "title")?;
        let query = optional_string(params, "query")?.map(str::to_lowercase);
        let workspace = optional_string(params, "workspace")?.map(PathBuf::from);
        let scope = optional_string(params, "scope")?.unwrap_or(if peer.canonical {
            "live"
        } else {
            "stored"
        });
        if !matches!(scope, "live" | "stored") {
            return Err(RpcError::invalid("scope must be live or stored"));
        }
        let filter = ListFilter {
            limit,
            title: title.map(str::to_owned),
            query,
            workspace,
            live_only: active || (peer.canonical && scope == "live"),
            canonical: peer.canonical,
            compact: active && !peer.canonical,
        };
        // Snapshot the live table so disk reads never hold the gateway-wide session lock.
        let sessions: HashMap<String, Arc<LiveSession>> = self.sessions.lock().await.clone();
        let shared = Arc::clone(self);
        let rows = tokio::task::spawn_blocking(move || shared.list_rows(&sessions, &filter))
            .await
            .map_err(RpcError::internal)??;
        if peer.canonical {
            Ok(json!({"sessions":rows,"scope":scope,"next_cursor":null}))
        } else {
            Ok(json!({"sessions":rows}))
        }
    }

    /// One unreadable conversation is left out rather than hiding every other row.
    fn list_rows(
        &self,
        sessions: &HashMap<String, Arc<LiveSession>>,
        filter: &ListFilter,
    ) -> Result<Vec<Value>, RpcError> {
        let mut rows = Vec::new();
        for summary in self.library.sessions(None).map_err(RpcError::internal)? {
            if rows.len() >= filter.limit {
                break;
            }
            if filter.live_only && !sessions.contains_key(&summary.id) {
                continue;
            }
            if filter.title.as_ref().is_some_and(|t| t != &summary.title) {
                continue;
            }
            if filter.query.as_ref().is_some_and(|q| {
                !summary.title.to_lowercase().contains(q) && !summary.id.contains(q)
            }) {
                continue;
            }
            let Ok(settings) = Settings::read(&summary.directory) else {
                eprintln!("Skipped conversation {} with unreadable gateway settings", summary.id);
                continue;
            };
            let cwd = self
                .library
                .workspace_for_session(&summary.id)
                .map_err(RpcError::internal)?
                .or(settings.cwd);
            if filter
                .workspace
                .as_ref()
                .is_some_and(|w| cwd.as_ref().is_none_or(|c| !c.starts_with(w)))
            {
                continue;
            }
            let (history, running) = if let Some(session) = sessions.get(&summary.id) {
                let view = lock(&session.view);
                (view.history.clone(), view.running)
            } else {
                match read_history(&summary.directory) {
                    Ok(history) => (history, false),
                    Err(_) => {
                        eprintln!("Skipped conversation {} with an unreadable transcript", summary.id);
                        continue;
                    }
                }
            };
            let messages = transcript(&summary.id, &history, None);
            let model = settings
                .model
                .unwrap_or_else(|| self.provider.model.clone());
            let preview = history
                .turns
                .first()
                .map(|t| shorten(&t.prompt, 160))
                .unwrap_or_default();
            let last_reply = history.turns.last().and_then(|t| {
                t.entries.iter().rev().find_map(|e| match e {
                    Entry::Assistant { text, .. } => Some(shorten(text, 160)),
                    _ => None,
                })
            });
            if filter.compact {
                rows.push(json!({"current":false,"id":summary.id,"last_active":epoch_time(&summary.updated_at),
                    "message_count":messages.len(),"model":model,"preview":preview,"session_key":summary.id,
                    "source":settings.source,"started_at":epoch_time(&summary.created_at),"status":if running {"running"} else {"idle"},"title":summary.title}));
            } else {
                let mut row = json!({"id":summary.id,"title":summary.title,"preview":preview,"started_at":epoch_time(&summary.created_at),"message_count":messages.len(),"source":settings.source});
                if filter.canonical {
                    row["session_id"] = json!(summary.id);
                    row["running"] = json!(running);
                    row["model"] = json!(model);
                    row["last_reply"] = json!(last_reply);
                    row["cwd"] = json!(cwd);
                    row["workspace"] = row["cwd"].clone();
                    row["last_active_at"] = json!(epoch_time(&summary.updated_at));
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn replay(&self, params: &Value, _peer: &Peer) -> RpcResult {
        // Like the upstream shared client, reconnect replay can precede resume.
        let session = self.resolve(params).await?;
        let cursor = params["last_sequence"]
            .as_u64()
            .or_else(|| params["last_seen"].as_u64())
            .unwrap_or(0);
        let mismatch =
            optional_string(params, "replay_epoch")?.is_some_and(|epoch| epoch != self.epoch);
        let view = lock(&session.view);
        let first = view
            .replay
            .front()
            .and_then(|e| e["seq"].as_u64())
            .unwrap_or(view.sequence + 1);
        let truncated = mismatch
            || cursor > view.sequence
            || cursor < view.evicted_through
            || (cursor < view.sequence && cursor + 1 < first);
        let events: Vec<_> = if mismatch {
            vec![]
        } else {
            view.replay
                .iter()
                .filter(|e| e["seq"].as_u64().unwrap_or(0) > cursor)
                .cloned()
                .collect()
        };
        Ok(
            json!({"count":events.len(),"events":events,"latest_seq":view.sequence,"last_sequence":view.sequence,
            "truncated":truncated,"snapshot_required":truncated,"epoch":self.epoch,"replay_epoch":self.epoch,"open_requests":[]}),
        )
    }

    async fn interrupt(self: &Arc<Self>, params: &Value, peer: &Peer, admission: bool) -> RpcResult {
        let session = self.attached(params, peer).await?;
        let id = lock(&session.summary).id.clone();
        let mut view = lock(&session.view);
        if admission {
            let admission_id = required_string(params, "admission_id")?;
            let receipt = view.authority
                .receipts
                .iter()
                .find(|r| r.id == admission_id)
                .ok_or_else(|| RpcError::new(4004, "not_found", "Admission is not retained"))?;
            if receipt.status == "terminal" {
                return Ok(receipt.value(&self.profile_id, &id, self.authority_epoch, false));
            }
            if matches!(receipt.status.as_str(), "queued" | "parked") {
                let mut next = view.authority.clone();
                let pending = next.receipts.iter_mut().find(|r| r.id == admission_id).expect("found admission");
                pending.status = "terminal".into();
                pending.outcome = Some("cancelled".into());
                let result = pending.value(&self.profile_id, &id, self.authority_epoch, false);
                let round = pending.group.as_ref().map(|group| group.round.clone());
                if let Some(round) = round { next.cancel_group(&round); }
                next.persist(&session.writer,None).map_err(RpcError::internal)?;
                view.authority = next;
                view.revision += 1;
                drop(view);
                self.emit(&session,"session.info",self.info(&session));
                let owner = self.clone();
                self.gateway_tasks.spawn(async move {
                    match owner.claim_next(&session) {
                        Ok(Some(first)) => owner.spawn_worker(session, first).await,
                        Ok(None) => {},
                        Err(error) => owner.emit(&session,"error",json!({"message":error.to_string()})),
                    }
                });
                return Ok(result);
            }
            if view.current_admission.as_deref() != Some(admission_id) {
                return Err(RpcError::new(
                    4001,
                    "admission_started",
                    "The requested admission is not the active turn",
                ));
            }
        } else if peer.canonical {
            let generation = params["execution_generation"].as_u64().ok_or_else(|| {
                RpcError::invalid("execution_generation is required on native controls")
            })?;
            if generation != view.generation {
                return Err(RpcError::fenced(
                    "stale_generation",
                    "This control refers to an older turn",
                    &view,
                ));
            }
        }
        let interrupted = view.running;
        Self::stop_active_turn(&session, &mut view, !admission)?;
        if admission {
            let receipt = view.authority
                .receipts
                .iter()
                .find(|r| Some(&r.id) == view.current_admission.as_ref())
                .expect("active admission retained");
            return Ok(receipt.value(&self.profile_id, &id, self.authority_epoch, false));
        }
        if peer.canonical {
            Ok(
                json!({"ref":{"profile_id":self.profile_id,"session_id":id},"instance_id":self.instance_id,"authority_epoch":self.authority_epoch,
                "revision":view.revision,"execution_generation":view.generation,"execution_state":if view.running {"running"} else {"idle"}}),
            )
        } else {
            Ok(
                json!({"status":if interrupted {"interrupted"} else {"not_interrupted"},"interrupted":interrupted}),
            )
        }
    }

    async fn respond(&self, params: &Value, peer: &Peer) -> RpcResult {
        let session = self.attached(params, peer).await?;
        let choice = optional_string(params, "choice")?.unwrap_or("deny");
        if !matches!(choice, "once" | "deny") {
            return Err(RpcError::invalid(
                "Only the offered once and deny choices are supported",
            ));
        }
        let requested =
            optional_string(params, "prompt_id")?.or(optional_string(params, "request_id")?);
        if peer.canonical && requested.is_none() {
            return Err(RpcError::invalid("prompt_id is required"));
        }
        let mut resolved = Vec::new();
        let mut identity = Vec::new();
        {
            let mut view = lock(&session.view);
            if peer.canonical {
                let generation = params["execution_generation"]
                    .as_u64()
                    .ok_or_else(|| RpcError::invalid("execution_generation is required"))?;
                if generation != view.generation {
                    return Err(RpcError::fenced(
                        "stale_generation",
                        "This approval belongs to an older turn",
                        &view,
                    ));
                }
            }
            let ids: Vec<_> = if params["all"].as_bool() == Some(true) {
                view.approvals.keys().cloned().collect()
            } else if let Some(id) = requested {
                vec![id.to_owned()]
            } else {
                view.approvals.keys().next().cloned().into_iter().collect()
            };
            for id in ids {
                if Self::answer_approval(&mut view, &id, choice == "once") {
                    resolved.push(id);
                    continue;
                }
                let can_review = view.approvals.get(&id).is_some_and(|approval| {
                    matches!(&approval.action, ApprovalAction::Identity { peer: actor, .. }
                        if actor == &peer.id && requested == Some(id.as_str()) && peer.local_native
                            && approval.payload["execution_generation"] == view.generation
                            && approval.expires_ms > chrono::Utc::now().timestamp_millis())
                });
                if can_review
                    && let Some(Approval { action: ApprovalAction::Identity { apply, .. }, timeout, .. }) =
                        view.approvals.remove(&id)
                {
                    timeout.cancel();
                    if choice == "once" {
                        identity.push((id, apply));
                    } else {
                        resolved.push(id);
                    }
                }
            }
        }
        let mut identity_failed = false;
        for (id, apply) in identity {
            if tokio::task::spawn_blocking(apply).await.is_ok_and(|result| result.is_ok()) {
                resolved.push(id);
            } else {
                identity_failed = true;
                self.emit(&session, "approval.settled",
                    json!({"prompt_id":id,"request_id":id,"status":"failed"}));
            }
        }
        if identity_failed {
            return Err(RpcError::new(4001, "ego_identity_stale", "The Identity draft could not be saved. Review it again."));
        }
        if peer.canonical && resolved.is_empty() {
            return Err(RpcError::new(
                4001,
                "prompt_stale",
                "Approval is no longer waiting; resume to refresh",
            ));
        }
        for id in &resolved {
            self.emit(
                &session,
                "approval.settled",
                json!({"prompt_id":id,"request_id":id,"status":"resolved"}),
            );
        }
        if peer.canonical {
            Ok(json!({"status":"resolved"}))
        } else {
            Ok(json!({"resolved":resolved.len()}))
        }
    }

    async fn mutate(&self, params: &Value, peer: &Peer, canonical_method: bool) -> RpcResult {
        let session = self.attached(params, peer).await?;
        let operation = if canonical_method {
            required_string(params, "operation")?
        } else {
            "rename"
        };
        if operation == "rewind" {
            return self.rewind_committed(&session, params, peer).await;
        }
        if !matches!(operation, "rename" | "model") {
            return Err(RpcError::unsupported(format!(
                "session.mutate operation {operation} is not implemented"
            )));
        }
        let payload = if canonical_method {
            &params["payload"]
        } else {
            params
        };
        if !payload.is_object() {
            return Err(RpcError::invalid("payload must be an object"));
        }
        let request_id = if canonical_method {
            Some(required_string(params, "request_id")?)
        } else {
            None
        };
        if request_id.is_some_and(|s| s.is_empty() || s.len() > 256) {
            return Err(RpcError::invalid("request_id must contain 1-256 bytes"));
        }
        let fingerprint=json!({"operation":operation,"payload":payload,"expected_revision":params["expected_revision"],"expected_generation":params["expected_generation"]}).to_string();
        let _admissions = lock(&self.admissions);
        if self.shutdown.is_cancelled() { return Err(RpcError::new(4001, "shutdown", "Gateway is shutting down")); }
        // The lock order is shared with info(): summary, settings, config, view.
        let mut summary = lock(&session.summary);
        let mut settings = lock(&session.settings);
        let mut config = lock(&session.config);
        let mut view = lock(&session.view);
        let id = summary.id.clone();
        if let Some(request_id) = request_id {
            if let Some((_, old, value)) =
                view.mutations.iter().find(|(key, _, _)| key == request_id)
            {
                if old != &fingerprint {
                    return Err(RpcError::new(
                        4001,
                        "request_conflict",
                        "Mutation request identity was reused with different options",
                    ));
                }
                return Ok(value.clone());
            }
            let expected = params["expected_revision"]
                .as_u64()
                .ok_or_else(|| RpcError::invalid("expected_revision is required"))?;
            if expected != view.revision {
                return Err(RpcError::fenced(
                    "revision_conflict",
                    "Session changed; resume before editing",
                    &view,
                ));
            }
        }
        if operation == "model" {
            if view.running {
                return Err(RpcError::fenced(
                    "session_busy",
                    "Change the model after the turn finishes",
                    &view,
                ));
            }
            let expected = params["expected_generation"]
                .as_u64()
                .ok_or_else(|| RpcError::invalid("expected_generation is required"))?;
            if expected != view.generation {
                return Err(RpcError::fenced(
                    "stale_generation",
                    "Session execution changed",
                    &view,
                ));
            }
            if let Some(provider) = optional_string(payload, "provider")?
                && provider != provider_label(&self.provider)
            {
                return Err(RpcError::unsupported(
                    "Only the gateway's configured provider is available",
                ));
            }
            reject_options(payload, &["reasoning", "confirm"])?;
            let model = required_string(payload, "model")?;
            if model.trim().is_empty() || model.len() > 256 {
                return Err(RpcError::invalid("model must contain 1-256 bytes"));
            }
            let mut saved = settings.clone();
            saved.model = Some(model.into());
            if let Some(value) = payload.get("generation_options") {
                saved.generation_options = serde_json::from_value(value.clone()).map_err(|e| RpcError::invalid(e.to_string()))?;
                saved.generation_options.validate().map_err(|e| RpcError::invalid(e.to_string()))?;
            }
            saved.metadata_revision += 1;
            saved.save(&session.dir).map_err(RpcError::internal)?;
            *settings = saved;
            config.model = model.into();
        } else {
            let title = required_string(payload, "title")?;
            if title.trim().is_empty() || title.chars().count() > 200 {
                return Err(RpcError::invalid("title must contain 1-200 characters"));
            }
            let mut saved = settings.clone();
            saved.metadata_revision += 1;
            saved.save(&session.dir).map_err(RpcError::internal)?;
            *settings = saved;
            *summary = self
                .library
                .rename_session(&id, title)
                .map_err(RpcError::internal)?;
        }
        view.revision += 1;
        let receipt = json!({"session_id":id,"operation":operation,"revision":view.revision,"execution_generation":view.generation,"ok":true});
        if let Some(request_id) = request_id {
            view.mutations
                .push_back((request_id.into(), fingerprint, receipt.clone()));
            while view.mutations.len() > 256 {
                view.mutations.pop_front();
            }
        }
        drop(view);
        drop(config);
        drop(settings);
        drop(summary);
        self.emit(&session, "session.updated", receipt.clone());
        self.emit(&session, "session.info", self.info(&session));
        Ok(receipt)
    }

    async fn run_turn(
        self: Arc<Self>,
        session: Arc<LiveSession>,
        mut config: Config,
        mut admission: Receipt,
        cancel: CancellationToken,
    ) -> bool {
        config.model.clone_from(&admission.model);
        let id = lock(&session.summary).id.clone();
        let controls = hermes_engine::TurnControls {
            corrections: lock(&session.view).corrections.as_ref().map(|(_, receiver)| receiver.clone()),
            clarify: admission.channel.as_ref().is_some_and(|policy| policy.interactive),
            channel_tools: admission.channel.as_ref().is_some_and(|policy| policy.tools),
            channel_policy: admission.channel.as_ref().map(|channel| channel.policy.clone()).unwrap_or_default(),
        };
        let mut stream = TurnStream {
            approval_context: admission.channel.as_ref().and_then(|channel| channel.approval_context.clone()),
            unattended: admission.unattended || admission.channel.as_ref().is_some_and(|policy| !policy.interactive),
            id: admission.id.clone(),
            segment: 0,
            offset: 0,
            sealed: std::collections::HashSet::new(),
            tools: std::collections::HashSet::new(),
            results: std::collections::HashSet::new(),
        };
        self.emit(&session, "message.start", json!({"turn_id":stream.id}));
        self.emit(&session, "session.info", self.info(&session));
        let (tx, rx) = async_channel::bounded(1024);
        let python = tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            memory = self.prepare_python_memory(&session, &admission) => memory,
            // The provider may be busy in another chat. Run this turn without recall.
            _ = tokio::time::sleep(Duration::from_secs(60)) => None,
        };
        let mut engine = session.engine.lock().await;
        let initial_count = engine.history().turns.len();
        let mut members = session.member_engines.lock().await;
        let preparation: Result<()> = (|| {
            if let Some(member) = &admission.group {
                ensure!(self.library.bots()?.iter().any(|bot| bot.name == member.name), "Group member no longer exists");
                if !members.contains_key(&member.name) {
                    let profile = self.library.bot_profile(&member.name)?;
                    let variant = engine.profile_variant(&profile, &id)?;
                    if let Some(controls) = variant.process_controls() { lock(&session.member_controls).insert(member.name.clone(), controls); }
                    members.insert(member.name.clone(), variant);
                }
            }
            Ok(())
        })();
        if let Some(member) = &admission.group
            && let Some(target) = members.get_mut(&member.name)
            && let Ok(profile) = self.library.bot_profile(&member.name)
        {
            target.set_plugin_runtime(self.plugin_runtime(&profile).await);
        }
        let prior = engine.history().clone();
        let (result, history, reasoning_summaries) = if let Err(error) = preparation {
            (Err(error), prior, Vec::new())
        } else {
            let target = if let Some(member) = &admission.group {
                let target = members.get_mut(&member.name).expect("prepared member");
                target.accept_committed_history(prior);
                target
            } else { &mut *engine };
            let prompt = match &admission.group {
                Some(member) if member.first => format!("{}\u{2063}{}\u{2063}\n\n(Group chat \"{}\": you are {}. Reply in your own voice; the other members answer after you.)", admission.text, member.display, member.title, member.display),
                _ => admission.text.clone(),
            };
            if !lock(&session.settings).is_telegram_session() {
                self.selected_memory().await.configure_turn(target, &self.library, &session, &mut admission);
                target.set_python_memory(python.clone());
            }
            let result = {
                let future = target.run_controlled_turn(config, admission.options.clone(), prompt, admission.attachments.clone(), tx, cancel.clone(), controls);
                tokio::pin!(future);
                loop {
                    tokio::select! {
                        result=&mut future => break result,
                        event=rx.recv() => if let Ok(event)=event {self.handle_event(&session,event,&mut stream,&cancel);},
                    }
                }
            };
            target.clear_native_query();
            target.set_python_memory(None);
            let abandoned = result.is_err() || target.history().turns.get(initial_count).is_some_and(|turn|
                matches!(turn.status, Status::Interrupted | Status::Failed(_)));
            if abandoned && !self.shutdown.is_cancelled() && let Err(error) = target.release_abandoned_turn(initial_count).await {
                self.emit(&session, "error", json!({"message":format!("Could not release abandoned processes: {error:#}")}));
            }
            (result, target.history().clone(), target.take_reasoning_summaries())
        };
        while let Ok(event) = rx.try_recv() {
            self.handle_event(&session, event, &mut stream, &cancel);
        }
        if admission.group.is_some() { engine.accept_committed_history(history.clone()); }
        drop(members);
        drop(engine);
        let (status, error) = match result {
            Err(error) => ("error", Some(format!("{error:#}"))),
            Ok(()) => match history.turns.get(initial_count).map(|t| &t.status) {
                None if cancel.is_cancelled() => ("interrupted", None),
                Some(Status::Complete) => ("complete", None),
                Some(Status::Interrupted) => ("interrupted", None),
                Some(Status::Failed(message)) => ("error", Some(message.clone())),
                _ => (
                    "error",
                    Some("The Rust runtime ended without a terminal status".into()),
                ),
            },
        };
        let text = history
            .turns
            .get(initial_count)
            .and_then(|turn| {
                turn.entries.iter().rev().find_map(|entry| match entry {
                    Entry::Assistant { text, .. } => Some(text.clone()),
                    _ => None,
                })
            })
            .unwrap_or_default();
        let payload =
            json!({"text":text,"status":status,"error":error,"partial":status!="complete"});
        let capture = if admission.ordinary_chat && self.selected_memory().await.native() {
            self.pending_memory_capture(&session, &admission, initial_count, &history, reasoning_summaries)
        } else { None };
        let has_capture = capture.is_some();
        let committed_ns = if status == "complete" {
            chrono::Utc::now().timestamp_nanos_opt().map(|ns| ns.max(admission.admitted_ns))
        } else {
            None
        };
        let python_capture = self.pending_python_delivery(
            &session, &admission, initial_count, &history, committed_ns, python.as_ref(),
        );
        let has_python_capture = python_capture.is_some();
        {
            let mut view = lock(&session.view);
            let mut authority = view.authority.clone();
            view.revision += 1;
            let current = view.current_admission.clone();
            if let Some(receipt) = authority
                .receipts
                .iter_mut()
                .find(|r| Some(&r.id) == current.as_ref())
            {
                receipt.status = "terminal".into();
                receipt.outcome = Some(status.into());
                receipt.result = Some(payload.clone());
                receipt.capture = capture;
                receipt.committed_ns = committed_ns;
                receipt.python_delivery = python_capture;
            }
            if status != "complete" && let Some(group) = &admission.group { authority.cancel_group(&group.round); }
            if let Err(error) = authority.persist(&session.writer, Some(&history)) {
                // A failed commit leaves the on-disk claim uncertain. Fence this
                // session until the owner reopens and reconciles it.
                view.running = false;
                view.cancel = None;
                drop(view);
                self.emit(&session, "error", json!({"message":format!("Could not commit turn: {error:#}")}));
                return false;
            }
            view.history = history;
            view.authority = authority;
            let questions: Vec<_> = std::mem::take(&mut view.clarifications).into_keys().collect();
            self.messaging.close_requests(&id, &questions);
            view.corrections = None;
            let approvals = std::mem::take(&mut view.approvals);
            for approval in approvals.values() {
                approval.timeout.cancel();
                approval.decline();
            }
            let cancelled: Vec<_> = approvals.into_keys().collect();
            if !cancelled.is_empty() {
                self.emit_locked(&session, &id, &mut view, "approval.cancelled",
                    json!({"session_id":id,"stored_session_id":id,"reason":status,"cancelled_count":cancelled.len(),"request_ids":cancelled}));
            }
            view.running = false;
            view.cancel = None;
            view.started = None;
            // Publish under the same lock as the idle transition. A client reacting
            // to completion must see the committed receipt and consistent snapshot.
            self.emit_locked(&session, &id, &mut view, "message.complete", payload);
            view.current_admission = None;
        }
        if has_capture { self.memory.wake.notify_one(); }
        if has_python_capture { self.schedule_python_delivery(&id); }
        match self.library.touch_session(&id) {
            Ok(summary)=>*lock(&session.summary)=summary,
            Err(error)=>self.emit(&session,"error",json!({"message":format!("Conversation saved, but catalog update failed: {error:#}")})),
        }
        self.emit(&session, "session.info", self.info(&session));
        true
    }

    fn handle_event(
        self: &Arc<Self>,
        session: &Arc<LiveSession>,
        event: Event,
        stream: &mut TurnStream,
        cancel: &CancellationToken,
    ) {
        match event {
            Event::Subagent(progress) => self.emit(session, "subagent.progress", serde_json::to_value(&progress).unwrap_or_default()),
            Event::MemoryRecall(status) => self.emit(session, "memory.recall", serde_json::to_value(status).unwrap_or_default()),
            Event::Warning(message) => self.emit(session, "message.warning", json!({"message":message})),
            Event::Clarification { question, choices, answer } => {
                let request = new_id();
                let mut view = lock(&session.view);
                let payload = json!({"kind":"clarification","request_id":request,"execution_generation":view.generation,
                    "question":question,"choices":choices});
                view.clarifications.insert(request, Clarification {
                    payload:payload.clone(), expires_ms:chrono::Utc::now().timestamp_millis() + 300_000, answer,
                });
                drop(view);
                self.emit(session, "clarify.request", payload);
            }
            Event::Correction { text } => {
                self.emit(session,"message.correction",json!({"text":text,"turn_id":stream.id,"segment":stream.segment}));
                stream.segment += 1;
                stream.offset = 0;
            }
            Event::Text(text) => {
                self.emit(session,"message.delta",json!({"text":text,"turn_id":stream.id,"segment":stream.segment,"offset":stream.offset}));
                stream.offset += text.encode_utf16().count();
            }
            Event::Snapshot(history) => {
                if let Some(turn) = history.turns.last() {
                    for (index, entry) in turn.entries.iter().enumerate() {
                        match entry {
                            Entry::User { .. } => {}
                            Entry::Assistant { text, calls } => {
                                if !calls.is_empty()
                                    && !text.is_empty()
                                    && stream.sealed.insert(index)
                                {
                                    self.emit(session,"message.interim",json!({"text":text,"already_streamed":true,"turn_id":stream.id,"segment":stream.segment}));
                                    stream.segment += 1;
                                    stream.offset = 0;
                                }
                                for call in calls {
                                    if stream.tools.insert(call.id.clone()) {
                                        self.emit(session,"tool.start",json!({"tool_id":call.id,"name":call.name,"args":serde_json::from_str::<Value>(&call.arguments).unwrap_or(Value::Null),"args_text":call.arguments}));
                                    }
                                }
                            }
                            Entry::Tool {
                                id, name, result, ..
                            } => {
                                if stream.results.insert(id.clone()) {
                                    self.emit(session,"tool.complete",json!({"tool_id":id,"name":name,"result":serde_json::from_str::<Value>(result).unwrap_or_else(|_|json!(result)),"result_text":result}));
                                }
                            }
                        }
                    }
                }
                lock(&session.view).history = history;
            }
            Event::Approval { call, answer } => {
                if stream.unattended { let _ = answer.try_send(false); return; }
                let tool = messaging::tool_key(&call.name, &call.arguments).ok();
                if let (Some(context), Some(tool)) = (&stream.approval_context, &tool)
                    && context.authorized(tool)
                {
                    let _ = answer.try_send(true);
                    return;
                }
                let expires_ms = chrono::Utc::now().timestamp_millis() + 300_000;
                let id = new_id();
                let timeout = CancellationToken::new();
                let mut command = format!("{}({})", call.name, call.arguments);
                let key = lock(&session.config).api_key.clone();
                if !key.is_empty() {
                    command = command.replace(&key, "[redacted]");
                }
                let generation = lock(&session.view).generation;
                let payload = json!({"kind":"approval","prompt_id":id,"request_id":id,"execution_generation":generation,
                    "choices":["once","deny"],"allow_session":false,"allow_permanent":false,"tool_name":call.name,
                    "command":command,"description":"Approve this Rust tool call once?","timeout_s":300});
                lock(&session.view).approvals.insert(
                    id.clone(),
                    Approval {
                        payload: payload.clone(),
                        action: ApprovalAction::Tool(answer),
                        tool,
                        expires_ms,
                        timeout: timeout.clone(),
                    },
                );
                self.emit(session, "approval.request", payload);
                let shared = self.clone();
                let session = session.clone();
                let cancel = cancel.clone();
                self.gateway_tasks.spawn(async move {
                    tokio::select! {
                        _=timeout.cancelled()=>return,
                        _=cancel.cancelled()=>return,
                        _=tokio::time::sleep(Duration::from_secs(300))=>{},
                    }
                    let approval = lock(&session.view).approvals.remove(&id);
                    if let Some(approval) = approval {
                        approval.decline();
                        shared.emit(
                            &session,
                            "approval.settled",
                            json!({"prompt_id":id,"request_id":id,"status":"expired"}),
                        );
                    }
                });
            }
        }
    }
}

struct TurnStream {
    unattended: bool,
    approval_context: Option<messaging::ToolApprovalContext>,
    id: String,
    segment: u64,
    offset: usize,
    sealed: std::collections::HashSet<usize>,
    tools: std::collections::HashSet<String>,
    results: std::collections::HashSet<String>,
}

fn required_string<'a>(params: &'a Value, name: &str) -> std::result::Result<&'a str, RpcError> {
    params[name]
        .as_str()
        .ok_or_else(|| RpcError::invalid(format!("{name} must be a string")))
}
fn optional_string<'a>(
    params: &'a Value,
    name: &str,
) -> std::result::Result<Option<&'a str>, RpcError> {
    if params[name].is_null() {
        Ok(None)
    } else {
        required_string(params, name).map(Some)
    }
}
fn reject_options(params: &Value, names: &[&str]) -> std::result::Result<(), RpcError> {
    for name in names {
        if let Some(value) = params.get(name)
            && !value.is_null()
            && value != &json!(false)
            && value != &json!([])
            && value != &json!("")
        {
            return Err(RpcError::unsupported(format!(
                "Explicit option {name} is not implemented by the Rust gateway"
            )));
        }
    }
    Ok(())
}

fn provider_label(config: &Config) -> &'static str {
    let base = config.base_url.trim_end_matches('/');
    if hermes_engine::portal::is_portal(base) {
        "nous"
    } else if base == hermes_engine::OPENROUTER {
        "openrouter"
    } else if base.starts_with("https://api.anthropic.com/") || base.ends_with("/messages") {
        "anthropic"
    } else if base.starts_with("https://api.openai.com/") {
        "openai"
    } else {
        "openai-compatible"
    }
}
fn tool_names() -> Vec<String> {
    hermes_engine::tools::tool_specs()
        .iter()
        .filter_map(|tool| {
            tool.pointer("/function/name")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn shorten(value: &str, maximum: usize) -> String {
    value.chars().take(maximum).collect()
}
struct StoredConversation {
    history: History,
    authority: Option<Authority>,
}

fn read_record(dir: &Path) -> Result<StoredConversation> {
    let (history, authority) = hermes_engine::read_conversation(dir)?;
    let authority = authority.map(serde_json::from_value).transpose().context("Could not read conversation state")?;
    Ok(StoredConversation { history, authority })
}

fn read_history(dir: &Path) -> Result<History> {
    Ok(read_record(dir)?.history)
}
fn transcript(sid: &str, history: &History, authority: Option<&Authority>) -> Vec<Value> {
    let mut messages = Vec::new();
    let receipts: HashMap<usize, &Receipt> = authority.into_iter().flat_map(|a| a.receipts.iter())
        .filter_map(|r| r.turn_index.map(|n| (n,r))).collect();
    for (turn_index, turn) in history.turns.iter().enumerate() {
        let receipt = receipts.get(&turn_index);
        let turn_id = receipt.map(|r| r.id.clone()).unwrap_or_else(||format!("{sid}-turn-{turn_index}"));
        let first = messages.len();
        messages.push(json!({"role":"user","text":turn.prompt,"content":turn.prompt,"row_id":messages.len()+1,
            "turn_index":turn_index,"attachments":turn.attachments,"model":turn.model,"status":turn.status}));
        for entry in &turn.entries {
            match entry {
                Entry::User { text } => messages.push(json!({"role":"user","text":text,"content":text,"row_id":messages.len()+1})),
                Entry::Assistant{text,calls}=>{
                    let mut message=json!({"role":"assistant","text":text,"content":text,"row_id":messages.len()+1});
                    if !calls.is_empty() {message["tool_calls"]=json!(calls.iter().map(|call|json!({"id":call.id,"type":"function","function":{"name":call.name,"arguments":call.arguments}})).collect::<Vec<_>>());}
                    messages.push(message);
                }
                Entry::Tool{id,name,result,approved}=>messages.push(json!({"role":"tool","text":result,"content":result,"tool_call_id":id,"name":name,"approved":approved,"row_id":messages.len()+1})),
            }
        }
        for (ordinal, message) in messages[first..].iter_mut().enumerate() {
            message["turn_id"] = json!(turn_id);
            message["message_id"] = json!(format!("{turn_id}-row-{ordinal}"));
            message["model"] = json!(turn.model);
            if let Some(receipt) = receipt.filter(|receipt| receipt.admitted_ns > 0) {
                message["timestamp"] = json!(receipt.admitted_ns as f64 / 1_000_000_000.0);
            }
            message["turn_status"] = receipt.map(|r| json!(r.outcome.as_deref().unwrap_or(&r.status))).unwrap_or_else(||json!(turn.status));
        }
    }
    messages
}

// HTTP/upgrade auth. No proxy or forwarded headers can widen the loopback boundary.
fn boundary(
    shared: &Shared,
    headers: &HeaderMap,
    remote: SocketAddr,
) -> std::result::Result<(), StatusCode> {
    if shared.auth.audience(shared, headers, remote)? != auth::Audience::Local {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}
fn header_token(headers: &HeaderMap) -> Option<&str> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let session = headers
        .get("x-hermes-session-token")
        .and_then(|h| h.to_str().ok());
    match (bearer, session) {
        (Some(_), Some(_)) => None,
        (Some(token), None) | (None, Some(token)) => Some(token),
        _ => None,
    }
}
async fn health(
    State(shared): State<Arc<Shared>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = boundary(&shared, &headers, remote) {
        return status.into_response();
    }
    Json(json!({"status":"ok","implementation":"hermes-rust","version":env!("CARGO_PKG_VERSION")}))
        .into_response()
}
impl Shared {
    fn issue_ticket(&self) -> Option<String> {
        self.tickets.issue_native()
    }
}

#[derive(Deserialize)]
struct TicketQuery {
    profile: Option<String>,
}

async fn mint_ticket(
    access: auth::Access,
    State(shared): State<Arc<Shared>>,
    Query(query): Query<TicketQuery>,
) -> Response {
    if !rest::profile_matches(&shared, query.profile.as_deref(), false) {
        return auth::error(StatusCode::FORBIDDEN, "This gateway serves a different profile");
    }
    shared.tickets.mint(access)
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WsQuery {
    token: Option<String>,
    ticket: Option<String>,
    native_dial: Option<String>,
}
async fn upgrade(
    State(shared): State<Arc<Shared>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let audience = match shared.auth.audience(&shared, &headers, remote) {
        Ok(audience) => audience,
        Err(status) => return status.into_response(),
    };
    let offered = headers.get_all(header::SEC_WEBSOCKET_PROTOCOL);
    if offered.iter().any(|value| value.to_str().is_err()) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let protocols: Vec<_> = offered.iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let protocol_tickets: Vec<_> = protocols
        .iter()
        .filter_map(|p| p.strip_prefix(TICKET_PREFIX))
        .collect();
    let canonical = query.native_dial.is_some() || protocols.contains(&PROTOCOL);
    if !protocol_tickets.is_empty()
        && (protocol_tickets.len() != 1 || !protocols.contains(&PROTOCOL))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let credentials = if audience == auth::Audience::Remote {
        auth::credential_count(&headers)
    } else {
        headers.get_all(header::AUTHORIZATION).iter().count()
            + headers.get_all("x-hermes-session-token").iter().count()
    };
    let sources = usize::from(query.token.is_some())
        + usize::from(query.ticket.is_some())
        + credentials
        + protocol_tickets.len();
    if sources != 1 {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (revoked, local_native) = if let Some(ticket) = query.ticket.as_deref().or_else(|| protocol_tickets.first().copied()) {
        let Some(ticket) = shared.tickets.redeem(ticket, audience) else {
            return StatusCode::UNAUTHORIZED.into_response();
        };
        (ticket.revocation(), canonical && ticket.local_native())
    } else if audience == auth::Audience::Local && !canonical
        && query.token.as_deref().or_else(|| header_token(&headers))
            .is_some_and(|token| same_secret(token, &shared.token))
    {
        (CancellationToken::new(), false)
    } else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if shared.connections.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
        shared.connections.fetch_sub(1, Ordering::AcqRel);
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    let upgraded = ws.max_message_size(6 * 1024 * 1024).max_frame_size(6 * 1024 * 1024);
    let upgraded = if protocols.contains(&PROTOCOL) {
        upgraded.protocols([PROTOCOL])
    } else {
        upgraded
    };
    let handler = shared.gateway_tasks.token();
    let failed = Arc::downgrade(&shared);
    upgraded
        .on_failed_upgrade(move |_| {
            if let Some(shared) = failed.upgrade() { shared.connections.fetch_sub(1, Ordering::AcqRel); }
        })
        .on_upgrade(move |socket| async move {
            serve_socket(shared, socket, canonical, local_native, revoked).await;
            drop(handler);
        })
        .into_response()
}

const SOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

async fn close_websocket<S, Item>(sink: &mut S)
where
    S: futures_util::Sink<Item> + Unpin,
{
    // Closing flushes buffered output too, including a write that already
    // timed out. A stalled peer must not keep the owner alive during teardown.
    let _ = tokio::time::timeout(SOCKET_CLOSE_TIMEOUT, sink.close()).await;
}

async fn serve_socket(shared: Arc<Shared>, socket: WebSocket, canonical: bool, local_native: bool, revoked: CancellationToken) {
    let (mut sink, mut source) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Value>(256);
    let stop = shared.shutdown.child_token();
    let peer = Peer {
        id: new_id(),
        tx,
        stop: stop.clone(),
        canonical,
        local_native,
    };
    let ready = json!({"jsonrpc":"2.0","method":"event","params":{"type":"gateway.ready","payload":{
        "skin":{},"change_events":false,"heartbeat":true,"replay_epoch":shared.epoch,"session_authority":canonical,
        "instance_id":shared.instance_id,"profile_id":shared.profile_id,"authority_epoch":shared.authority_epoch}}});
    let _ = peer.tx.try_send(ready);
    let writer_stop = stop.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                _=writer_stop.cancelled()=>break,
                frame=rx.recv()=>match frame {
                    Some(frame) => {
                        let sent = tokio::select! {
                            _ = writer_stop.cancelled() => false,
                            result = tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Text(frame.to_string().into()))) => matches!(result, Ok(Ok(()))),
                        };
                        if !sent { break; }
                    }
                    None=>break,
                },
            }
        }
        rx.close();
        writer_stop.cancel();
        close_websocket(&mut sink).await;
    });
    // Keep dispatch ordered while the reader continues polling WebSocket
    // control frames. The bounded queue applies backpressure during bursts.
    let (requests, mut queued) = mpsc::channel(8);
    let read = async {
        loop {
            let incoming = tokio::select! {
                biased;
                _ = revoked.cancelled() => break,
                _ = stop.cancelled() => break,
                frame = source.next() => frame,
            };
            match incoming {
                Some(Ok(Message::Text(raw))) => {
                    let parsed = serde_json::from_str::<Value>(&raw);
                    let enqueue = async {
                        // This endpoint is stateless. Its client uses it for
                        // liveness while ordinary requests are queued.
                        if parsed.as_ref().is_ok_and(|frame| {
                            frame["jsonrpc"] == "2.0" && frame["method"] == "gateway.ping"
                        }) {
                            match socket_reply(&shared, &peer, parsed).await {
                                Some(reply) => peer.tx.send(reply).await.is_ok(),
                                None => true,
                            }
                        } else {
                            requests.send(parsed).await.is_ok()
                        }
                    };
                    let accepted = tokio::select! {
                        biased;
                        _ = revoked.cancelled() => false,
                        _ = stop.cancelled() => false,
                        accepted = enqueue => accepted,
                    };
                    if !accepted {
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            }
        }
        stop.cancel();
    };
    let dispatch = async {
        loop {
            let request = tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                request = queued.recv() => request,
            };
            let Some(request) = request else { break };
            if let Some(reply) = socket_reply(&shared, &peer, request).await
                && peer.tx.send(reply).await.is_err()
            {
                break;
            }
        }
    };
    tokio::join!(read, dispatch);
    stop.cancel();
    for session in shared.sessions.lock().await.values() {
        lock(&session.peers).remove(&peer.id);
    }
    let _ = writer.await;
    shared.connections.fetch_sub(1, Ordering::AcqRel);
}

async fn socket_reply(
    shared: &Arc<Shared>,
    peer: &Peer,
    parsed: serde_json::Result<Value>,
) -> Option<Value> {
    let (id, result) = match parsed {
        Err(_) => (
            Value::Null,
            Err(RpcError::new(-32700, "parse_error", "Invalid JSON")),
        ),
        Ok(frame) => {
            let id = frame.get("id").cloned().unwrap_or(Value::Null);
            if frame.get("id").is_none() && frame.get("method").is_none() {
                return None;
            }
            if frame["jsonrpc"] != "2.0"
                || !frame["method"].is_string()
                || !(id.is_null() || id.is_string() || id.is_number())
            {
                (
                    id,
                    Err(RpcError::new(
                        -32600,
                        "invalid_request",
                        "Expected a JSON-RPC 2.0 method request",
                    )),
                )
            } else {
                let method = frame["method"].as_str().unwrap_or("");
                let params = frame.get("params").cloned().unwrap_or_else(|| json!({}));
                let result = shared.dispatch(method, &params, peer).await;
                frame.get("id")?;
                (id, result)
            }
        }
    };
    Some(match result {
        Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
        Err(error) => json!({"jsonrpc":"2.0","id":id,"error":error.value()}),
    })
}

#[cfg(test)]
mod socket_tests;
