# Native Rust gateway

This crate runs the Rust `hermes_engine::Session::turn` over real HTTP and WebSocket transports. It does not spawn the Python gateway. It shares the desktop's `hermes-library` catalog, native transcript files, configured providers, approved workspace tools and optional MCP adapter.

It implements a **scoped subset**, checked against the supplied Hermes upstream checkout's `apps/shared/src/gateway-contract.openrpc.json`, `tui_gateway/ws.py`, `tui_gateway/contracts/canonical_sessions.py`, and Desktop `canonical-protocol.ts`. The gateway owns chat execution and serves the native Desktop over its local owner connection. Its upstream compatibility covers the documented client surfaces.

## Run

From the workspace root:

```sh
cargo run -p hermes-gateway -- gateway run --port 8765
```

`--data-dir PATH` selects the catalog; otherwise it uses `Session::default_dir()` (including `HERMES_RUST_DATA_DIR`). `--host` accepts only a loopback IP. Port `0` chooses a free port. The single startup JSON line contains the HTTP origin, WebSocket URL and token file path. Ctrl-C or SIGTERM interrupts active turns, saves their terminal state, closes clients and stops the listener.

Provider configuration is the engine's `HERMES_RUST_BASE_URL`, `HERMES_RUST_MODEL` and `HERMES_RUST_API_KEY` environment configuration. The default Nous endpoint can use the desktop's valid Portal login. A custom endpoint receives only its explicitly configured key. Provider changes made in another running desktop are not automatically applied to an already running gateway. No provider secret appears in RPC snapshots.

The CLI creates a random, private `gateway.token` in the selected data directory. On Unix it requires mode `0600` for an existing token file. `--token-file PATH` selects another file; `HERMES_RUST_GATEWAY_TOKEN` supplies a token without writing it. Tokens must contain 32–512 printable ASCII characters without spaces. The secret is never printed by the server.

## Connect

Native clients first send authenticated `POST /api/auth/ws-ticket`, using either `Authorization: Bearer <gateway token>` or `X-Hermes-Session-Token: <gateway token>`. The reply is `{"ticket":"…","ttl_seconds":30}` with `Cache-Control: no-store`.

Then open `/api/ws?native_dial=1` with both WebSocket subprotocols:

```text
hermes-gateway-v1
hermes-gateway-ticket.<ticket>
```

The server selects only `hermes-gateway-v1`. Tickets expire after 30 seconds and are atomically consumed once. Reconnects require a fresh ticket. A canonical socket must create or resume a session before controlling it. Native-mode sockets never accept the reusable token in place of a ticket.

For legacy JSON-RPC clients, open `/api/ws?ticket=<fresh ticket>` without `native_dial`, or send the token in the upgrade's bearer/session-token header. The legacy `?token=` path is also supported for existing clients; prefer a one-use ticket so the reusable credential does not enter a URL.

The first frame has method `event`, type `gateway.ready`, a replay epoch, heartbeat support and `session_authority: true` on a native dial (`false` on legacy). `runtime.describe` returns the supported creation parameters and the explicit `rust-engine-subset-v1` capability. It does not advertise full upstream `sessions-v1` support. `rpc.discover` returns the exact method list and scope limitations. The first-party `gateway.ping` heartbeat is answered throughout a running turn, including approval waits.

Private local discovery advertises `session-authority-v1` after the profile's authority is ready. Identify, profile resolution, ticket minting and `runtime.describe` share the same profile, instance and persistent authority epoch. The epoch advances on owner restart. The private control socket accepts interactive tickets scoped to its profile. The stock client discovers individual method support through `rpc.discover`.

The listener binds to loopback. Local routes require the listener's Host and accept an absent Origin, the matching loopback HTTP origin, `file://`, or `app://hermes`. Requests for the local authority carrying forwarding headers are refused.

### Published gateway

Set `webapp.public_url` in the selected profile's `config.yaml` (or set `HERMES_DASHBOARD_PUBLIC_URL`) to the HTTPS address clients use, including a mount prefix if needed. Terminate TLS at the reverse proxy, preserve the public Host, and strip the configured mount prefix when proxying the path to the Rust listener. A supplied Origin must match the configured HTTPS origin. Forwarding headers do not establish an authority.

Configure at least one remote credential:

- `HERMES_DASHBOARD_SESSION_TOKEN` provisions the stock deployment credential, either a literal token or `file:PATH`. Authenticate HTTP requests with a bearer or the `X-Hermes-Session-Token` header. The deployment credential is independent of the private local owner token. `dashboard.session_token_auth.enabled: false` disables it.
- `dashboard.basic_auth.username` plus `password_hash` accepts the stock `scrypt$n$r$p$salt$hash` format. The corresponding `HERMES_DASHBOARD_BASIC_AUTH_*` environment variables and the stock plaintext `password` setting are also supported. Sign in through `/login` for a secure browser session or through the native PKCE flow for bearer credentials. Sessions issued by the password provider expire when the gateway restarts.

After authenticating, read `/api/profiles` or `/api/profiles/active` to select the profile served by the gateway. Session lists are available at `/api/sessions`, `/api/profiles/sessions`, and `/api/profiles/sessions/sidebar`. `GET /api/sessions/{id}/messages` uses the canonical transcript projection. Its default is the latest 500 rows, ordered chronologically within the page. Use `order=latest` and `offset` to page backward, or use an explicit `limit` with the default oldest-first paging.

Mint a one-use ticket with authenticated `POST /api/auth/ws-ticket`. Browser and Desktop clients can dial with `?ticket=...`. Android-style clients can offer both WebSocket subprotocols without an Origin. Terminal tickets use `POST /api/webapp/socket-ticket?socket=host-terminal` and are redeemed only on the terminal route. Remote upgrades require a remote ticket, including when the underlying peer is a local proxy.

## Supported operations

| Methods | Behavior |
| --- | --- |
| `ping`, `gateway.ping` | Liveness; the first-party heartbeat returns `{"ok":true}`. |
| `rpc.discover`, `runtime.describe`, `gateway.capabilities`, `client.capabilities` | Honest subset discovery. Server-request RPCs and batch clarify are not advertised. |
| `session.create` | New durable native catalog session. Supports title, model, configured-provider label and an existing `cwd`; source/cols are presentation metadata. Explicit unsupported policies are refused. Creation request IDs are saved for retry deduplication across restarts. |
| `session.resume`, `session.activate` | Attach this socket to a native/imported catalog session by exact Rust ID. Return transcript, live state and pending approvals. `omit_messages` skips the transcript while retaining its count. `detach_others` releases this socket's other session attachments before opening the target. No upstream title lookup or SQLite runtime session is impersonated. |
| `session.list`, `session.active_list` | List stored/live catalog sessions, with title/query/limit and workspace filtering. No cursor pagination. Native `session.list` defaults to live scope; request `scope: "stored"` for saved sessions. |
| `session.history`, `session.info`, `session.status` | Native transcript projection and current engine state. Row IDs identify projected rows and are stable while the append-only transcript is unchanged. Rewind is not implemented. |
| `prompt.submit` | Admit one plain-text Rust turn and stream real model deltas. Native replies carry an admission receipt; legacy replies have `status: "streaming"`. One turn runs per session. Busy or explicit queued submissions are refused. |
| `prompt.receipt`, `prompt.cancel` | Read the retained admission's outcome, or request interruption of its active turn. Cancellation ACKs describe current state; wait for terminal receipt/event to observe completion. |
| `approval.pending`, `approval.received`, `approval.respond` | Real engine tool approval. Only the offered `once` and `deny` choices are accepted. Pending approvals survive socket reconnects; unanswered approvals expire after five minutes. Native responses require prompt ID and execution generation. No standing approval is granted. |
| `session.interrupt` | Cancel streaming/model/tool approval work and persist interruption. Native controls require the current execution generation, so an older control cannot stop a newer turn. |
| `session.events.since` | Bounded ordered event replay with sequence and restart epoch. A gap or different epoch explicitly requests a fresh snapshot. |
| `session.title`, `session.mutate` | Rename; canonical `session.mutate` also changes the model at an idle boundary. Canonical edits require revision fences and request IDs. Model changes and the private profile client's `rewind` also require execution generation. Rewind keeps `payload.keep_turns` committed turns and stores its retry receipt with the shortened transcript. Rename and model retries are deduplicated within retained process state. |
| `shell.exec` | Capture local composer commands from a private native attachment to a locally created session with a frozen local backend. See the supported command forms below. |
| `session.close` | Detach this socket from an idle session; release its engine lock when its last attachment closes it. It never deletes history. |

Requests use JSON-RPC 2.0 IDs and object parameters. Unknown methods return `-32601`; malformed JSON returns `-32700`; invalid arguments return `-32602`; state/policy refusals include `error.data.reason` and, for stale fences, current revision/generation. Each listener serves its configured profile. Batch JSON-RPC requests are not supported.

Stock Ink sends `!command` and `{!command}` through `shell.exec`. The gateway checks the private native attachment, session origin and frozen backend before starting a process. The session’s launch directory supplies the working directory. Commands receive no interactive input and have a 30-second execution deadline. Most use a fresh `/bin/sh` or Windows `cmd.exe`. Supported standalone Git reports use a directly resolved executable, and the Windows `more` reader uses System32’s `more.com`. Ordinary operator environment variables reach the child. Hermes-managed credentials and shell startup hooks are removed. The response carries the native exit code and the final 4,000 characters of stdout and 2,000 characters of stderr after newline normalization and redaction. Capture leaves the session transcript and admission queue untouched.

For an authenticated local composer, the session directory is the starting directory for ordinary file reads and directory listings. Relative paths such as `..\notes.txt` on Windows and `../notes.txt` on POSIX can reach a sibling file. Absolute local paths also work. A plain `!` command displays the captured output. Interpolation with `{!…}` includes it in the submitted prompt. Missing files return the native command's exit status and diagnostic. Input redirection also accepts ordinary parent paths. Output redirection retains its narrower rules for local targets.

Windows recursive and wildcard `findstr` searches support one local file operand and use a bounded list of checked files. Selected filenames must be ASCII for the native `/F` path list. Windows working-tree Git reports run with a one-process limit. Git operations that require external filters can return an error. The pip version query uses the trusted interpreter’s isolated mode, which excludes workspace and user-site module overrides.

The native classifier checks the pinned stock rule tables and recognizes platform-specific command forms, including pipelines, file redirection, conditional chains, selected file utilities and literal Python script invocations. It returns `dangerous_command` for unclassified forms. The differential driver `scripts/verify_stock_composer_shell.py` exercises both owners using the stock private client. `scripts/verify_stock_ink_composer.mjs` exercises the pinned Ink renderer and composer hooks. `scripts/compare_composer_classifiers.py` compares the two classifiers without executing the input commands. Build the Rust driver with `cargo build --locked -p hermes-engine --example composer_classifier`. Its JSON report counts differences in both directions, and a disagreement causes a nonzero exit status.

A minimal native conversation is:

1. `runtime.describe {}`
2. `session.create {"request_id":"unique-create-id","source":"gui","title":"Example","cwd":"/existing/project"}`
3. `prompt.submit {"session_id":"returned-id","submission_id":"unique-send-id","text":"Explain this project"}`
4. Consume `message.start`, `message.delta`, `message.interim`, `tool.start`, `approval.request`, `tool.complete`, `message.complete`, and `session.info` events as applicable.
5. For a tool card, send `approval.respond {"session_id":"…","prompt_id":"…","execution_generation":1,"choice":"once"}` or `"deny"`.
6. On reconnect, mint a new ticket and `session.resume` before sending or responding. Resume's `prompts` array restores cards even when their original events were missed.

Events use `{"jsonrpc":"2.0","method":"event","params":{"type":"message.delta","session_id":"…","seq":3,"replay_epoch":"…","execution_generation":1,"payload":{"text":"hello","turn_id":"…","segment":0,"offset":0}}}`. Delta offsets count UTF-16 code units, matching the first-party renderer; interim segments have independent offsets. Complete events use upstream statuses `complete`, `error` or `interrupted`. Approval events expose both canonical `prompt_id` and legacy `request_id`.

## Persistence and limits

Conversation content and the gateway's turn records are saved together in the profile's `mercury.db`. Creation identity/model/source/cwd metadata lives alongside it in `gateway.json`; the catalog remains owned by `hermes-library`. The gateway cannot open a conversation while another desktop/process holds its exclusive engine lock; it reports a `session_busy` refusal. Close the other owner before opening that conversation through the gateway. Separate sessions can run concurrently.

Replay is at most 512 events and 512 KiB per loaded session; admissions and mutation receipts retain the last 256 entries. Those caches reset on process restart. Clients must refetch history after a replay gap/restart and must not blindly resend an old ambiguous tool-producing turn outside the retained admission window. Unfinished saved turns recover as interrupted through the engine. This crate does not claim upstream's durable admission ledger or exactly-once semantics across process death.

At most 64 sessions are loaded and 32 sockets connected. WebSocket frames/messages are capped at 1 MiB and prompts at 256 KiB; a slow client is disconnected when its bounded outgoing queue fills and can reconcile from history/replay. Closing a socket leaves its active turn running, so a short transport failure does not abandon an approval. The approval deadline and explicit interruption still apply. Stop the gateway gracefully to persist shutdown interruption.

Unsupported: queued/steered/redirected inputs, multimodal attachments, edit/regenerate/rewind/branch/reset mutations, cross-profile routing, legacy server-to-client request RPCs, tool and plugin management, process/control-socket discovery, bot/room transports, billing, voice, screen/browser control, dashboard settings REST and automatic handoff to a Python gateway. Any optional MCP server is external to Rust; it is connected through the engine, not a fallback to Python.

## Headless Telegram

A native profile can opt in to automatic plain-text replies to unthreaded private DMs from allowlisted Telegram users. Each bot and chat has a durable conversation in that profile. The conversation uses the configured model and profile persona. Incoming text is literal, and these turns have no tools, file or URL expansion, workspace context, plugins, or profile memory.

Stop the profile gateway before configuring it. Put the BotFather token in a private file and use numeric Telegram user IDs in the allowlist. Telegram long polling requires a bot without a webhook. Give each bot one active consumer.

```sh
hermes-gateway gateway telegram configure --data-dir /path/to/profile --bot-token-file /path/to/private-token --allow-id 123456789
hermes-gateway gateway run --data-dir /path/to/profile
hermes-gateway gateway telegram status --data-dir /path/to/profile
```

Repeat `--allow-id` for additional users. Omit `--data-dir` to use the default profile. The setup uses the existing native credential store, which seals tokens on supported profiles with a system credential service and otherwise uses a private settings file. On Unix the input file must have owner-only permissions. Existing manual inbox configurations keep their behavior until they opt in. The native JSON inbox and polling offset are imported once into the profile database. Only newly accepted messages enqueue agent turns.

Mercury persists accepted updates before acknowledging them to Telegram. Duplicate updates reuse the original record. The connector resumes queued work and sends committed replies after restart. Each chat advances in order, and an uncertain execution or send pauses later work for that chat. Telegram's explicit rate-limit response schedules a retry. A send with a lost or ambiguous response requires an operator decision because it may already have reached the chat.

Use the bot ID and update details from `telegram status` when resolving an uncertain result.

```sh
hermes-gateway gateway telegram resolve-delivery --data-dir /path/to/profile --bot-id 123456 --update-id 1001 --attempt 1 --resolution delivered
hermes-gateway gateway telegram resolve-execution --data-dir /path/to/profile --bot-id 123456 --update-id 1002
```

Choose `--resolution delivered` after confirming the reply in Telegram, or `--resolution discard` to abandon delivery. Resolving either outcome releases the chat without resending. Resolving an uncertain execution marks that turn interrupted and releases the chat. A deleted routed conversation is reported as blocked.

## Profile memory provider

Choose a provider in Desktop under **Settings → Memory & Context**, or set `memory_provider` in the profile's `agent-settings.json`. The setting applies to subsequent chat turns.

| Setting | Behavior |
| --- | --- |
| `auto` (default) | Use the Reliquary stock plugin when the profile is already enrolled. Otherwise, use the profile's existing Markdown notes. Automatic never enrolls a profile. |
| `reliquary` | Enroll the profile and use its Reliquary owners for capture and agent-requested queries. Requires a build with the `reliquary` feature. |
| `notes` | Use Hermes' existing `MEMORY.md`, `USER.md`, and `memory` tool. The agent and user note controls apply independently. |
| `external` | Expose the designated external provider's MCP tools to the agent. Configure the HTTP MCP endpoint under **Capabilities → Connectors**, select **Use as external memory provider**, and enable memory and MCP tools. The provider defines its tools and storage behavior. |
| `off` | Disable native capture, memory tools, Markdown note injection, and the designated external memory connector. Other configured tools keep their own settings. |

Gateway and Desktop include the Reliquary stock plugin by default. Desktop lists it under **Capabilities → Plugins**. To build without it, pass `--no-default-features` to `cargo build -p hermes-gateway` or `cargo build -p hermes-desktop`. An explicit `reliquary` selection reports that the provider is unavailable in those builds. Reliquary's MPL-2.0 license and corresponding source links are recorded in [`RELIQUARY-NOTICE`](../../RELIQUARY-NOTICE) and packaged with official releases.

### Reliquary enrollment and capture

Selecting **Reliquary** enrolls a private PHY named for the profile and `general.rel` in `plugin-data/reliquary_store/<profile>/` in the Hermes data directory, so replacing the app during an update keeps them. Each project has a named REL under that profile's `projects/` folder. Project renames update the REL filename. Renaming a profile with its gateway stopped updates the folder and PHY filename. Both operations preserve owner IDs and existing chat bindings. Filenames use filesystem-safe display names and suffixes when names collide.

Existing manual enrollments still use their registered owner files and the original service lease. A profile rename updates any registered paths that moved with the profile and preserves the original owner identities and external paths. A newly created ordinary chat binds to its assigned General or project REL and the authorized private PHY. Chats created before enrollment, imported or restored chats, and background jobs are excluded from automatic capture.

Managed owners live outside profile exports. An imported profile gets independent owners on enrollment. Deleting a profile after its gateway stops removes the profile's registered managed store, including its Archive and Echo copies.

Open **Capabilities → Plugins → Reliquary → Configure** to edit the profile’s Reliquary Switchboard routes in **Settings → Memory & Context**. The Switchboard selects models for General, Insomnia, Insomnia Metadata, Entity Extraction, General Decision, Entity Resolution Decision, Memory Retrieval Decision, Entity Resolution, Dream, and Embedding. General and Entity Resolution initially follow the Hermes chat model. Insomnia and Dream fall back to General. Ownership metadata falls back to Insomnia, and decisions use a dedicated route, the shared General Decision route, or General in that order. Completed-turn processing also requires configured Entity Resolution and Embedding routes.

Save provider endpoints in Hermes’ Models settings or sign in under Providers → Accounts. A Switchboard route can reference an existing Hermes endpoint or supported account and specify a model override. Embedding needs a saved OpenAI-compatible endpoint, the model’s output dimensions, and its normalization setting. Structured completions, decisions, and embeddings all use Hermes’ provider transport and credential resolution. The profile’s `reliquary.cfg` stores Switchboard route selections in CVCFG and contains no provider credentials.

At first use, any existing `memory_embedding` selection in `agent-settings.json` supplies the initial Embedding route. Saving Switchboard settings establishes the profile’s `reliquary.cfg` as its route configuration, including when a route has been cleared. Gateway-only hosts can configure the same slots with `hermes_reliquary_plugin::save_switchboard_config`. An incompatible stored embedding identity or an unavailable route pauses processing while chat remains available.

The gateway commits the turn and its pending capture together. Archive records the user prompt, assistant commentary, and the final assistant response in order. Echo records bounded, redacted model-round activity, tool calls and results, and provider-supplied reasoning summaries. The background worker records Archive admission and semantic processing separately. Archiving or deleting a source fences future capture. If an open Reliquary import defers its Archive tombstone, the gateway retains the fence and retries the tombstone.

### Reliquary Ego

Enable **Reliquary Ego** in the persistent memory settings for a Reliquary profile. The profile gateway mounts that profile's active PHY with write authority and holds the Ego worker with the native memory mount. General and Embedding support automatic summary updates. Identity review and Pin controls also work when a summary route is unavailable.

Mercury requests the authorized Ego package after assembling the native request and reserving model output room. Mercury uses a conservative UTF-8 byte bound for the Ego package and the same fixed counter for the PHY cap and its compression tool. The approved Identity replaces the `SOUL.md` persona slot for that request. Pins and verified summaries are appended once as standing context. Native Reliquary selection excludes the built-in `MEMORY.md` and `USER.md` notes. Current-chat compaction, transcript storage, and explicit Memory, Archive and Echo tools continue through their existing paths.

Use `/identity` to view the active Identity or `/identity <exact text>` to review a draft. The local profile client approves the exact draft once through the existing approval surface. In normal native chats, `list_pins` finds Pins in an explicit `rel` or `phy` scope even when they did not fit the prompt. Edits and deletion require the listed ID and revision. A reaffirmation or softening can move priority one tier while preserving the instruction.

The native outgoing-capture test mounts a disposable profile and sends ordinary requests to a loopback model. Set `MERCURY_EGO_CAPTURE_FIXTURE` to a disposable Core fixture folder with one verified REL and PHY pair to include both summary forms in the outgoing assertion. Set `MERCURY_EGO_CAPTURE_REQUEST` to write the captured fixture request.

### Reliquary retrieval

The model can call `memory_query` to search eligible Memories and current Observations in the chat's bound General or project REL and active private PHY. The result contains one Core ranking, exact owner-qualified Memory references and exact Observation citations. `refine_query` and `traverse_graph` use authorized results from the same turn. These tools leave Freshness unchanged.

Private PHY Perception writes are opt-in. Enable **Private PHY Perception** in Persistent memory or set `memory_phy_perception_enabled` for a gateway-only profile with a configured Perception decision endpoint. Recall includes the active PHY on read-only mounts too.

The native mixed-recall test uses disposable Core fixture files `mixed.rel`, `current.phy` and `pending.phy`. Set `MERCURY_PHY_PERCEPTION_FIXTURE` to their folder and run `cargo test -p hermes-reliquary-plugin --features native native_query_returns_one_owner_qualified_ranking_and_fences_a_phy_switch -- --ignored` to exercise ordinary recall, exact references and disclosure across a PHY switch using a loopback decision endpoint.

`archive_search` accepts a natural-language time reference such as “last Tuesday.” It uses the profile's configured calendar timezone, with UTC as the fallback, and defaults to the date of the original message. Pass `basis: "mentioned"` to filter on dates discussed in the messages. A query without a resolvable time reference returns `temporal_query_required` and suggests `memory_query`. `archive_read` and `echo_read` accept only an exact reference returned to the model during the current turn. Successful retrieval content goes to the requesting model round. The stored tool entry records its result count.

In Desktop, `/archive <query>` searches the current General or project Archive. `/archive-tree <query>` includes the related General or project RELs accessible from that scope. The commands accept hybrid search without a time reference and display results in Desktop.

Archiving or restoring a chat keeps its accepted knowledge available. The private profile client's `session.mutate` rewind and `catalog.session.delete` report committed removals to Reliquary. A rewind retires the copied suffix and leaves the surviving prefix available for further capture. Delete retires the whole copy. The response's `memory_synced` indicates acknowledgment by Core. Pending work survives restart in the native chat authority or the catalog deletion journal. While a source is syncing, saved copy references for that source are fenced. A chat can be deleted at any time, including while memory is off or a copy was interrupted. Removing a chat or turns never sends their text to a model: an import that was not extracted yet is stopped, and the removal stays pending until Core can close that import. Core cannot cancel an import at the pinned revision, and until it can, that General or project REL takes no new copies. A rewind still waits for a copy that is being staged. During upgrade, the adapter clears a legacy archive fence after it verifies that the chat still exists and that Core's accepted copy remains live, or that no copy was ever accepted. Current delete notices have their own durable fence.


## Verification

```sh
cargo test -p hermes-gateway
cargo test -p hermes-gateway --test websocket stock_reliquary_enrolls_renames_and_recalls_only_on_request
cargo test -p hermes-reliquary-plugin --features native
cargo clippy -p hermes-gateway --all-targets -- -D warnings
```

The integration tests bind real loopback HTTP/WebSocket listeners and a deterministic SSE model fixture. They exercise single-use ticket redemption, subprotocol negotiation, auth/Host/Origin rejection, native create deduplication, streamed Rust tool approval, reconnect/resume, real UTC tool output, history after gateway restart, admission receipts, replay epochs, fenced rename retries, legacy UTF-16 deltas, busy admission refusal and interruption of a live provider stream. They do not require credentials or contact a paid model provider. This transport suite establishes the documented shapes; it does not certify every upstream Desktop/TUI screen.

## Original shared client port

`src/replay.rs` ports the replay state from the official **NousResearch/hermes-agent v0.23.3** `apps/shared/src/json-rpc-gateway.ts`. The baseline SHA-256 is `6271ca1b034e50df8ce88a2e3f0149a66247d0a2b989880bf8a16e6df94eda8e`. It was checked against the clean official checkout as well as the unchanged source included in this repository. The port preserves these original distinctions:

- Live event cursors advance monotonically; ordinary live delivery still includes late frames, as the original does. Replayed and parked events use sequence deduplication.
- Reconnect installs every session's hold before publishing the open state. Gap events dispatch before held live frames, then that session's history barrier resolves.
- Each session settles independently. An unsupported replay or its 10-second deadline releases its held frames; losing the owning socket resolves its barrier `false`.
- A process restart clears only legacy process cursors. Canonical session epochs remain independent, and a parked new epoch is adopted after the old replay gap.
- Truncated tails are skipped. `snapshot_required` emits `session.replay_gap` and retires that cursor/epoch. `not_found` retires the deleted session. The cursor limit is 256, with least recently advanced cursors retired through an explicit gap event.
- Old socket responses/cleanup cannot flush a replacement socket's replay hold. The process epoch is attached as the original `replayEpoch` annotation.

`src/client.rs` supplies the standalone Rust WebSocket transport for that state: native ticket minting, ws/wss URL validation, multiplexed request IDs, request deadlines and cancellation, heartbeat liveness, typed refusal reasons, pending server-request redelivery, and history barriers. It exposes a broadcast event stream rather than JavaScript callbacks. A consumer seeing `RecvError::Lagged` must re-snapshot its session. Connection errors omit credential-bearing URLs. There is no JavaScript runtime dependency in the Rust client or server.

```rust,ignore
use hermes_gateway::client::{GatewayClient, ClientMessage};
use serde_json::json;

let client = GatewayClient::default();
let mut messages = client.subscribe();
client.connect_native("http://127.0.0.1:8765", &saved_gateway_token).await?;
let session = client.request("session.create", json!({"title":"Example"})).await?;
let sid = session["session_id"].as_str().unwrap();
client.request("prompt.submit", json!({"session_id":sid,"text":"Hello"})).await?;
// Consume ClientMessage::Event values, and render/answer approval cards.
// After an explicit reconnect, wait before publishing a pending history read:
if let Some(barrier) = client.session_replay_barrier(sid) {
    if !barrier.wait().await { /* discard the old read; read again on next open */ }
}
client.close();
```

For upstream legacy server-to-client requests, set `ClientOptions.server_requests = true`, subscribe before connecting, and handle `ClientMessage::ServerRequest`. Its response handle permits the first `respond` or `fail` only. `open_requests` replay is delivered before the containing RPC resolves. The native server in this crate uses canonical approval events instead of the legacy server-request RPC path.

The machine-readable `tests/fixtures/upstream-replay.json` contains **18 vectors whose expected traces were produced by executing the original TypeScript client**, including its original channel, on synthetic sockets. `tests/replay_vectors.rs` compares the Rust implementation against those traces. The TypeScript verifier uses Node's built-in type stripping and a loader that resolves the source's `.js` imports to neighboring `.ts` files; it does not rewrite source logic, install packages or contact a service.

From this workspace directory, the optional reference check is:

```sh
node --experimental-strip-types \
  --loader ./crates/gateway/tests/ts-source-loader.mjs \
  ./crates/gateway/tests/replay-reference.mjs \
  ../shared/src/json-rpc-gateway.ts \
  ./crates/gateway/tests/fixtures/upstream-replay.json --verify
```

The live stock Node fixture uses the original Desktop and shared client sources from the pinned `caafe5389ae58` object. Run it from the workspace root with Node 24, Git, tar, and OpenSSL. It creates its own TLS proxy, synthetic credentials and deterministic model endpoint.

```sh
node scripts/verify_stock_node_gateway.mjs STOCK_CHECKOUT RUST_GATEWAY_EXECUTABLE
```

The Rust transport test additionally connects this client to the real native Rust server, disconnects it while a clock tool awaits approval, completes the turn through another viewer, and verifies one replayed terminal event after reconnect, a valid history barrier, the saved transcript, and concurrent RPCs. These checks establish the ported replay behavior and essential native flow; they do not claim a full port of the original Desktop screens, JS callback registry, or all upstream canonical mutation adapters.
