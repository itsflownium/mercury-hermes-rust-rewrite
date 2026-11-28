//! The host runs a real plugin in embedded CPython and speaks MCP stdio to it.
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

const PLUGIN: &str = r#"
import asyncio, json

print("plugins may print; this must not reach the protocol")

def _shout(args, **_):
    return json.dumps({"text": str(args.get("text", "")).upper() + "!"})

async def _later(args):
    await asyncio.sleep(0)
    return {"greeting": "hi " + args.get("name", "")}

def _broken(args):
    raise RuntimeError("boom")

def _guard(tool_name, args, **_):
    if args.get("unavailable_policy"):
        raise RuntimeError("Policy configuration is unavailable")
    if tool_name == "terminal" and "rm -rf" in args.get("command", ""):
        return {"action": "block", "message": "not on my watch"}

def register(ctx):
    schema = lambda name: {"name": name, "description": name + " tool", "parameters": {"type": "object", "properties": {}}}
    ctx.register_tool("shout_text", "shout", schema("shout_text"), _shout)
    ctx.register_tool("greet", "shout", schema("greet"), _later, is_async=True)
    ctx.register_tool("broken", "shout", schema("broken"), _broken)
    ctx.register_tool("hidden", "shout", schema("hidden"), _shout, check_fn=lambda: False)
    ctx.register_tool("needs_key", "shout", schema("needs_key"), _shout, requires_env=["HERMES_TEST_UNSET_KEY"])
    ctx.register_hook("pre_tool_call", _guard)
    ctx.register_command("shout", lambda raw: raw)
    ctx.register_tool("config_echo", "shout", schema("config_echo"), lambda args: ctx.get_config("volume", "quiet"))
"#;

struct Host { child: std::process::Child, out: Receiver<String>, next: u64 }
impl Host {
    fn new(mut child: std::process::Child) -> Self {
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (send, out) = mpsc::channel();
        std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        Self {child, out, next: 0}
    }

    fn send(&mut self, message: Value) {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(format!("{message}\n").as_bytes()).unwrap();
    }

    fn read(&mut self, method: &str) -> Value {
        let line = self.out.recv_timeout(Duration::from_secs(10)).unwrap_or_else(|err| {
            panic!("No reply from the plugin ({}) for {method}: {err}", self.child.id())
        });
        serde_json::from_str(&line).unwrap_or_else(|_| panic!("not JSON-RPC: {line:?}"))
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next += 1;
        self.send(json!({"jsonrpc": "2.0", "id": self.next, "method": method, "params": params}));
        let reply = self.read(method);
        assert_eq!(reply["id"], self.next);
        reply
    }

    fn expect_exit(&mut self) {
        drop(self.child.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "Plugin exited with {status}");
                return;
            }
            assert!(Instant::now() < deadline, "Plugin did not exit after stdin closed");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn a_plugin_registers_tools_and_hooks_through_the_rust_ctx() {
    let dir = tempfile::tempdir().unwrap();
    let plugin = dir.path().join("shout");
    std::fs::create_dir_all(&plugin).unwrap();
    std::fs::write(plugin.join("plugin.yaml"), "name: shout\n").unwrap();
    std::fs::write(plugin.join("__init__.py"), PLUGIN).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_hermes-python-host")).arg("serve").arg(&plugin)
        .env("PLUGIN_CONFIG_VOLUME", "loud").env_remove("HERMES_TEST_UNSET_KEY")
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut host = Host::new(child);

    let hello = host.request("initialize", json!({"protocolVersion": "2025-06-18"}));
    assert_eq!(hello["result"]["capabilities"]["experimental"]["hermes"]["hooks"], json!(["pre_tool_call"]));

    let tools = host.request("tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["shout_text", "greet", "broken", "config_echo"], "check_fn and requires_env hide tools");

    let text = |reply: &Value| reply["result"]["content"][0]["text"].as_str().unwrap().to_owned();
    let shout = host.request("tools/call", json!({"name": "shout_text", "arguments": {"text": "hi"}}));
    assert_eq!(text(&shout), r#"{"text": "HI!"}"#);
    assert_eq!(text(&host.request("tools/call", json!({"name": "greet", "arguments": {"name": "ana"}}))), r#"{"greeting": "hi ana"}"#);
    assert_eq!(text(&host.request("tools/call", json!({"name": "config_echo", "arguments": {}}))), "loud");
    let broken = host.request("tools/call", json!({"name": "broken", "arguments": {}}));
    assert_eq!(broken["result"]["isError"], true);
    assert!(text(&broken).contains("boom"));
    assert!(host.request("tools/call", json!({"name": "hidden", "arguments": {}}))["error"].is_object());

    let blocked = host.request("hermes/hook", json!({"name": "pre_tool_call", "kwargs": {"tool_name": "terminal", "args": {"command": "rm -rf /"}, "tool_call_id": "c1"}}));
    assert_eq!(blocked["result"]["results"], json!([{"action": "block", "message": "not on my watch"}]));
    let allowed = host.request("hermes/hook", json!({"name": "pre_tool_call", "kwargs": {"tool_name": "terminal", "args": {"command": "ls"}}}));
    assert_eq!(allowed["result"]["results"], json!([]));
    let failed = host.request("hermes/hook", json!({"name": "pre_tool_call", "kwargs": {"tool_name": "terminal", "args": {"unavailable_policy": true}}}));
    assert!(failed["error"]["message"].as_str().unwrap().contains("Policy configuration is unavailable"));
    assert!(failed.get("result").is_none());
    assert_eq!(host.request("ping", json!({}))["result"], json!({}));

    host.expect_exit();
}

#[test]
fn an_abandoned_tool_stops_at_its_next_await() {
    let dir = tempfile::tempdir().unwrap();
    let plugin = dir.path().join("slow");
    std::fs::create_dir_all(&plugin).unwrap();
    std::fs::write(plugin.join("plugin.yaml"), "name: slow\n").unwrap();
    std::fs::write(plugin.join("__init__.py"), r#"
import asyncio
async def _slow(args, **_):
    await asyncio.sleep(60)
def register(ctx):
    ctx.register_tool("slow", "slow", {"name": "slow", "description": "", "parameters": {"type": "object"}}, _slow, is_async=True)
"#).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_hermes-python-host")).arg("serve").arg(&plugin)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut host = Host::new(child);
    host.request("initialize", json!({"protocolVersion": "2025-06-18"}));
    host.send(json!({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "slow", "arguments": {}}}));
    host.send(json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 7}}));
    let reply = host.read("tools/call");
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["result"]["isError"], true);
    // The plugin goes on working.
    assert_eq!(host.request("ping", json!({}))["result"], json!({}));
    host.expect_exit();
}

#[test]
fn a_plugin_that_fails_to_load_exits_with_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let plugin = dir.path().join("needs-hermes");
    std::fs::create_dir_all(&plugin).unwrap();
    std::fs::write(plugin.join("__init__.py"), "from hermes_cli.config import load_config\ndef register(ctx): pass\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_hermes-python-host")).arg("serve").arg(&plugin)
        .env_remove("HERMES_HOME").stdin(Stdio::null()).output().unwrap();
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stderr).contains("hermes_cli"));
}

const COMMAND_AND_MEMORY: &str = r#"
from pathlib import Path

profile_activations = 0

class FixtureMemory:
    name = "Fixture Memory"

    def initialize(self, session_id, **kwargs):
        self.session_id = session_id
        self.results = []

    async def sync_turn(self, user, assistant, *, session_id):
        assert session_id == self.session_id
        self.results.append(f"{user}:{assistant}")

    def prefetch(self, query):
        return {"session_id": self.session_id, "results": self.results}

async def echo(source, session_id, **kwargs):
    return f"{session_id}:{source}:{kwargs['platform']}"

def register(host):
    global profile_activations
    handle = host.register_command(" /ECHO ", echo, "Echo the args", " words ")
    def dispose(raw):
        handle.dispose()
        handle.dispose()
    host.register_command("dispose", dispose)
    host.register_memory_provider(FixtureMemory())
    host.register_memory_observer(None)
    if hasattr(host, "llm"):
        host.register_command("session-source", lambda raw, source: source.session_id)
        def unloaded():
            global profile_activations
            profile_activations += 1
            Path(host.get_config("trace")).write_text(str(profile_activations))
        host.on_unload(unloaded)
"#;

fn launch_plugin(src: &str, allow_services: bool) -> (tempfile::TempDir, Host) {
    let dir = tempfile::tempdir().unwrap();
    let plugin = dir.path().join("fixture");
    std::fs::create_dir(&plugin).unwrap();
    std::fs::write(plugin.join("__init__.py"), src).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_hermes-python-host"));
    cmd.arg("serve")
        .arg(plugin)
        .env("PLUGIN_CONFIG_TRACE", dir.path().join("trace"))
        .env_remove("HERMES_HOME")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    if allow_services {
        cmd.env("HERMES_PLUGIN_SERVICES", "1");
    } else {
        cmd.env_remove("HERMES_PLUGIN_SERVICES");
    }
    let child = cmd.spawn().unwrap();
    (dir, Host::new(child))
}

#[test]
fn commands_and_session_memory_share_one_plugin_activation() {
    let (dir, mut host) = launch_plugin(COMMAND_AND_MEMORY, true);
    let info = host.request("hermes/describe", json!({}));
    assert_eq!(info["result"]["provider"], "Fixture Memory");
    assert_eq!(info["result"]["commands"][0], json!({
        "name": "echo", "description": "Echo the args", "args_hint": "words", "argument_mode": "text",
    }));
    assert_eq!(info["result"]["unsupported"], json!(["register_memory_observer"]));

    let context = json!({"source": {"session_id": "a"}, "session_id": "a", "platform": "local"});
    let echo = host.request("hermes/command", json!({"name": "echo", "arguments": "hello", "context": context}));
    assert_eq!(echo["result"], "a:hello:local");
    let source = host.request("hermes/command", json!({"name": "session-source", "context": context}));
    assert_eq!(source["result"], "a");
    let disposed = host.request("hermes/command", json!({"name": "dispose"}));
    assert_eq!(disposed.get("result"), Some(&Value::Null));
    assert!(host.request("hermes/command", json!({"name": "echo"})).get("error").is_some());

    for session_id in ["a", "b", "a"] {
        assert_eq!(host.request("hermes/memory.open", json!({"session_id": session_id}))["result"]["available"], true);
    }
    let invoke = |host: &mut Host, session_id: &str, method: &str, args: Value, kwargs: Value| {
        host.request("hermes/memory.call", json!({
            "session_id": session_id, "method": method, "args": args, "kwargs": kwargs,
        }))
    };
    assert!(invoke(&mut host, "a", "sync_turn", json!(["foo", "bar"]), json!({"session_id": "a"})).get("result").is_some());
    assert_eq!(invoke(&mut host, "a", "prefetch", json!(["q"]), json!({}))["result"], json!({"session_id": "a", "results": ["foo:bar"]}));
    assert_eq!(invoke(&mut host, "b", "prefetch", json!(["q"]), json!({}))["result"], json!({"session_id": "b", "results": []}));
    assert!(invoke(&mut host, "a", "__init__", json!([]), json!({})).get("error").is_some());
    let _ = host.request("hermes/memory.close", json!({"session_id": "a"}));
    assert!(invoke(&mut host, "a", "prefetch", json!(["q"]), json!({})).get("error").is_some());
    assert!(host.request("hermes/unload", json!({})).get("result").is_some());
    host.expect_exit();
    assert_eq!(std::fs::read_to_string(dir.path().join("trace")).unwrap(), "1");
}

#[test]
fn gateway_services_work_during_registration_and_an_async_command() {
    let (_dir, mut host) = launch_plugin(r#"
async def register(host):
    await host.llm.aembed(["setup"], timeout=5)
    async def ask(raw):
        answer = await host.llm.acomplete_structured(input=raw, timeout=5)
        return answer["parsed"]
    host.register_command("ask", ask)
"#, true);
    let setup = host.read("registration service request");
    assert_eq!(setup["method"], "hermes/service");
    assert!(setup["id"].as_str().unwrap().starts_with("host:"));
    assert_eq!(setup["params"]["operation"], "aembed");
    assert_eq!(setup["params"]["args"], json!([["setup"]]));
    host.send(json!({"jsonrpc": "2.0", "id": setup["id"], "result": {}}));
    host.send(json!({"jsonrpc": "2.0", "id": 1, "method": "hermes/command",
        "params": {"name": "ask", "arguments": "question"}}));
    let inference = host.read("command service request");
    assert_eq!(inference["method"], "hermes/service");
    assert_eq!(inference["params"]["operation"], "acomplete_structured");
    assert_eq!(inference["params"]["kwargs"]["input"], "question");
    host.send(json!({"jsonrpc": "2.0", "id": inference["id"], "result": {"parsed": "answer"}}));
    let reply = host.read("command reply");
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["result"], "answer");
    host.expect_exit();
}

#[test]
fn a_memory_provider_built_with_the_model_services_opens_for_each_session() {
    let (_dir, mut host) = launch_plugin(r#"
class Memory:
    name = "Uses services"
    def __init__(self, llm):
        self.llm = llm
    def initialize(self, session_id, **kwargs):
        self.session_id = session_id
    def prefetch(self, query):
        return {"results": []}

def register(host):
    host.register_memory_provider(Memory(host.llm))
"#, true);
    for session_id in ["a", "b"] {
        let opened = host.request("hermes/memory.open", json!({"session_id": session_id}));
        assert_eq!(opened["result"]["available"], true, "{opened}");
    }
}

#[test]
fn async_callbacks_share_one_event_loop() {
    let (_dir, mut host) = launch_plugin(r#"
import asyncio
first = None
async def same_loop(args, **_):
    global first
    loop = asyncio.get_running_loop()
    if first is None:
        first = loop
    return {"same": first is loop}
def register(ctx):
    ctx.register_tool("same_loop", "loop", {"name": "same_loop", "description": "", "parameters": {"type": "object"}}, same_loop, is_async=True)
"#, false);
    host.request("initialize", json!({"protocolVersion": "2025-06-18"}));
    let text = |reply: &Value| reply["result"]["content"][0]["text"].as_str().unwrap().to_owned();
    for _ in 0..2 {
        let reply = host.request("tools/call", json!({"name": "same_loop", "arguments": {}}));
        assert_eq!(text(&reply), r#"{"same": true}"#, "{reply}");
    }
}

#[test]
fn a_plugin_can_start_a_spawned_worker_process() {
    let (_dir, mut host) = launch_plugin(r#"
import multiprocessing, sys
def exit_three():
    sys.exit(3)
def start_worker(args, **_):
    # The target is the plugin's own function, which the worker must import.
    worker = multiprocessing.get_context("spawn").Process(target=exit_three)
    worker.start()
    worker.join(5)
    exitcode = worker.exitcode
    if exitcode is None:
        worker.terminate()
        worker.join(2)
    return {"exitcode": exitcode}
def register(ctx):
    ctx.register_tool("start_worker", "workers", {"name": "start_worker", "description": "", "parameters": {"type": "object"}}, start_worker)
"#, false);
    host.request("initialize", json!({"protocolVersion": "2025-06-18"}));
    let reply = host.request("tools/call", json!({"name": "start_worker", "arguments": {}}));
    assert_eq!(reply["result"]["content"][0]["text"].as_str(), Some(r#"{"exitcode": 3}"#), "{reply}");
}
