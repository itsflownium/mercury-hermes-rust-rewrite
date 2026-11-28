//! A terminal client for the same turn engine used by the native window.
use anyhow::{Result, ensure};
use hermes_engine::{CancellationToken, Config, Entry, Event, Session, Status};
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<()> {
    let prompt = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    ensure!(!prompt.is_empty(), "Pass a prompt as the argument");
    let mut session = Session::open(&Session::default_dir()?)?;
    let (events, inbox) = async_channel::unbounded();
    let task = tokio::spawn(async move {
        session.turn(Config::from_env(), prompt, events, CancellationToken::new()).await?;
        anyhow::Ok(session)
    });
    while let Ok(event) = inbox.recv().await {
        match event {
            Event::Text(text) => { print!("{text}"); io::stdout().flush()?; }
            Event::Warning(message) => eprintln!("{message}"),
            Event::Approval { call, answer } => {
                eprintln!("\nAllow {} {}? [y/N]", call.name, call.arguments);
                let mut line = String::new();
                io::stdin().read_line(&mut line)?;
                answer.send(line.trim().eq_ignore_ascii_case("y")).await?;
            }
            Event::Subagent(p) => eprintln!("\n[subagent {}/{}] {}: {}", p.task_index + 1, p.task_count, p.status, p.goal),
            Event::Snapshot(_) | Event::MemoryRecall(_) | Event::Clarification { .. } | Event::Correction { .. } => {}
        }
    }
    let session = task.await??;
    let last = session.history().turns.last().expect("a completed invocation has a turn");
    for entry in &last.entries {
        if let Entry::Tool { name, approved, result, .. } = entry {
            eprintln!("\n{name} (approved={approved}) → {result}");
        }
    }
    ensure!(last.status == Status::Complete, "Turn ended with {:?}", last.status);
    println!();
    Ok(())
}
