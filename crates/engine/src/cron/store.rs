use super::*;
use crate::db;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const MAX_JOBS: usize = 1000;
const MAX_RUNS: usize = 5000;
const MAX_BYTES: u64 = 32 * 1024 * 1024;
#[derive(Debug, Clone)]
pub struct Store {
    root: PathBuf,
    profile: PathBuf,
    owner: Arc<Owner>,
    gateway: bool,
}
#[derive(Debug)]
struct Owner {
    id: String,
    lock: Option<File>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(lock) = &self.lock {
            // Release synchronously before another owner starts on this Windows profile.
            let _ = FileExt::unlock(lock);
        }
    }
}
#[derive(Serialize, Deserialize, Default)]
struct Document {
    #[serde(default)]
    jobs: Vec<Job>,
    #[serde(default)]
    runs: Vec<Run>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}
const DOCUMENT: &str = "routines";
/// Where routines and their runs were kept before the profile's database.
const LEGACY: &str = "jobs.json";
impl Document {
    fn from_saved(saved: &db::Document) -> Result<Self> {
        const DAMAGED: &str = "Could not read scheduled jobs; the saved jobs were left untouched";
        fn rows<T: serde::de::DeserializeOwned>(saved: &db::Document, wanted: &str) -> Result<Vec<T>> {
            saved.records.iter().filter(|(kind, ..)| kind == wanted)
                .map(|(.., body)| serde_json::from_str(body)).collect::<serde_json::Result<_>>().context(DAMAGED)
        }
        Ok(Self { jobs: rows(saved, "job")?, runs: rows(saved, "run")?, extra: serde_json::from_str(&saved.body).context(DAMAGED)? })
    }
    fn to_saved(&self) -> Result<db::Document> {
        let mut records = Vec::with_capacity(self.jobs.len() + self.runs.len());
        for job in &self.jobs {
            records.push(("job".to_owned(), job.id.clone(), serde_json::to_string(job)?));
        }
        for run in &self.runs {
            records.push(("run".to_owned(), run.id.clone(), serde_json::to_string(run)?));
        }
        Ok(db::Document { body: serde_json::to_string(&self.extra)?, records })
    }
}
fn read(profile: &Path) -> Result<Option<Document>> {
    let load = || -> Result<Option<Document>> {
        let Some(mut db) = db::open_existing(profile)? else { return Ok(None) };
        db::read_document(&mut db, DOCUMENT)?.as_ref().map(Document::from_saved).transpose()
    };
    if let Some(document) = load()? {
        return Ok(Some(document));
    }
    match read_legacy(&profile.join("cron"))? {
        Some(document) => Ok(Some(document)),
        // It may have moved into the database in between.
        None => load(),
    }
}
fn read_legacy(root: &Path) -> Result<Option<Document>> {
    let path = root.join(LEGACY);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        !fs::symlink_metadata(&path)?.file_type().is_symlink() && file.metadata()?.is_file(),
        "Scheduled jobs file must not be a symbolic link"
    );
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_BYTES,
        "Scheduled jobs file is too large"
    );
    serde_json::from_slice(&bytes)
        .context("Could not read scheduled jobs; the existing file was left untouched")
        .map(Some)
}
/// Routines and their runs as a backup keeps them.
pub fn export(profile: &Path) -> Result<Option<Vec<u8>>> {
    Ok(read(profile)?.map(|document| serde_json::to_vec_pretty(&document)).transpose()?)
}
static NEXT: AtomicU64 = AtomicU64::new(1);
fn id() -> String {
    format!(
        "{:016x}{:08x}{:08x}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64,
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn private_dir(path: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            !metadata.file_type().is_symlink() && metadata.is_dir(),
            "Scheduler directory is not a regular directory"
        );
    }
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
fn lock_file(path: &Path) -> Result<File> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            !metadata.file_type().is_symlink() && metadata.is_file(),
            "Scheduler lock must be a regular file"
        );
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}
impl Store {
    pub fn open(profile_root: &Path) -> Result<Self> {
        Self::open_mode(profile_root, false, false)
    }
    /// Open an editor without acquiring the runner lock or recovering claims.
    pub fn open_view(profile_root: &Path) -> Result<Self> {
        Self::open_mode(profile_root, false, true)
    }
    /// Acquire the runner lock and adopt durable gateway claims for reconciliation.
    pub fn open_gateway(profile_root: &Path) -> Result<Self> {
        let store = Self::open_mode(profile_root, true, false)?;
        ensure!(store.owns_scheduler(), "Another process owns this profile's scheduler");
        Ok(store)
    }
    fn open_mode(profile_root: &Path, gateway: bool, view: bool) -> Result<Self> {
        let profile = profile_root.canonicalize().context("Could not open profile directory")?;
        let root = profile.join("cron");
        private_dir(&root)?;
        private_dir(&root.join("runs"))?;
        let lock = if view { None } else {
            let lock = lock_file(&root.join(".runner.lock"))?;
            match lock.try_lock_exclusive() {
                Ok(()) => Some(lock),
                Err(error) if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() => None,
                Err(error) => return Err(error).context("Could not acquire the scheduler lock"),
            }
        };
        let store = Self { root, profile, owner: Arc::new(Owner { id: id(), lock }), gateway };
        if store.owns_scheduler() {
            store.transaction(|document| {
                recover(document, Utc::now(), gateway);
                if gateway {
                    for run in &mut document.runs {
                        if run.status.live() && run.gateway_job.is_some() { run.owner = store.owner.id.clone(); }
                    }
                }
                Ok(())
            })?;
        }
        Ok(store)
    }
    /// Gateway claims retain their original definition across owner restarts.
    pub fn gateway_claims(&self) -> Result<Vec<Claim>> {
        ensure!(self.gateway && self.owns_scheduler(), "Gateway scheduler does not own this profile");
        Ok(self.read()?.runs.into_iter().filter_map(|run| {
            if !run.status.live() || run.owner != self.owner.id { return None; }
            run.gateway_job.as_ref().map(|job| Claim { job: (**job).clone(), run: run.clone() })
        }).collect())
    }
    pub fn bind_gateway_session(&self, run_id: &str, session_id: &str) -> Result<()> {
        self.bind_gateway_chat(run_id, session_id, |run| &mut run.gateway_session)
    }

    pub fn bind_gateway_routine(&self, run_id: &str, session_id: &str) -> Result<()> {
        self.bind_gateway_chat(run_id, session_id, |run| &mut run.gateway_routine_session)
    }

    fn bind_gateway_chat(
        &self,
        run_id: &str,
        session_id: &str,
        binding: impl FnOnce(&mut Run) -> &mut Option<String>,
    ) -> Result<()> {
        ensure!(self.gateway && self.owns_scheduler(), "Gateway scheduler does not own this profile");
        self.transaction(|document| {
            let run = document.runs.iter_mut().find(|run| run.id == run_id).context("Run no longer exists")?;
            ensure!(run.status.live() && run.owner == self.owner.id && run.gateway_job.is_some(), "Run claim is stale");
            let target = binding(run);
            if let Some(existing) = target {
                ensure!(existing == session_id, "Run is already bound to another session");
            } else {
                *target = Some(session_id.to_owned());
            }
            Ok(())
        })
    }
    pub fn profile_root(&self) -> &Path {
        &self.profile
    }
    pub fn owns_scheduler(&self) -> bool {
        self.owner.lock.is_some()
    }
    fn transaction<T>(&self, operation: impl FnOnce(&mut Document) -> Result<T>) -> Result<T> {
        let lock = lock_file(&self.root.join(".jobs.lock"))?;
        lock.lock_exclusive()?;
        let mut db = db::open(&self.profile)?;
        let saved = db::read_document(&mut db, DOCUMENT)?;
        let mut document = match &saved {
            Some(saved) => Document::from_saved(saved)?,
            None => read_legacy(&self.root)?.unwrap_or_default(),
        };
        let value = operation(&mut document)?;
        ensure!(
            document.jobs.len() <= MAX_JOBS && document.runs.len() <= MAX_RUNS + 32,
            "Scheduler store exceeds its limit"
        );
        let next = document.to_saved()?;
        let bytes = next.body.len() + next.records.iter().map(|(.., body)| body.len()).sum::<usize>();
        ensure!(
            bytes as u64 <= MAX_BYTES,
            "Scheduled jobs store is full; remove older run records first"
        );
        db::write_document(&mut db, DOCUMENT, saved.as_ref(), &next).context("Could not save scheduled jobs")?;
        if saved.is_none() {
            db::retire(&self.root.join(LEGACY));
        }
        Ok(value)
    }
    fn read(&self) -> Result<Document> {
        Ok(read(&self.profile)?.unwrap_or_default())
    }
    pub fn jobs(&self) -> Result<Vec<Job>> {
        let document = self.read()?;
        let mut jobs = document.jobs;
        for job in &mut jobs {
            if document
                .runs
                .iter()
                .any(|run| run.job_id == job.id && run.status.live())
            {
                job.state = "running".into();
            }
        }
        Ok(jobs)
    }
    /// Runs claimed or running now, whoever owns them.
    pub fn live_runs(&self) -> Result<Vec<Run>> {
        Ok(self.read()?.runs.into_iter().filter(|run| run.status.live()).collect())
    }
    pub fn runs(&self, job_id: &str) -> Result<Vec<Run>> {
        let mut runs: Vec<_> = self
            .read()?
            .runs
            .into_iter()
            .filter(|run| run.job_id == job_id)
            .collect();
        runs.reverse();
        Ok(runs)
    }
    pub fn create(&self, draft: Draft, now: DateTime<Utc>) -> Result<Job> {
        let job = build(draft, now, None)?;
        self.transaction(|document| {
            ensure!(document.jobs.len() < MAX_JOBS, "Too many scheduled jobs");
            document.jobs.push(job.clone());
            Ok(job)
        })
    }
    pub fn update(&self, id: &str, draft: Draft, now: DateTime<Utc>) -> Result<Job> {
        self.transaction(|document| {
            let job = document
                .jobs
                .iter_mut()
                .find(|job| job.id == id)
                .context("Scheduled job not found")?;
            *job = build(draft, now, Some(job))?;
            Ok(job.clone())
        })
    }
    pub fn pause(&self, id: &str, now: DateTime<Utc>) -> Result<()> {
        self.transaction(|document| {
            let job = find(document, id)?;
            job.enabled = false;
            job.state = "paused".into();
            job.paused_at = Some(stamp(now));
            job.next_run_at = None;
            Ok(())
        })
    }
    pub fn resume(&self, id: &str, now: DateTime<Utc>) -> Result<()> {
        self.transaction(|document| {
            let job = find(document, id)?;
            ensure!(
                !exhausted(job),
                "Repeat limit reached; use Trigger now for an extra run"
            );
            let next = first(&job.schedule, now, job.timezone())?
                .context("This one-shot has completed; use Trigger now for an extra run")?;
            job.enabled = true;
            job.state = "scheduled".into();
            job.paused_at = None;
            job.next_run_at = Some(stamp(next));
            Ok(())
        })
    }
    pub fn remove(&self, id: &str) -> Result<()> {
        self.transaction(|document| {
            ensure!(
                document.jobs.iter().any(|job| job.id == id),
                "Scheduled job not found"
            );
            document.jobs.retain(|job| job.id != id);
            for run in &mut document.runs {
                if run.job_id == id && run.status.live() {
                    run.cancel_requested = true;
                }
            }
            Ok(())
        })
    }
    pub fn cancel(&self, run_id: &str) -> Result<()> {
        self.transaction(|document| {
            let run = document
                .runs
                .iter_mut()
                .find(|run| run.id == run_id)
                .context("Run not found")?;
            if run.status.live() {
                run.cancel_requested = true;
            }
            Ok(())
        })
    }
    pub fn cancellation_requested(&self, run_id: &str) -> Result<bool> {
        Ok(self
            .read()?
            .runs
            .iter()
            .find(|run| run.id == run_id)
            .is_none_or(|run| run.cancel_requested))
    }
    pub fn claim_due(&self, now: DateTime<Utc>) -> Result<Vec<Claim>> {
        self.claim_due_matching(now, |_| true)
    }
    /// Claim only the due jobs `eligible` accepts: without a usable model, script-only jobs still run.
    pub fn claim_due_matching(&self, now: DateTime<Utc>, eligible: impl Fn(&Job) -> bool) -> Result<Vec<Claim>> {
        if !self.owns_scheduler() {
            return Ok(Vec::new());
        }
        let snapshot = self.read()?;
        if !snapshot.jobs.iter().any(|job| {
            job.enabled
                && job
                    .next_run_at
                    .as_deref()
                    .and_then(|value| instant(value).ok())
                    .is_some_and(|at| at <= now)
        }) {
            return Ok(Vec::new());
        }
        self.transaction(|document| {
            let mut claims = Vec::new();
            let already_running = document.runs.iter().filter(|run| run.status.live()).count();
            // Longest-waiting first: in creation order, two jobs that overrun their intervals would
            // take both slots every time and starve a later job.
            let mut order: Vec<usize> = (0..document.jobs.len()).collect();
            order.sort_by_key(|&index| document.jobs[index].next_run_at.as_deref().and_then(|value| instant(value).ok()));
            for index in order {
                if claims.len() + already_running >= 2 {
                    break;
                }
                let job = &document.jobs[index];
                if !eligible(job) {
                    continue;
                }
                if !job.enabled
                    || job.paused_at.is_some()
                    || exhausted(job)
                    || document
                        .runs
                        .iter()
                        .any(|run| run.job_id == job.id && run.status.live())
                {
                    continue;
                }
                let Some(at) = job
                    .next_run_at
                    .as_deref()
                    .and_then(|value| instant(value).ok())
                else {
                    continue;
                };
                if at > now {
                    continue;
                }
                let lateness = (now - at).num_seconds();
                let grace = if job.grace_minutes == Some(0) {
                    30
                } else {
                    grace_seconds(job, at)?
                };
                if lateness > grace && (job.grace_minutes == Some(0) || job.catch_up != Some(true))
                {
                    let job = &mut document.jobs[index];
                    job.last_status = Some("skipped".into());
                    job.last_error =
                        Some("Missed occurrence was outside the catch-up window".into());
                    job.next_run_at = job.schedule.next_after(now, job.timezone())?.map(stamp);
                    if job.schedule.is_once() {
                        job.enabled = false;
                        job.state = "completed".into();
                    }
                    continue;
                }
                let claim = claim(document, index, false, now, &self.owner.id, &self.root, self.gateway, None)?;
                claims.push(claim);
            }
            prune(document);
            Ok(claims)
        })
    }
    pub fn claim_now(&self, id: &str, now: DateTime<Utc>) -> Result<Claim> {
        ensure!(
            self.owns_scheduler(),
            "Another window owns this profile's scheduler"
        );
        self.transaction(|document| {
            let index = document
                .jobs
                .iter()
                .position(|job| job.id == id)
                .context("Scheduled job not found")?;
            ensure!(
                !document
                    .runs
                    .iter()
                    .any(|run| run.job_id == id && run.status.live()),
                "This job already has a running occurrence"
            );
            let claim = claim(document, index, true, now, &self.owner.id, &self.root, self.gateway, None)?;
            prune(document);
            Ok(claim)
        })
    }
    /// A manual trigger may be retried with the same request identity.
    pub fn claim_gateway_now(&self, job_id: &str, request_id: &str, now: DateTime<Utc>) -> Result<Claim> {
        ensure!(self.gateway && self.owns_scheduler(), "Gateway scheduler does not own this profile");
        ensure!(!request_id.is_empty() && request_id.len() <= 200, "Invalid trigger identity");
        self.transaction(|document| {
            if let Some(run) = document.runs.iter().find(|run| run.gateway_request_id.as_deref() == Some(request_id)) {
                ensure!(run.job_id == job_id, "Trigger identity was used for another job");
                let job = run.gateway_job.as_ref().context("Trigger has no frozen job")?;
                return Ok(Claim { job: (**job).clone(), run: run.clone() });
            }
            let index = document.jobs.iter().position(|job| job.id == job_id).context("Scheduled job not found")?;
            ensure!(!document.runs.iter().any(|run| run.job_id == job_id && run.status.live()), "This job already has a running occurrence");
            let claim = claim(document, index, true, now, &self.owner.id, &self.root, true, Some(request_id))?;
            prune(document);
            Ok(claim)
        })
    }
    pub fn begin(&self, claim: &Claim, now: DateTime<Utc>) -> Result<()> {
        self.transaction(|document| {
            let run = document
                .runs
                .iter_mut()
                .find(|run| run.id == claim.run.id)
                .context("Run no longer exists")?;
            ensure!(
                run.owner == self.owner.id && run.status == RunStatus::Claimed,
                "Run claim is stale"
            );
            ensure!(!run.cancel_requested, "Run was cancelled before starting");
            run.status = RunStatus::Running;
            run.started_at = Some(stamp(now));
            Ok(())
        })
    }
    pub fn finish(&self, claim: &Claim, outcome: Outcome, now: DateTime<Utc>) -> Result<()> {
        self.transaction(|document| {
            let run = document
                .runs
                .iter_mut()
                .find(|run| run.id == claim.run.id)
                .context("Run no longer exists")?;
            ensure!(run.owner == self.owner.id, "Run owner changed");
            if !run.status.live() {
                return Ok(());
            }
            run.finished_at = Some(stamp(now));
            match outcome {
                Outcome::Completed { output } => {
                    run.status = RunStatus::Completed;
                    run.output = bounded(&output, 262144);
                    run.error = None;
                }
                Outcome::Failed { error } => {
                    run.status = RunStatus::Failed;
                    run.error = Some(bounded(&error, 4096));
                }
                Outcome::Cancelled { reason } => {
                    run.status = RunStatus::Cancelled;
                    run.error = Some(bounded(&reason, 4096));
                }
            }
            if let Some(job) = document.jobs.iter_mut().find(|job| job.id == run.job_id) {
                settle(job, run, now);
            }
            Ok(())
        })
    }
}
fn bounded(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}
fn find<'a>(document: &'a mut Document, id: &str) -> Result<&'a mut Job> {
    document
        .jobs
        .iter_mut()
        .find(|job| job.id == id)
        .context("Scheduled job not found")
}
fn exhausted(job: &Job) -> bool {
    job.repeat
        .times
        .is_some_and(|times| times > 0 && job.repeat.completed >= times)
}
fn first(schedule: &Schedule, now: DateTime<Utc>, zone: &str) -> Result<Option<DateTime<Utc>>> {
    if let Schedule::Once { run_at, .. } = schedule {
        let at = instant(run_at)?;
        ensure!(
            (now - at).num_seconds() <= 120,
            "One-shot time is in the past"
        );
        Ok(Some(at))
    } else {
        schedule.next_after(now, zone)
    }
}
fn build(mut draft: Draft, now: DateTime<Utc>, previous: Option<&Job>) -> Result<Job> {
    let (script, monitor) = (draft.script.trim().to_owned(), draft.monitor.trim().to_owned());
    ensure!(
        (draft.no_agent || !draft.prompt.trim().is_empty()) && draft.prompt.len() <= 65536,
        "Prompt is required (maximum 64 KiB)"
    );
    ensure!(!draft.no_agent || !script.is_empty(), "A script-only job needs a script");
    ensure!(script.len() <= 1024 && monitor.len() <= 2048, "The script or monitor is too long");
    let mut extra = previous.map_or_else(BTreeMap::new, |job| job.extra.clone());
    let web = monitor.starts_with("http://") || monitor.starts_with("https://");
    for (name, value) in [
        ("script", script.as_str()),
        ("monitor_script", if web { "" } else { &monitor }),
        ("monitor_url", if web { &monitor } else { "" }),
    ] {
        match value.is_empty() {
            true => extra.remove(name),
            false => extra.insert(name.into(), value.into()),
        };
    }
    match draft.no_agent {
        true => extra.insert("no_agent".into(), true.into()),
        false => extra.remove("no_agent"),
    };
    ensure!(draft.name.len() <= 240, "Name is too long");
    ensure!(
        draft.model.as_ref().is_none_or(|model| model.len() <= 256),
        "Model ID is too long"
    );
    let channel = draft.deliver.strip_prefix("channel:route-")
        .is_some_and(|id| id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    ensure!(draft.deliver == "local" || channel, "Choose local delivery or a gateway channel route");
    ensure!(
        draft.grace_minutes.is_none_or(|minutes| minutes <= 1440),
        "Catch-up grace must be between 0 and 1440 minutes"
    );
    schedule::zone(&draft.timezone)?;
    let schedule = Schedule::parse(&draft.schedule, now, &draft.timezone)?;
    let workdir = draft
        .workdir
        .map(|path| path.canonicalize())
        .transpose()
        .context("Working directory does not exist")?;
    if let Some(path) = &workdir {
        ensure!(path.is_dir(), "Working directory must be a folder");
    }
    let times = draft
        .repeat
        .filter(|times| *times > 0)
        .or_else(|| schedule.is_once().then_some(1));
    // A one-shot names an absolute instant, so its timezone alone does not move it.
    let changed = previous.is_none_or(|job| {
        !same_timing(&job.schedule, &schedule) || (!schedule.is_once() && job.timezone() != draft.timezone)
    });
    // A one-shot given a new time is a new occurrence, so its single run is owed again.
    let completed = previous
        .filter(|job| !(schedule.is_once() && !same_timing(&job.schedule, &schedule)))
        .map_or(0, |job| job.repeat.completed);
    let next = if draft.paused {
        None
    } else if !changed {
        previous.and_then(|job| job.next_run_at.clone())
    } else {
        first(&schedule, now, &draft.timezone)?.map(stamp)
    };
    let name = if draft.name.trim().is_empty() {
        let source = if draft.prompt.trim().is_empty() { &script } else { &draft.prompt };
        source.chars().take(50).collect()
    } else {
        draft.name.trim().into()
    };
    if draft
        .model
        .as_ref()
        .is_some_and(|model| model.trim().is_empty())
    {
        draft.model = None;
    }
    if draft.model.is_none() {
        draft.provider = None;
    }
    let mut job = Job {
        id: previous.map_or_else(id, |job| job.id.clone()),
        name,
        prompt: draft.prompt.trim().into(),
        schedule,
        repeat: Repeat { times, completed },
        enabled: !draft.paused,
        state: if draft.paused { "paused" } else { "scheduled" }.into(),
        timezone: Some(draft.timezone),
        grace_minutes: draft.grace_minutes,
        catch_up: draft.catch_up,
        deliver: draft.deliver,
        workdir,
        model: draft.model,
        provider: draft.provider,
        created_at: previous.map_or_else(|| stamp(now), |job| job.created_at.clone()),
        next_run_at: next,
        last_run_at: previous.and_then(|job| job.last_run_at.clone()),
        last_status: previous.and_then(|job| job.last_status.clone()),
        last_error: previous.and_then(|job| job.last_error.clone()),
        paused_at: draft.paused.then(|| stamp(now)),
        definition_revision: previous.map_or(1, |job| job.definition_revision + 1),
        extra,
    };
    // An enabled job with nowhere to run next has finished: a one-shot skipped outside its
    // catch-up window keeps no count, and renaming it must not leave it "scheduled" forever.
    if exhausted(&job) || (job.enabled && job.next_run_at.is_none()) {
        job.enabled = false;
        job.state = "completed".into();
        job.next_run_at = None;
    }
    Ok(job)
}
fn same_timing(a: &Schedule, b: &Schedule) -> bool {
    match (a, b) {
        (Schedule::Once { run_at: a, .. }, Schedule::Once { run_at: b, .. }) => a == b,
        (Schedule::Interval { minutes: a, .. }, Schedule::Interval { minutes: b, .. }) => a == b,
        (Schedule::Cron { expr: a, .. }, Schedule::Cron { expr: b, .. }) => a == b,
        _ => false,
    }
}
fn grace_seconds(job: &Job, at: DateTime<Utc>) -> Result<i64> {
    if let Some(minutes) = job.grace_minutes {
        return Ok(i64::from(minutes) * 60);
    }
    if job.schedule.is_once() {
        return Ok(120);
    }
    let period = job
        .schedule
        .next_after(at, job.timezone())?
        .map_or(240, |next| (next - at).num_seconds());
    Ok((period / 2).clamp(120, 7200))
}
fn claim(
    document: &mut Document,
    index: usize,
    manual: bool,
    now: DateTime<Utc>,
    owner: &str,
    root: &Path,
    gateway: bool,
    request_id: Option<&str>,
) -> Result<Claim> {
    let job = &mut document.jobs[index];
    let run_id = id();
    let scheduled_instant = if manual {
        None
    } else {
        job.next_run_at.clone()
    };
    if !manual {
        let next = if let (Some(seconds), Some(at)) = (
            job.schedule.interval_seconds(),
            scheduled_instant.as_deref().and_then(|at| instant(at).ok()),
        ) {
            let steps = ((now - at).num_seconds() / seconds).max(0) + 1;
            Some(at + Duration::seconds(steps * seconds))
        } else {
            job.schedule.next_after(now, job.timezone())?
        };
        job.next_run_at = next.map(stamp);
    }
    let run = Run {
        id: run_id.clone(),
        job_id: job.id.clone(),
        status: RunStatus::Claimed,
        claimed_at: stamp(now),
        started_at: None,
        finished_at: None,
        scheduled_instant,
        manual,
        repeat_counted: false,
        owner: owner.into(),
        definition_revision: job.definition_revision,
        session_dir: root.join("runs").join(&run_id),
        output: String::new(),
        error: None,
        cancel_requested: false,
        gateway_job: gateway.then(|| Box::new(job.clone())),
        gateway_session: None,
        gateway_routine_session: None,
        gateway_request_id: request_id.map(str::to_owned),
    };
    let claim = Claim {
        job: job.clone(),
        run: run.clone(),
    };
    document.runs.push(run);
    Ok(claim)
}
fn settle(job: &mut Job, run: &mut Run, now: DateTime<Utc>) {
    job.last_run_at = Some(stamp(now));
    job.last_status = Some(
        match run.status {
            RunStatus::Completed => "ok",
            RunStatus::Cancelled | RunStatus::Interrupted => "cancelled",
            _ => "error",
        }
        .into(),
    );
    job.last_error = run.error.clone();
    // A run of a definition the user has since edited (a one-shot moved to a new time) must not
    // finish the new occurrence or count against it.
    if job.schedule.is_once() && run.definition_revision != job.definition_revision {
        return;
    }
    if !run.manual && !run.repeat_counted {
        job.repeat.completed = job.repeat.completed.saturating_add(1);
        run.repeat_counted = true;
    }
    if !run.manual && (exhausted(job) || job.schedule.is_once()) {
        job.enabled = false;
        job.next_run_at = None;
        job.state = "completed".into();
    } else if !job.enabled {
        // Paused or finished already says why it is off; a manual extra run changes neither.
    } else {
        job.state = if run.status == RunStatus::Failed {
            "error"
        } else {
            "scheduled"
        }
        .into();
    }
}
fn recover(document: &mut Document, now: DateTime<Utc>, preserve_gateway: bool) {
    for run in &mut document.runs {
        if !run.status.live() || (preserve_gateway && run.gateway_job.is_some()) {
            continue;
        }
        let started = run.status == RunStatus::Running;
        run.status = RunStatus::Interrupted;
        run.finished_at = Some(stamp(now));
        run.error = Some(
            if started {
                "Previous scheduler stopped during execution; this occurrence will not be repeated"
            } else {
                "Previous scheduler stopped before dispatch; pending occurrence restored"
            }
            .into(),
        );
        if let Some(job) = document.jobs.iter_mut().find(|job| job.id == run.job_id) {
            if started {
                settle(job, run, now);
            } else if !run.manual
                && job.enabled
                && job.definition_revision == run.definition_revision
            {
                if let Some(slot) = &run.scheduled_instant {
                    if job.next_run_at.as_ref().is_none_or(|next| slot < next) {
                        job.next_run_at = Some(slot.clone());
                    }
                }
            }
        }
    }
}
fn prune(document: &mut Document) {
    if document.runs.len() > MAX_RUNS {
        let mut remove = document.runs.len() - MAX_RUNS;
        document.runs.retain(|run| {
            // Request identities must remain resolvable; refuse new work when the bounded store is full rather than silently duplicating a retry.
            if remove > 0 && !run.status.live() && run.gateway_request_id.is_none() {
                remove -= 1;
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod gateway_recovery_tests {
    use super::*;
    #[test]
    fn routines_move_from_their_file_once_with_their_runs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let job = store.create(Draft { name: "Fixture".into(), prompt: "hello".into(), schedule: "every 1h".into(), ..Default::default() }, Utc::now()).unwrap();
        store.claim_now(&job.id, Utc::now()).unwrap();
        drop(store);
        // As an earlier version left it, with a field this one does not know.
        let mut saved: serde_json::Value = serde_json::from_slice(&export(dir.path()).unwrap().unwrap()).unwrap();
        saved["future"] = serde_json::json!({"kept": true});
        db::open(dir.path()).unwrap().execute("DELETE FROM documents", []).unwrap();
        let file = dir.path().join("cron").join(LEGACY);
        let bytes = serde_json::to_vec_pretty(&saved).unwrap();
        fs::write(&file, &bytes).unwrap();

        // Looking does not move it.
        assert_eq!(Store::open_view(dir.path()).unwrap().jobs().unwrap()[0].id, job.id);
        assert!(file.is_file());
        let store = Store::open(dir.path()).unwrap();
        store.pause(&job.id, Utc::now()).unwrap();
        assert!(!file.exists());
        assert_eq!(fs::read(dir.path().join("cron/jobs.json.before-sqlite")).unwrap(), bytes);
        assert_eq!(store.runs(&job.id).unwrap().len(), 1);
        let now: serde_json::Value = serde_json::from_slice(&export(dir.path()).unwrap().unwrap()).unwrap();
        assert_eq!(now["future"], saved["future"]);
        // A damaged file is reported and kept.
        let other = tempfile::tempdir().unwrap();
        fs::create_dir(other.path().join("cron")).unwrap();
        fs::write(other.path().join("cron").join(LEGACY), b"{").unwrap();
        assert!(Store::open(other.path()).is_err());
        assert_eq!(fs::read(other.path().join("cron").join(LEGACY)).unwrap(), b"{");
    }

    #[test]
    fn gateway_adopts_claim_with_frozen_definition_and_bound_session() {
        let dir = tempfile::tempdir().unwrap();
        let now = Utc::now();
        let store = Store::open_gateway(dir.path()).unwrap();
        let job = store.create(Draft { name: "original".into(), prompt: "first".into(), ..Draft::default() }, now).unwrap();
        let claim = store.claim_now(&job.id, now).unwrap();
        store.bind_gateway_session(&claim.run.id, "chat-1").unwrap();
        store.begin(&claim, now).unwrap();
        store.update(&job.id, Draft { name: "edited".into(), prompt: "second".into(), ..Draft::default() }, now).unwrap();
        let view = Store::open_view(dir.path()).unwrap();
        assert!(!view.owns_scheduler());
        assert_eq!(view.runs(&job.id).unwrap()[0].status, RunStatus::Running);
        drop(view);
        drop(store);
        let restarted = Store::open_gateway(dir.path()).unwrap();
        let claims = restarted.gateway_claims().unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].run.id, claim.run.id);
        assert_eq!(claims[0].run.gateway_session.as_deref(), Some("chat-1"));
        assert_eq!(claims[0].job.prompt, "first");
        assert_eq!(claims[0].run.status, RunStatus::Running);
        assert!(restarted.bind_gateway_session(&claim.run.id, "other").is_err());
        restarted.finish(&claims[0], Outcome::Completed { output: "done".into() }, now).unwrap();
        assert!(restarted.gateway_claims().unwrap().is_empty());
    }

    fn one_shot(store: &Store, at: DateTime<Utc>, now: DateTime<Utc>) -> Job {
        store.create(Draft { prompt: "remind me".into(), schedule: stamp(at), ..Default::default() }, now).unwrap()
    }
    fn job(store: &Store, id: &str) -> Job { store.jobs().unwrap().into_iter().find(|job| job.id == id).unwrap() }

    #[test]
    fn one_shot_edits_keep_pauses_and_new_times() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let now = Utc::now();
        let at = now + Duration::minutes(5);
        let later = at + Duration::days(1);

        // 41: moved to a new time while its old occurrence runs.
        let moved = one_shot(&store, at, now);
        let claim = store.claim_due(at).unwrap().remove(0);
        store.begin(&claim, at).unwrap();
        let mut draft = job(&store, &moved.id).draft();
        draft.schedule = stamp(later);
        store.update(&moved.id, draft, at).unwrap();
        store.finish(&claim, Outcome::Completed { output: "done".into() }, at).unwrap();
        let after = job(&store, &moved.id);
        assert_eq!((after.enabled, after.next_run_at.clone()), (true, Some(stamp(later))), "the new occurrence survives");

        // 43: paused while it ran, then given a new time.
        let paused = one_shot(&store, at, now);
        let claim = store.claim_due(at).unwrap().into_iter().find(|c| c.job.id == paused.id).unwrap();
        store.begin(&claim, at).unwrap();
        store.pause(&paused.id, at).unwrap();
        store.finish(&claim, Outcome::Completed { output: "done".into() }, at).unwrap();
        let mut draft = job(&store, &paused.id).draft();
        draft.schedule = stamp(later);
        let edited = store.update(&paused.id, draft, at).unwrap();
        assert!(!edited.enabled && edited.next_run_at.is_none(), "the pause holds");

        // 44: an extra manual run of a finished one-shot, then a new time.
        let finished = one_shot(&store, at, now);
        let claim = store.claim_due(at).unwrap().into_iter().find(|c| c.job.id == finished.id).unwrap();
        store.begin(&claim, at).unwrap();
        store.finish(&claim, Outcome::Completed { output: "done".into() }, at).unwrap();
        let extra = store.claim_now(&finished.id, at).unwrap();
        store.begin(&extra, at).unwrap();
        store.finish(&extra, Outcome::Completed { output: "again".into() }, at).unwrap();
        assert_eq!(job(&store, &finished.id).state, "completed", "a manual run does not pause it");
        let mut draft = job(&store, &finished.id).draft();
        draft.schedule = stamp(later);
        assert!(store.update(&finished.id, draft, at).unwrap().enabled, "it can be rescheduled");

        // 57: a finished one-shot whose timezone alone changes, well after it ran.
        let done = one_shot(&store, at, now);
        let claim = store.claim_due(at).unwrap().into_iter().find(|c| c.job.id == done.id).unwrap();
        store.begin(&claim, at).unwrap();
        store.finish(&claim, Outcome::Completed { output: "done".into() }, at).unwrap();
        let mut draft = job(&store, &done.id).draft();
        draft.timezone = "America/New_York".into();
        let kept = store.update(&done.id, draft, later).unwrap();
        assert_eq!(kept.state, "completed");
    }

    #[test]
    fn a_skipped_one_shot_stays_finished_when_renamed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let now = Utc::now();
        let at = now + Duration::minutes(5);
        let missed = one_shot(&store, at, now);
        assert!(store.claim_due(at + Duration::hours(3)).unwrap().is_empty(), "outside the catch-up window");
        let mut draft = job(&store, &missed.id).draft();
        draft.name = "Renamed".into();
        let renamed = store.update(&missed.id, draft, at + Duration::hours(3)).unwrap();
        assert_eq!((renamed.enabled, renamed.state.as_str(), renamed.next_run_at), (false, "completed", None));
    }

    #[test]
    fn the_longest_waiting_due_job_is_claimed_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let now = Utc::now();
        let jobs: Vec<_> = (0..3).map(|n| store.create(Draft { prompt: format!("job {n}"), schedule: "every 1m".into(), catch_up: Some(true), ..Default::default() }, now).unwrap()).collect();
        // The third job has waited longest.
        for (job, minutes) in jobs.iter().zip([1, 1, 30]) {
            store.transaction(|document| {
                find(document, &job.id)?.next_run_at = Some(stamp(now - Duration::minutes(minutes)));
                Ok(())
            }).unwrap();
        }
        let claimed: Vec<_> = store.claim_due(now).unwrap().into_iter().map(|claim| claim.job.id).collect();
        assert!(claimed.contains(&jobs[2].id), "{claimed:?}");
    }

    #[test]
    fn a_finished_one_shot_given_a_new_time_runs_again() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let now = Utc::now();
        let at = now + Duration::minutes(5);
        let job = store.create(Draft { prompt: "remind me".into(), schedule: stamp(at), ..Default::default() }, now).unwrap();
        let claims = store.claim_due(at).unwrap();
        store.begin(&claims[0], at).unwrap();
        store.finish(&claims[0], Outcome::Completed { output: "done".into() }, at).unwrap();
        let done = store.jobs().unwrap().remove(0);
        assert_eq!((done.enabled, done.state.as_str()), (false, "completed"));

        // Renaming it leaves it finished.
        let mut renamed = done.draft();
        renamed.name = "Reminder".into();
        let kept = store.update(&job.id, renamed, at).unwrap();
        assert_eq!((kept.enabled, kept.next_run_at.as_deref()), (false, None));

        let later = at + Duration::days(1);
        let mut moved = kept.draft();
        moved.schedule = stamp(later);
        let rescheduled = store.update(&job.id, moved, at).unwrap();
        assert_eq!((rescheduled.enabled, rescheduled.state.as_str()), (true, "scheduled"));
        assert_eq!(rescheduled.next_run_at, Some(stamp(later)));
        assert_eq!(store.claim_due(later).unwrap().len(), 1);

        // A paused job stays paused through an edit.
        let paused = store.create(Draft { prompt: "later".into(), schedule: "every 1h".into(), ..Default::default() }, now).unwrap();
        store.pause(&paused.id, now).unwrap();
        let paused = store.jobs().unwrap().into_iter().find(|j| j.id == paused.id).unwrap();
        let mut edited = paused.draft();
        edited.schedule = "every 2h".into();
        let edited = store.update(&paused.id, edited, now).unwrap();
        assert_eq!((edited.enabled, edited.next_run_at.as_deref()), (false, None));
    }
}
