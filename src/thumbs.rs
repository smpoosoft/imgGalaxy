//! Thumbnail queue: incremental, asynchronous, interruptible and restartable.
//!
//! The gallery never decodes media itself; it schedules work and shells out to FFmpeg.

use crate::app::Shared;
use crate::db::now;
use anyhow::{anyhow, bail, Result};
use rusqlite::{params_from_iter, types::Value};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::Semaphore;
use tracing::{info, warn};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Scope {
    pub file_ids: Vec<i64>,
    pub album_id: Option<i64>,
    pub dir: Option<String>,
    /// Also retry files in `failed` state (ignoring retry limits).
    pub retry_failed: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RunInfo {
    pub trigger: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
}

#[derive(Default)]
pub struct ThumbCtl {
    running: AtomicBool,
    cancel: AtomicBool,
    done_in_run: AtomicUsize,
    run: Mutex<Option<RunInfo>>,
}

impl ThumbCtl {
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    pub fn current_run(&self) -> Option<RunInfo> {
        self.run.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Job {
    id: i64,
    path: String,
    kind: String,
    fingerprint: String,
}

/// Start a manual run in the background (all pending files in scope, until the queue is empty).
/// Returns false if a run is already in progress.
pub fn trigger(app: &Shared, scope: Scope) -> bool {
    if app.thumbs.running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return false;
    }
    let app = app.clone();
    tokio::spawn(async move {
        run(&app, scope, true, "manual").await;
    });
    true
}

/// Periodic job: process one batch of pending (and retry-eligible failed) files.
pub async fn scheduler(app: Shared) {
    let Some(period) = app.cfg.thumb_interval() else { return };
    loop {
        tokio::time::sleep(period).await;
        if app.is_shutdown() {
            return;
        }
        if app.thumbs.running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            run(&app, Scope::default(), false, "scheduled").await;
        }
    }
}

/// Reset rows left in `processing` by an interrupted run.
pub async fn recover(app: &Shared) -> Result<()> {
    let n = app
        .db
        .call(|c| Ok(c.execute("UPDATE files SET thumbnail_status='pending' WHERE thumbnail_status='processing'", [])?))
        .await?;
    if n > 0 {
        info!("recovered {n} interrupted thumbnail jobs");
    }
    Ok(())
}

/// Caller must have set `running`. Clears it when done.
async fn run(app: &Shared, scope: Scope, until_empty: bool, trigger: &str) {
    app.thumbs.cancel.store(false, Ordering::SeqCst);
    app.thumbs.done_in_run.store(0, Ordering::SeqCst);
    let mut info = RunInfo { trigger: trigger.to_string(), started_at: now(), ..Default::default() };
    *app.thumbs.run.lock().unwrap_or_else(|e| e.into_inner()) = Some(info.clone());
    let sem = Arc::new(Semaphore::new(app.cfg.thumbnail.workers.max(1)));
    let batch_size = app.cfg.thumbnail.batch_size.max(1);
    let mut first = true;
    loop {
        if app.is_shutdown() || app.thumbs.cancel.load(Ordering::SeqCst) {
            break;
        }
        let jobs = match claim_batch(app, &scope, batch_size, until_empty).await {
            Ok(j) => j,
            Err(e) => {
                warn!("thumbnail queue error: {e:#}");
                break;
            }
        };
        if jobs.is_empty() {
            break;
        }
        if first {
            info!("thumbnail run started ({trigger}): {} in first batch", jobs.len());
            first = false;
        }
        let mut handles = Vec::new();
        for job in jobs {
            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };
            if app.is_shutdown() || app.thumbs.cancel.load(Ordering::SeqCst) {
                // Hand the job back to the queue.
                release(app, job.id).await;
                drop(permit);
                continue;
            }
            let app2 = app.clone();
            handles.push(tokio::spawn(async move {
                let ok = process(&app2, &job).await;
                app2.thumbs.done_in_run.fetch_add(1, Ordering::SeqCst);
                drop(permit);
                ok
            }));
        }
        for h in handles {
            match h.await {
                Ok(true) => {
                    info.succeeded += 1;
                    info.processed += 1;
                }
                Ok(false) => {
                    info.failed += 1;
                    info.processed += 1;
                }
                Err(e) => warn!("thumbnail task panicked: {e}"),
            }
        }
        *app.thumbs.run.lock().unwrap_or_else(|e| e.into_inner()) = Some(info.clone());
        if !until_empty {
            break;
        }
    }
    info.finished_at = Some(now());
    if info.processed > 0 {
        info!("thumbnail run finished: {} ok, {} failed", info.succeeded, info.failed);
    }
    *app.thumbs.run.lock().unwrap_or_else(|e| e.into_inner()) = Some(info);
    app.thumbs.running.store(false, Ordering::SeqCst);
}

async fn release(app: &Shared, id: i64) {
    let _ = app.db.call(move |c| Ok(c.execute("UPDATE files SET thumbnail_status='pending' WHERE id=?1 AND thumbnail_status='processing'", [id])?)).await;
}

/// Select the next batch and mark it `processing` atomically.
async fn claim_batch(app: &Shared, scope: &Scope, limit: usize, manual: bool) -> Result<Vec<Job>> {
    let scope = scope.clone();
    let max_retries = app.cfg.thumbnail.max_retries;
    let backoff = app.cfg.thumbnail.retry_backoff_secs;
    app.db
        .call(move |c| {
            let mut sql = String::from("SELECT id,path,kind,fingerprint FROM files WHERE status!='missing' AND ");
            let mut args: Vec<Value> = Vec::new();
            if scope.retry_failed {
                sql.push_str("thumbnail_status IN ('pending','failed')");
            } else if manual {
                sql.push_str("thumbnail_status='pending'");
            } else {
                sql.push_str("(thumbnail_status='pending' OR (thumbnail_status='failed' AND thumb_retry<? AND COALESCE(thumb_last_attempt,0)<?))");
                args.push(Value::Integer(max_retries));
                args.push(Value::Integer(now() - backoff));
            }
            if !scope.file_ids.is_empty() {
                let ph = vec!["?"; scope.file_ids.len()].join(",");
                sql.push_str(&format!(" AND id IN ({ph})"));
                args.extend(scope.file_ids.iter().map(|i| Value::Integer(*i)));
            }
            if let Some(a) = scope.album_id {
                sql.push_str(" AND id IN (SELECT file_id FROM album_files WHERE album_id=?)");
                args.push(Value::Integer(a));
            }
            if let Some(d) = &scope.dir {
                let d = d.trim_end_matches('/');
                let esc = d.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
                sql.push_str(" AND (dir=? OR dir LIKE ? ESCAPE '\\')");
                args.push(Value::Text(if d.is_empty() { "/".into() } else { d.to_string() }));
                args.push(Value::Text(format!("{esc}/%")));
            }
            sql.push_str(" ORDER BY first_seen_at DESC, id DESC LIMIT ?");
            args.push(Value::Integer(limit as i64));
            let tx = c.transaction()?;
            let jobs: Vec<Job> = {
                let mut st = tx.prepare(&sql)?;
                let rows = st.query_map(params_from_iter(args.iter()), |r| {
                    Ok(Job { id: r.get(0)?, path: r.get(1)?, kind: r.get(2)?, fingerprint: r.get(3)? })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            {
                let mut up = tx.prepare("UPDATE files SET thumbnail_status='processing', thumb_last_attempt=?2 WHERE id=?1")?;
                let t = now();
                for j in &jobs {
                    up.execute(rusqlite::params![j.id, t])?;
                }
            }
            tx.commit()?;
            Ok(jobs)
        })
        .await
}

pub fn thumb_path(app: &Shared, id: i64) -> PathBuf {
    app.cfg.thumbnail.directory.join(id.to_string()).join(format!("{}.jpg", app.cfg.thumbnail.size))
}

/// Generate one thumbnail and record the outcome. Returns true on success.
async fn process(app: &Shared, job: &Job) -> bool {
    let started = Instant::now();
    let result = generate(app, job).await;
    let ok = result.is_ok();
    match &result {
        Ok(_) => info!("thumbnail ok: {} ({} ms)", job.path, started.elapsed().as_millis()),
        Err(e) => warn!("thumbnail failure: {}: {e:#}", job.path),
    }
    let (w, h, d) = result.as_ref().ok().cloned().unwrap_or((None, None, None));
    let err = result.err().map(|e| format!("{e:#}").chars().take(500).collect::<String>());
    let id = job.id;
    let fp = job.fingerprint.clone();
    let upd = app
        .db
        .call(move |c| {
            // Only commit the outcome if the file did not change / vanish while we were working.
            let n = if err.is_none() {
                c.execute(
                    "UPDATE files SET thumbnail_status='ready', thumb_error=NULL, thumb_retry=0,
                        width=COALESCE(?3,width), height=COALESCE(?4,height), duration=COALESCE(?5,duration)
                     WHERE id=?1 AND fingerprint=?2 AND status!='missing' AND thumbnail_status='processing'",
                    rusqlite::params![id, fp, w, h, d],
                )?
            } else {
                c.execute(
                    "UPDATE files SET thumbnail_status='failed', thumb_error=?3, thumb_retry=thumb_retry+1
                     WHERE id=?1 AND fingerprint=?2 AND status!='missing' AND thumbnail_status='processing'",
                    rusqlite::params![id, fp, err],
                )?
            };
            Ok(n)
        })
        .await;
    match upd {
        Ok(0) => {
            // Superseded (changed or removed mid-flight): drop what we wrote; the new state decides.
            let still_live: bool = app
                .db
                .call(move |c| Ok(c.query_row("SELECT status!='missing' FROM files WHERE id=?1", [id], |r| r.get(0)).unwrap_or(false)))
                .await
                .unwrap_or(false);
            if !still_live {
                crate::sync::remove_thumb_dir(app, id).await;
            } else if ok {
                let _ = tokio::fs::remove_file(thumb_path(app, id)).await;
            }
        }
        Ok(_) => {}
        Err(e) => warn!("recording thumbnail result failed: {e:#}"),
    }
    ok
}

type Dims = (Option<i64>, Option<i64>, Option<f64>);

async fn generate(app: &Shared, job: &Job) -> Result<Dims> {
    let cfg = &app.cfg.thumbnail;
    let out = thumb_path(app, job.id);
    let dir = out.parent().ok_or_else(|| anyhow!("bad thumbnail path"))?.to_path_buf();
    tokio::fs::create_dir_all(&dir).await?;
    let tmp = dir.join(format!("{}.tmp.jpg", cfg.size));
    let _ = tokio::fs::remove_file(&tmp).await;

    let url = app.dav.url_for(&job.path);
    let auth = app.dav.auth_header();
    let timeout = Duration::from_secs(cfg.timeout_secs.max(10));
    let is_video = job.kind == "video";

    let attempts: &[Option<&str>] = if is_video { &[Some("1"), None] } else { &[None] };
    let mut last_err = String::new();
    let mut produced = false;
    for ss in attempts {
        let _ = tokio::fs::remove_file(&tmp).await;
        match run_ffmpeg(&cfg.ffmpeg, &url, auth.as_deref(), *ss, cfg.size, &tmp, timeout).await {
            Ok(()) => {
                if tokio::fs::metadata(&tmp).await.map(|m| m.len() > 0).unwrap_or(false) {
                    produced = true;
                    break;
                }
                last_err = "ffmpeg produced no output".into();
            }
            Err(e) => last_err = format!("{e:#}"),
        }
    }
    if !produced {
        let _ = tokio::fs::remove_file(&tmp).await;
        bail!("{last_err}");
    }
    tokio::fs::rename(&tmp, &out).await?;
    Ok(probe(app, &url, auth.as_deref(), timeout).await.unwrap_or((None, None, None)))
}

async fn run_ffmpeg(
    ffmpeg: &str,
    url: &str,
    auth: Option<&str>,
    seek: Option<&str>,
    size: u32,
    out: &std::path::Path,
    timeout: Duration,
) -> Result<()> {
    let mut cmd = Command::new(ffmpeg);
    cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-protocol_whitelist", "http,https,tcp,tls,crypto"]);
    if let Some(a) = auth {
        cmd.arg("-headers").arg(format!("Authorization: {a}\r\n"));
    }
    if let Some(ss) = seek {
        cmd.args(["-ss", ss]);
    }
    let vf = format!("scale=w='min({size},iw)':h='min({size},ih)':force_original_aspect_ratio=decrease,format=yuvj420p");
    cmd.arg("-i").arg(url).args(["-frames:v", "1", "-an", "-sn", "-vf", &vf, "-q:v", "3", "-f", "image2"]).arg(out);
    cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped()).kill_on_drop(true);
    let child = cmd.spawn().map_err(|e| anyhow!("cannot start '{ffmpeg}': {e}"))?;
    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(r) => r?,
        Err(_) => bail!("ffmpeg timed out after {}s", timeout.as_secs()),
    };
    if !output.status.success() {
        let msg = String::from_utf8_lossy(&output.stderr);
        let msg = msg.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("unknown error");
        // Never leak credentials (they are only in headers, but be safe with URLs).
        bail!("ffmpeg exited with {}: {}", output.status, msg.trim());
    }
    Ok(())
}

/// Best-effort width/height/duration via ffprobe.
async fn probe(app: &Shared, url: &str, auth: Option<&str>, timeout: Duration) -> Result<Dims> {
    let mut cmd = Command::new(app.cfg.ffprobe_path());
    cmd.args(["-v", "error", "-protocol_whitelist", "http,https,tcp,tls,crypto"]);
    if let Some(a) = auth {
        cmd.arg("-headers").arg(format!("Authorization: {a}\r\n"));
    }
    cmd.args(["-select_streams", "v:0", "-show_entries", "stream=width,height:format=duration", "-of", "json"]).arg(url);
    cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true);
    let child = cmd.spawn()?;
    let output = tokio::time::timeout(timeout, child.wait_with_output()).await??;
    if !output.status.success() {
        bail!("ffprobe failed");
    }
    let v: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let w = v["streams"][0]["width"].as_i64();
    let h = v["streams"][0]["height"].as_i64();
    let d = v["format"]["duration"].as_str().and_then(|s| s.parse::<f64>().ok());
    Ok((w, h, d))
}
