//! Incremental synchronisation of the WebDAV tree into SQLite.
//!
//! Flow: crawl remote -> diff against the index -> classify new / changed / vanished files ->
//! match vanished against new files to detect moves & renames -> apply in one transaction ->
//! invalidate thumbnails -> purge old tombstones -> remove orphan thumbnails.

use crate::app::Shared;
use crate::db::now;
use crate::media;
use crate::webdav::RemoteFile;
use anyhow::{bail, Result};
use rusqlite::params;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncResult {
    pub new: usize,
    pub changed: usize,
    pub removed: usize,
    pub moved: usize,
    pub revived: usize,
    pub unchanged: usize,
    pub purged: usize,
    pub hashed: usize,
    pub orphan_thumbnails_removed: usize,
    pub remote_total: usize,
    pub duration_ms: u64,
    pub started_at: i64,
    pub finished_at: i64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub running: bool,
    pub started_at: Option<i64>,
    pub last: Option<SyncResult>,
}

#[derive(Default)]
pub struct SyncCtl {
    running: AtomicBool,
    started_at: Mutex<Option<i64>>,
    last: Mutex<Option<SyncResult>>,
}

impl SyncCtl {
    pub fn status(&self) -> SyncStatus {
        SyncStatus {
            running: self.running.load(Ordering::SeqCst),
            started_at: *self.started_at.lock().unwrap_or_else(|e| e.into_inner()),
            last: self.last.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        }
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Run one sync unless one is already running. Returns `None` if skipped.
pub async fn run_guarded(app: &Shared) -> Option<SyncResult> {
    if app.sync.running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return None;
    }
    let started = now();
    *app.sync.started_at.lock().unwrap_or_else(|e| e.into_inner()) = Some(started);
    let t = Instant::now();
    let mut res = match do_sync(app).await {
        Ok(r) => r,
        Err(e) => {
            error!("sync failed: {e:#}");
            SyncResult { error: Some(format!("{e:#}")), ..Default::default() }
        }
    };
    res.started_at = started;
    res.finished_at = now();
    res.duration_ms = t.elapsed().as_millis() as u64;
    if res.error.is_none() {
        info!("sync completed in {:.1}s", res.duration_ms as f64 / 1000.0);
    }
    *app.sync.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(res.clone());
    *app.sync.started_at.lock().unwrap_or_else(|e| e.into_inner()) = None;
    app.sync.running.store(false, Ordering::SeqCst);
    Some(res)
}

/// Spawn a sync in the background; false if one is already running.
pub fn trigger(app: &Shared) -> bool {
    if app.sync.is_running() {
        return false;
    }
    let app = app.clone();
    tokio::spawn(async move {
        run_guarded(&app).await;
    });
    true
}

/// Periodic sync loop (first run at startup if configured).
pub async fn scheduler(app: Shared) {
    let interval = app.cfg.sync_interval();
    if app.cfg.sync.on_startup {
        run_guarded(&app).await;
    }
    let Some(period) = interval else { return };
    loop {
        tokio::time::sleep(period).await;
        if app.is_shutdown() {
            return;
        }
        run_guarded(&app).await;
    }
}

#[derive(Clone)]
struct DbRow {
    id: i64,
    path: String,
    name: String,
    kind: String,
    size: i64,
    mtime: i64,
    status: String,
    hash: Option<String>,
}

type Key = (i64, i64);

async fn do_sync(app: &Shared) -> Result<SyncResult> {
    info!("sync started");
    let remote = match app.dav.crawl().await {
        Ok(r) => r,
        Err(e) => {
            error!("WebDAV connection/scan failed: {e:#}");
            return Err(e);
        }
    };
    let mut res = SyncResult { remote_total: remote.len(), ..Default::default() };

    let rows: Vec<DbRow> = app
        .db
        .call(|c| {
            let mut st = c.prepare("SELECT id,path,name,kind,size,mtime,status,hash FROM files")?;
            let it = st.query_map([], |r| {
                Ok(DbRow {
                    id: r.get(0)?,
                    path: r.get(1)?,
                    name: r.get(2)?,
                    kind: r.get(3)?,
                    size: r.get(4)?,
                    mtime: r.get(5)?,
                    status: r.get(6)?,
                    hash: r.get(7)?,
                })
            })?;
            Ok(it.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await?;

    let live_in_db = rows.iter().filter(|r| r.status != "missing").count();
    if remote.is_empty() && live_in_db > 0 && !app.cfg.sync.allow_empty_remote {
        bail!(
            "WebDAV scan found no media files but the index holds {live_in_db}; refusing to mark them all missing \
             (set sync.allow_empty_remote = true if this is intended)"
        );
    }

    let by_path: HashMap<&str, &DbRow> = rows.iter().map(|r| (r.path.as_str(), r)).collect();
    let remote_paths: HashSet<&str> = remote.iter().map(|f| f.path.as_str()).collect();

    let mut new_files: Vec<&RemoteFile> = Vec::new();
    let mut changed: Vec<(i64, &RemoteFile)> = Vec::new();
    let mut revived: Vec<(i64, &RemoteFile, bool)> = Vec::new();
    for f in &remote {
        match by_path.get(f.path.as_str()) {
            Some(r) => {
                let diff = r.size != f.size || r.mtime != f.mtime;
                if r.status == "missing" {
                    revived.push((r.id, f, diff));
                } else if diff {
                    changed.push((r.id, f));
                } else {
                    res.unchanged += 1;
                }
            }
            None => new_files.push(f),
        }
    }
    let gone: Vec<&DbRow> = rows.iter().filter(|r| r.status != "missing" && !remote_paths.contains(r.path.as_str())).collect();
    let tombstones: Vec<&DbRow> = rows.iter().filter(|r| r.status == "missing" && !remote_paths.contains(r.path.as_str())).collect();

    // ---- move / rename detection -------------------------------------------------------
    let mut cand_by_key: HashMap<Key, Vec<&DbRow>> = HashMap::new();
    for r in gone.iter().chain(tombstones.iter()) {
        cand_by_key.entry((r.size, r.mtime)).or_default().push(r);
    }
    let mut new_by_key: HashMap<Key, Vec<&RemoteFile>> = HashMap::new();
    for f in &new_files {
        new_by_key.entry((f.size, f.mtime)).or_default().push(f);
    }

    // On-demand hashing: (a) new files in ambiguous groups whose candidates carry a hash,
    // (b) known files that share (size, mtime) with another remote file and have no hash yet.
    let mut to_hash: Vec<&RemoteFile> = Vec::new();
    for (key, news) in &new_by_key {
        if let Some(cands) = cand_by_key.get(key) {
            let ambiguous = !(news.len() == 1 && cands.len() == 1);
            if ambiguous && cands.iter().any(|c| c.hash.is_some()) {
                to_hash.extend(news.iter().copied());
            }
        }
    }
    let mut remote_key_count: HashMap<Key, usize> = HashMap::new();
    for f in &remote {
        *remote_key_count.entry((f.size, f.mtime)).or_default() += 1;
    }
    let changed_paths: HashSet<&str> = changed.iter().map(|(_, f)| f.path.as_str()).collect();
    let mut queued: HashSet<&str> = to_hash.iter().map(|f| f.path.as_str()).collect();
    for f in &remote {
        if f.size > 0 && remote_key_count.get(&(f.size, f.mtime)).copied().unwrap_or(0) > 1 {
            let need = match by_path.get(f.path.as_str()) {
                Some(r) => r.status != "missing" && (r.hash.is_none() || changed_paths.contains(f.path.as_str())),
                None => true,
            };
            if need && queued.insert(f.path.as_str()) {
                to_hash.push(f);
            }
        }
    }
    to_hash.truncate(app.cfg.sync.max_hash_per_run);
    let mut hashes: HashMap<String, String> = HashMap::new();
    for f in to_hash {
        match quick_hash(app, &f.path, f.size).await {
            Ok(h) => {
                hashes.insert(f.path.clone(), h);
            }
            Err(e) => warn!("hash failed for {}: {e:#}", f.path),
        }
    }
    res.hashed = hashes.len();

    let mut moves: Vec<(&DbRow, &RemoteFile)> = Vec::new();
    let mut inserts: Vec<&RemoteFile> = Vec::new();
    for (key, news) in &new_by_key {
        let cands: Vec<&DbRow> = cand_by_key.get(key).map(|v| v.clone()).unwrap_or_default();
        if cands.is_empty() {
            inserts.extend(news.iter().copied());
            continue;
        }
        if news.len() == 1 && cands.len() == 1 && cands[0].kind == news[0].kind.as_str() {
            moves.push((cands[0], news[0]));
            continue;
        }
        let mut used: HashSet<i64> = HashSet::new();
        for n in news {
            let free: Vec<&&DbRow> = cands.iter().filter(|c| !used.contains(&c.id) && c.kind == n.kind.as_str()).collect();
            let mut pick: Option<&DbRow> = None;
            if let Some(h) = hashes.get(&n.path) {
                let m: Vec<&&&DbRow> = free.iter().filter(|c| c.hash.as_deref() == Some(h.as_str())).collect();
                if m.len() == 1 {
                    pick = Some(**m[0]);
                }
            }
            if pick.is_none() {
                let same_name: Vec<&&&DbRow> = free.iter().filter(|c| c.name == n.name).collect();
                let rivals = news.iter().filter(|o| o.name == n.name).count();
                if same_name.len() == 1 && rivals == 1 {
                    pick = Some(**same_name[0]);
                }
            }
            match pick {
                Some(c) => {
                    used.insert(c.id);
                    moves.push((c, n));
                }
                None => inserts.push(n),
            }
        }
    }
    res.new = inserts.len();
    res.moved = moves.len();
    res.changed = changed.len();
    let moved_ids: HashSet<i64> = moves.iter().map(|(r, _)| r.id).collect();
    let removed: Vec<&DbRow> = gone.iter().filter(|r| !moved_ids.contains(&r.id)).copied().collect();
    res.removed = removed.len();
    res.revived = revived.len();

    // ---- apply ------------------------------------------------------------------------
    let ts = now();
    let retention = app.cfg.sync.retention_days.max(0) * 86400;
    let ins_v: Vec<(RemoteFile, Option<String>)> =
        inserts.iter().map(|f| ((*f).clone(), hashes.get(&f.path).cloned())).collect();
    let chg_v: Vec<(i64, RemoteFile)> = changed.iter().map(|(id, f)| (*id, (*f).clone())).collect();
    let rev_v: Vec<(i64, RemoteFile, bool)> = revived.iter().map(|(id, f, d)| (*id, (*f).clone(), *d)).collect();
    let mov_v: Vec<(i64, String, RemoteFile, Option<String>)> =
        moves.iter().map(|(r, f)| (r.id, r.path.clone(), (*f).clone(), hashes.get(&f.path).cloned())).collect();
    let rem_ids: Vec<i64> = removed.iter().map(|r| r.id).collect();
    let known_hashes: Vec<(String, String)> =
        hashes.iter().filter(|(p, _)| by_path.contains_key(p.as_str())).map(|(p, h)| (p.clone(), h.clone())).collect();
    let invalidate: Vec<i64> = chg_v.iter().map(|(id, _)| *id).chain(rem_ids.iter().copied()).collect();

    let purged = app
        .db
        .call(move |c| {
            let tx = c.transaction()?;
            tx.execute("UPDATE files SET status='active' WHERE status='changed'", [])?;
            {
                let mut ins = tx.prepare(
                    "INSERT INTO files (path,dir,name,ext,kind,size,mtime,mime,fingerprint,hash,status,thumbnail_status,first_seen_at,last_seen_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'active','pending',?11,?11)",
                )?;
                for (f, h) in &ins_v {
                    ins.execute(params![
                        f.path, f.dir, f.name, f.ext, f.kind.as_str(), f.size, f.mtime, f.mime,
                        media::fingerprint(&f.path, f.size, f.mtime), h, ts
                    ])?;
                }
                let mut upd = tx.prepare(
                    "UPDATE files SET size=?2, mtime=?3, mime=?4, fingerprint=?5, hash=NULL, status='changed',
                        thumbnail_status='pending', thumb_error=NULL, thumb_retry=0, thumb_last_attempt=NULL,
                        deleted_at=NULL, last_seen_at=?6 WHERE id=?1",
                )?;
                for (id, f) in &chg_v {
                    upd.execute(params![id, f.size, f.mtime, f.mime, media::fingerprint(&f.path, f.size, f.mtime), ts])?;
                }
                let mut rev = tx.prepare(
                    "UPDATE files SET size=?2, mtime=?3, mime=?4, fingerprint=?5, status=?6, deleted_at=NULL,
                        thumbnail_status='pending', thumb_error=NULL, thumb_retry=0, thumb_last_attempt=NULL,
                        hash=CASE WHEN ?7 THEN NULL ELSE hash END, last_seen_at=?8 WHERE id=?1",
                )?;
                for (id, f, diff) in &rev_v {
                    let st = if *diff { "changed" } else { "active" };
                    rev.execute(params![id, f.size, f.mtime, f.mime, media::fingerprint(&f.path, f.size, f.mtime), st, diff, ts])?;
                }
                let mut mv = tx.prepare(
                    "UPDATE files SET path=?2, dir=?3, name=?4, ext=?5, old_path=?6, size=?7, mtime=?8, mime=?9, fingerprint=?10,
                        hash=COALESCE(?11, hash), status='active', deleted_at=NULL, last_seen_at=?12,
                        thumbnail_status=CASE WHEN thumbnail_status IN ('none') THEN 'pending' ELSE thumbnail_status END
                     WHERE id=?1",
                )?;
                for (id, old, f, h) in &mov_v {
                    mv.execute(params![
                        id, f.path, f.dir, f.name, f.ext, old, f.size, f.mtime, f.mime,
                        media::fingerprint(&f.path, f.size, f.mtime), h, ts
                    ])?;
                }
                let mut gone_st = tx.prepare(
                    "UPDATE files SET status='missing', deleted_at=?2, thumbnail_status='none', thumb_error=NULL WHERE id=?1",
                )?;
                for id in &rem_ids {
                    gone_st.execute(params![id, ts])?;
                }
                let mut hs = tx.prepare("UPDATE files SET hash=?2 WHERE path=?1 AND hash IS NULL")?;
                for (p, h) in &known_hashes {
                    hs.execute(params![p, h])?;
                }
            }
            tx.execute(
                "UPDATE files SET last_seen_at=?1 WHERE status!='missing' AND last_seen_at < ?1 - 3600",
                params![ts],
            )?;
            let purged = tx.execute("DELETE FROM files WHERE status='missing' AND deleted_at IS NOT NULL AND deleted_at < ?1", params![ts - retention])?;
            tx.commit()?;
            Ok(purged)
        })
        .await?;
    res.purged = purged;

    // Thumbnails of changed / vanished files are stale: delete them right away.
    for id in invalidate {
        remove_thumb_dir(app, id).await;
    }
    res.orphan_thumbnails_removed = cleanup_orphan_thumbnails(app).await.unwrap_or_else(|e| {
        warn!("orphan thumbnail scan failed: {e:#}");
        0
    });

    let pending: i64 = app.db.call(|c| Ok(c.query_row("SELECT COUNT(*) FROM files WHERE thumbnail_status='pending'", [], |r| r.get(0))?)).await?;
    info!("discovered {} new files", res.new);
    info!("detected {} changed files", res.changed);
    info!("detected {} removed files", res.removed);
    info!("detected {} moved files", res.moved);
    if res.revived > 0 {
        info!("revived {} previously missing files", res.revived);
    }
    info!("thumbnail queue: {pending} pending");
    Ok(res)
}

pub fn thumb_dir(app: &Shared, id: i64) -> PathBuf {
    app.cfg.thumbnail.directory.join(id.to_string())
}

pub async fn remove_thumb_dir(app: &Shared, id: i64) {
    let dir = thumb_dir(app, id);
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("removing {}: {e}", dir.display()),
    }
}

/// Delete thumbnail directories whose file no longer exists (or is missing).
pub async fn cleanup_orphan_thumbnails(app: &Shared) -> Result<usize> {
    let root = app.cfg.thumbnail.directory.clone();
    let mut rd = match tokio::fs::read_dir(&root).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    // List directories first, query ids afterwards: any listed dir then belongs to a row that
    // already existed, so a thumbnail created concurrently can never look orphaned.
    let mut dirs: Vec<(PathBuf, Option<i64>)> = Vec::new();
    while let Some(ent) = rd.next_entry().await? {
        if ent.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
            let id = ent.file_name().to_str().and_then(|s| s.parse::<i64>().ok());
            dirs.push((ent.path(), id));
        }
    }
    if dirs.is_empty() {
        return Ok(0);
    }
    let live: HashSet<i64> = app
        .db
        .call(|c| {
            let mut st = c.prepare("SELECT id FROM files WHERE status!='missing'")?;
            let ids = st.query_map([], |r| r.get::<_, i64>(0))?.collect::<rusqlite::Result<HashSet<_>>>()?;
            Ok(ids)
        })
        .await?;
    let mut removed = 0;
    for (path, id) in dirs {
        let orphan = match id {
            Some(id) => !live.contains(&id),
            None => false, // never touch directories we did not create
        };
        if orphan {
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => removed += 1,
                Err(e) => warn!("removing orphan {}: {e}", path.display()),
            }
        }
    }
    if removed > 0 {
        info!("removed {removed} orphan thumbnail directories");
    }
    Ok(removed)
}

/// Partial content hash: size + first 256 KiB + last 256 KiB (the whole file if small).
/// Deliberately cheap: it only disambiguates files that already agree on size and mtime.
pub async fn quick_hash(app: &Shared, rel: &str, size: i64) -> Result<String> {
    const CHUNK: i64 = 256 * 1024;
    let mut h = Sha256::new();
    h.update(size.to_le_bytes());
    if size <= 2 * CHUNK {
        h.update(app.dav.get_range_bytes(rel, 0, size.max(1)).await?);
    } else {
        h.update(app.dav.get_range_bytes(rel, 0, CHUNK).await?);
        h.update(app.dav.get_range_bytes(rel, size - CHUNK, CHUNK).await?);
    }
    Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}
