//! SQLite access: one connection behind a mutex, used from `spawn_blocking`.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

const SCHEMA_V1: &str = r#"
CREATE TABLE files (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    path               TEXT    NOT NULL UNIQUE,
    dir                TEXT    NOT NULL,
    name               TEXT    NOT NULL,
    ext                TEXT    NOT NULL,
    kind               TEXT    NOT NULL,                 -- 'image' | 'video'
    size               INTEGER NOT NULL,
    mtime              INTEGER NOT NULL,
    mime               TEXT    NOT NULL,
    width              INTEGER,
    height             INTEGER,
    duration           REAL,
    fingerprint        TEXT    NOT NULL,                 -- path|size|mtime
    hash               TEXT,                             -- partial content hash, on demand only
    status             TEXT    NOT NULL DEFAULT 'active',-- active | changed | missing
    old_path           TEXT,
    thumbnail_status   TEXT    NOT NULL DEFAULT 'pending', -- none | pending | processing | ready | failed
    thumb_error        TEXT,
    thumb_retry        INTEGER NOT NULL DEFAULT 0,
    thumb_last_attempt INTEGER,
    first_seen_at      INTEGER NOT NULL,
    last_seen_at       INTEGER NOT NULL,
    deleted_at         INTEGER
);
CREATE INDEX idx_files_dir         ON files(dir);
CREATE INDEX idx_files_fingerprint ON files(fingerprint);
CREATE INDEX idx_files_mtime       ON files(mtime);
CREATE INDEX idx_files_mime        ON files(mime);
CREATE INDEX idx_files_kind        ON files(kind);
CREATE INDEX idx_files_size_mtime  ON files(size, mtime);
CREATE INDEX idx_files_status      ON files(status);
CREATE INDEX idx_files_thumb       ON files(thumbnail_status);
CREATE INDEX idx_files_first_seen  ON files(first_seen_at);

CREATE TABLE tags (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_tags_name ON tags(name);

CREATE TABLE file_tags (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    tag_id  INTEGER NOT NULL REFERENCES tags(id)  ON DELETE CASCADE,
    PRIMARY KEY (file_id, tag_id)
);
CREATE INDEX idx_file_tags_file ON file_tags(file_id);
CREATE INDEX idx_file_tags_tag  ON file_tags(tag_id);

CREATE TABLE albums (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    description   TEXT    NOT NULL DEFAULT '',
    cover_file_id INTEGER REFERENCES files(id) ON DELETE SET NULL,
    created_at    INTEGER NOT NULL
);

CREATE TABLE album_files (
    album_id INTEGER NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    file_id  INTEGER NOT NULL REFERENCES files(id)  ON DELETE CASCADE,
    added_at INTEGER NOT NULL,
    PRIMARY KEY (album_id, file_id)
);
CREATE INDEX idx_album_files_album ON album_files(album_id);
CREATE INDEX idx_album_files_file  ON album_files(file_id);
"#;

impl Db {
    pub fn open(path: &Path) -> Result<Db> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
            }
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Db> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Db> {
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL; PRAGMA temp_store = MEMORY;",
        )?;
        let ver: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if ver < 1 {
            conn.execute_batch(&format!("BEGIN; {SCHEMA_V1} PRAGMA user_version = 1; COMMIT;"))
                .context("creating schema")?;
        }
        Ok(Db { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Run a closure with the connection on a blocking thread.
    pub async fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            f(&mut guard)
        })
        .await
        .context("database task panicked")?
    }
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
