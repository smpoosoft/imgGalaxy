//! HTTP API, media proxy and embedded web UI.

use crate::app::Shared;
use crate::db::now;
use crate::{sync, thumbs};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, OptionalExtension};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(RustEmbed)]
#[folder = "frontend/dist/"]
#[allow_missing = true]
struct Assets;

pub struct ApiError(StatusCode, String);

impl ApiError {
    fn bad(msg: impl Into<String>) -> Self {
        ApiError(StatusCode::BAD_REQUEST, msg.into())
    }
    fn not_found(what: &str) -> Self {
        ApiError(StatusCode::NOT_FOUND, format!("{what} not found"))
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("api error: {e:#}");
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}
type ApiResult<T> = Result<Json<T>, ApiError>;

pub fn router(app: Shared) -> Router {
    Router::new()
        .route("/api/files", get(list_files))
        .route("/api/files/bulk-tags", post(bulk_tags))
        .route("/api/files/:id", get(get_file))
        .route("/api/files/:id/tags", post(add_file_tag))
        .route("/api/files/:id/tags/:tag_id", delete(remove_file_tag))
        .route("/api/dirs", get(list_dirs))
        .route("/api/albums", get(list_albums).post(create_album))
        .route("/api/albums/:id", put(update_album).delete(delete_album))
        .route("/api/albums/:id/files", post(album_add).delete(album_remove))
        .route("/api/tags", get(list_tags).post(create_tag))
        .route("/api/tags/:id", delete(delete_tag))
        .route("/api/sync", post(post_sync))
        .route("/api/sync/status", get(sync_status))
        .route("/api/thumbnails/generate", post(thumb_generate))
        .route("/api/thumbnails/status", get(thumb_status))
        .route("/api/webdav/status", get(webdav_status))
        .route("/media/:id", get(media))
        .route("/thumbnail/:id", get(thumbnail))
        .fallback(static_handler)
        .with_state(app)
}

// ---------------------------------------------------------------------------------------
// files
// ---------------------------------------------------------------------------------------

#[derive(Serialize)]
struct FileItem {
    id: i64,
    path: String,
    dir: String,
    name: String,
    ext: String,
    kind: String,
    size: i64,
    mtime: i64,
    mime: String,
    width: Option<i64>,
    height: Option<i64>,
    duration: Option<f64>,
    thumbnail_status: String,
    status: String,
    first_seen_at: i64,
}

const FILE_COLS: &str =
    "f.id,f.path,f.dir,f.name,f.ext,f.kind,f.size,f.mtime,f.mime,f.width,f.height,f.duration,f.thumbnail_status,f.status,f.first_seen_at";

fn map_file(r: &rusqlite::Row) -> rusqlite::Result<FileItem> {
    Ok(FileItem {
        id: r.get(0)?,
        path: r.get(1)?,
        dir: r.get(2)?,
        name: r.get(3)?,
        ext: r.get(4)?,
        kind: r.get(5)?,
        size: r.get(6)?,
        mtime: r.get(7)?,
        mime: r.get(8)?,
        width: r.get(9)?,
        height: r.get(10)?,
        duration: r.get(11)?,
        thumbnail_status: r.get(12)?,
        status: r.get(13)?,
        first_seen_at: r.get(14)?,
    })
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ListParams {
    q: Option<String>,
    /// Comma separated tag names; all must match.
    tag: Option<String>,
    album: Option<i64>,
    dir: Option<String>,
    kind: Option<String>,
    sort: Option<String>,
    order: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

fn like_pattern(s: &str) -> String {
    format!("%{}%", s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))
}

fn build_filter(p: &ListParams) -> (String, Vec<Value>) {
    let mut sql = String::from(" WHERE f.status != 'missing'");
    let mut args: Vec<Value> = Vec::new();
    if let Some(q) = p.q.as_deref() {
        for w in q.split_whitespace() {
            sql.push_str(
                " AND (f.path LIKE ? ESCAPE '\\' OR EXISTS (SELECT 1 FROM file_tags ft JOIN tags t ON t.id=ft.tag_id \
                 WHERE ft.file_id=f.id AND t.name LIKE ? ESCAPE '\\'))",
            );
            args.push(Value::Text(like_pattern(w)));
            args.push(Value::Text(like_pattern(w)));
        }
    }
    if let Some(t) = p.tag.as_deref() {
        for name in t.split(',').map(|s| s.trim().trim_start_matches('#')).filter(|s| !s.is_empty()) {
            sql.push_str(
                " AND EXISTS (SELECT 1 FROM file_tags ft JOIN tags t ON t.id=ft.tag_id WHERE ft.file_id=f.id AND t.name = ? COLLATE NOCASE)",
            );
            args.push(Value::Text(name.to_string()));
        }
    }
    if let Some(a) = p.album {
        sql.push_str(" AND EXISTS (SELECT 1 FROM album_files af WHERE af.album_id=? AND af.file_id=f.id)");
        args.push(Value::Integer(a));
    }
    if let Some(d) = p.dir.as_deref().filter(|d| !d.is_empty()) {
        let d = d.trim_end_matches('/');
        let d = if d.is_empty() { "/" } else { d };
        if d == "/" {
            // whole library: no restriction
        } else {
            sql.push_str(" AND (f.dir = ? OR f.dir LIKE ? ESCAPE '\\')");
            args.push(Value::Text(d.to_string()));
            args.push(Value::Text(format!("{}/%", d.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))));
        }
    }
    if let Some(k) = p.kind.as_deref() {
        if k == "image" || k == "video" {
            sql.push_str(" AND f.kind = ?");
            args.push(Value::Text(k.to_string()));
        }
    }
    (sql, args)
}

#[derive(Serialize)]
struct FileList {
    items: Vec<FileItem>,
    total: i64,
    offset: i64,
    limit: i64,
}

async fn list_files(State(app): State<Shared>, Query(p): Query<ListParams>) -> ApiResult<FileList> {
    let limit = p.limit.unwrap_or(100).clamp(1, 500);
    let offset = p.offset.unwrap_or(0).max(0);
    let col = match p.sort.as_deref() {
        Some("name") => "f.name COLLATE NOCASE",
        Some("size") => "f.size",
        Some("added") => "f.first_seen_at",
        Some("path") => "f.path",
        _ => "f.mtime",
    };
    let dir = match p.order.as_deref() {
        Some("asc") => "ASC",
        Some("desc") => "DESC",
        // sensible defaults: names ascending, everything else newest first
        _ if matches!(p.sort.as_deref(), Some("name") | Some("path")) => "ASC",
        _ => "DESC",
    };
    let (where_sql, args) = build_filter(&p);
    let out = app
        .db
        .call(move |c| {
            let total: i64 = c.query_row(
                &format!("SELECT COUNT(*) FROM files f{where_sql}"),
                params_from_iter(args.iter()),
                |r| r.get(0),
            )?;
            let mut a2 = args.clone();
            a2.push(Value::Integer(limit));
            a2.push(Value::Integer(offset));
            let sql = format!("SELECT {FILE_COLS} FROM files f{where_sql} ORDER BY {col} {dir}, f.id {dir} LIMIT ? OFFSET ?");
            let mut st = c.prepare(&sql)?;
            let items = st.query_map(params_from_iter(a2.iter()), map_file)?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(FileList { items, total, offset, limit })
        })
        .await?;
    Ok(Json(out))
}

#[derive(Serialize)]
struct TagLite {
    id: i64,
    name: String,
}
#[derive(Serialize)]
struct AlbumLite {
    id: i64,
    name: String,
}
#[derive(Serialize)]
struct FileDetail {
    #[serde(flatten)]
    file: FileItem,
    old_path: Option<String>,
    thumb_error: Option<String>,
    tags: Vec<TagLite>,
    albums: Vec<AlbumLite>,
}

async fn get_file(State(app): State<Shared>, Path(id): Path<i64>) -> ApiResult<FileDetail> {
    let d = app
        .db
        .call(move |c| {
            let f = c
                .query_row(&format!("SELECT {FILE_COLS},f.old_path,f.thumb_error FROM files f WHERE f.id=?1 AND f.status!='missing'"), [id], |r| {
                    Ok((map_file(r)?, r.get::<_, Option<String>>(15)?, r.get::<_, Option<String>>(16)?))
                })
                .optional()?;
            let Some((file, old_path, thumb_error)) = f else { return Ok(None) };
            let mut st = c.prepare("SELECT t.id,t.name FROM file_tags ft JOIN tags t ON t.id=ft.tag_id WHERE ft.file_id=?1 ORDER BY t.name")?;
            let tags = st.query_map([id], |r| Ok(TagLite { id: r.get(0)?, name: r.get(1)? }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut st = c.prepare("SELECT a.id,a.name FROM album_files af JOIN albums a ON a.id=af.album_id WHERE af.file_id=?1 ORDER BY a.name")?;
            let albums = st.query_map([id], |r| Ok(AlbumLite { id: r.get(0)?, name: r.get(1)? }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Some(FileDetail { file, old_path, thumb_error, tags, albums }))
        })
        .await?;
    d.map(Json).ok_or_else(|| ApiError::not_found("file"))
}

#[derive(Serialize)]
struct DirItem {
    dir: String,
    count: i64,
}

async fn list_dirs(State(app): State<Shared>) -> ApiResult<Vec<DirItem>> {
    let v = app
        .db
        .call(|c| {
            let mut st = c.prepare("SELECT dir, COUNT(*) FROM files WHERE status!='missing' GROUP BY dir ORDER BY dir")?;
            let v = st.query_map([], |r| Ok(DirItem { dir: r.get(0)?, count: r.get(1)? }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(v)
        })
        .await?;
    Ok(Json(v))
}

// ---------------------------------------------------------------------------------------
// tags
// ---------------------------------------------------------------------------------------

fn clean_tag(name: &str) -> Result<String, ApiError> {
    let n = name.trim().trim_start_matches('#').trim().to_string();
    if n.is_empty() {
        return Err(ApiError::bad("tag name must not be empty"));
    }
    if n.chars().count() > 64 {
        return Err(ApiError::bad("tag name too long (max 64 characters)"));
    }
    if n.contains(',') {
        return Err(ApiError::bad("tag name must not contain commas"));
    }
    Ok(n)
}

fn ensure_tag(c: &rusqlite::Connection, name: &str) -> rusqlite::Result<i64> {
    c.execute("INSERT OR IGNORE INTO tags (name, created_at) VALUES (?1, ?2)", params![name, now()])?;
    c.query_row("SELECT id FROM tags WHERE name = ?1 COLLATE NOCASE", [name], |r| r.get(0))
}

#[derive(Serialize)]
struct TagItem {
    id: i64,
    name: String,
    count: i64,
    created_at: i64,
}

async fn list_tags(State(app): State<Shared>) -> ApiResult<Vec<TagItem>> {
    let v = app
        .db
        .call(|c| {
            let mut st = c.prepare(
                "SELECT t.id, t.name, t.created_at,
                        (SELECT COUNT(*) FROM file_tags ft JOIN files f ON f.id=ft.file_id WHERE ft.tag_id=t.id AND f.status!='missing')
                 FROM tags t ORDER BY t.name COLLATE NOCASE",
            )?;
            let v = st
                .query_map([], |r| Ok(TagItem { id: r.get(0)?, name: r.get(1)?, created_at: r.get(2)?, count: r.get(3)? }))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(v)
        })
        .await?;
    Ok(Json(v))
}

#[derive(Deserialize)]
struct NameBody {
    name: String,
}

async fn create_tag(State(app): State<Shared>, Json(b): Json<NameBody>) -> ApiResult<TagLite> {
    let name = clean_tag(&b.name)?;
    let t = app
        .db
        .call(move |c| {
            let id = ensure_tag(c, &name)?;
            let name: String = c.query_row("SELECT name FROM tags WHERE id=?1", [id], |r| r.get(0))?;
            Ok(TagLite { id, name })
        })
        .await?;
    Ok(Json(t))
}

async fn delete_tag(State(app): State<Shared>, Path(id): Path<i64>) -> ApiResult<serde_json::Value> {
    let n = app.db.call(move |c| Ok(c.execute("DELETE FROM tags WHERE id=?1", [id])?)).await?;
    if n == 0 {
        return Err(ApiError::not_found("tag"));
    }
    Ok(Json(json!({ "deleted": true })))
}

async fn add_file_tag(State(app): State<Shared>, Path(id): Path<i64>, Json(b): Json<NameBody>) -> ApiResult<TagLite> {
    let name = clean_tag(&b.name)?;
    let t = app
        .db
        .call(move |c| {
            let exists: bool = c.query_row("SELECT 1 FROM files WHERE id=?1 AND status!='missing'", [id], |_| Ok(true)).optional()?.unwrap_or(false);
            if !exists {
                return Ok(None);
            }
            let tid = ensure_tag(c, &name)?;
            c.execute("INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?1, ?2)", params![id, tid])?;
            let name: String = c.query_row("SELECT name FROM tags WHERE id=?1", [tid], |r| r.get(0))?;
            Ok(Some(TagLite { id: tid, name }))
        })
        .await?;
    t.map(Json).ok_or_else(|| ApiError::not_found("file"))
}

async fn remove_file_tag(State(app): State<Shared>, Path((id, tag_id)): Path<(i64, i64)>) -> ApiResult<serde_json::Value> {
    let n = app.db.call(move |c| Ok(c.execute("DELETE FROM file_tags WHERE file_id=?1 AND tag_id=?2", params![id, tag_id])?)).await?;
    Ok(Json(json!({ "removed": n })))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct BulkTags {
    file_ids: Vec<i64>,
    add: Vec<String>,
    remove: Vec<String>,
}

async fn bulk_tags(State(app): State<Shared>, Json(b): Json<BulkTags>) -> ApiResult<serde_json::Value> {
    let add: Vec<String> = b.add.iter().map(|n| clean_tag(n)).collect::<Result<_, _>>()?;
    let remove: Vec<String> = b.remove.iter().map(|n| clean_tag(n)).collect::<Result<_, _>>()?;
    let ids = b.file_ids;
    let n = app
        .db
        .call(move |c| {
            let tx = c.transaction()?;
            let mut touched = 0usize;
            for name in &add {
                let tid = ensure_tag(&tx, name)?;
                for fid in &ids {
                    touched += tx.execute(
                        "INSERT OR IGNORE INTO file_tags (file_id, tag_id) SELECT id, ?2 FROM files WHERE id=?1 AND status!='missing'",
                        params![fid, tid],
                    )?;
                }
            }
            for name in &remove {
                for fid in &ids {
                    touched += tx.execute(
                        "DELETE FROM file_tags WHERE file_id=?1 AND tag_id=(SELECT id FROM tags WHERE name=?2 COLLATE NOCASE)",
                        params![fid, name],
                    )?;
                }
            }
            tx.commit()?;
            Ok(touched)
        })
        .await?;
    Ok(Json(json!({ "changed": n })))
}

// ---------------------------------------------------------------------------------------
// albums
// ---------------------------------------------------------------------------------------

#[derive(Serialize)]
struct AlbumItem {
    id: i64,
    name: String,
    description: String,
    created_at: i64,
    count: i64,
    cover_file_id: Option<i64>,
}

const ALBUM_SQL: &str = "SELECT a.id, a.name, a.description, a.created_at,
    (SELECT COUNT(*) FROM album_files af JOIN files f ON f.id=af.file_id WHERE af.album_id=a.id AND f.status!='missing'),
    COALESCE(
        (SELECT f.id FROM files f WHERE f.id=a.cover_file_id AND f.status!='missing'),
        (SELECT f.id FROM album_files af JOIN files f ON f.id=af.file_id WHERE af.album_id=a.id AND f.status!='missing'
            ORDER BY af.added_at DESC, f.id DESC LIMIT 1))
    FROM albums a";

fn map_album(r: &rusqlite::Row) -> rusqlite::Result<AlbumItem> {
    Ok(AlbumItem { id: r.get(0)?, name: r.get(1)?, description: r.get(2)?, created_at: r.get(3)?, count: r.get(4)?, cover_file_id: r.get(5)? })
}

async fn list_albums(State(app): State<Shared>) -> ApiResult<Vec<AlbumItem>> {
    let v = app
        .db
        .call(|c| {
            let mut st = c.prepare(&format!("{ALBUM_SQL} ORDER BY a.name COLLATE NOCASE"))?;
            let v = st.query_map([], map_album)?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(v)
        })
        .await?;
    Ok(Json(v))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct AlbumBody {
    name: Option<String>,
    description: Option<String>,
    cover_file_id: Option<i64>,
}

fn clean_album_name(n: &str) -> Result<String, ApiError> {
    let n = n.trim().to_string();
    if n.is_empty() {
        return Err(ApiError::bad("album name must not be empty"));
    }
    if n.chars().count() > 100 {
        return Err(ApiError::bad("album name too long (max 100 characters)"));
    }
    Ok(n)
}

async fn create_album(State(app): State<Shared>, Json(b): Json<AlbumBody>) -> ApiResult<AlbumItem> {
    let name = clean_album_name(b.name.as_deref().unwrap_or(""))?;
    let desc = b.description.unwrap_or_default();
    let a = app
        .db
        .call(move |c| {
            c.execute("INSERT INTO albums (name, description, created_at) VALUES (?1, ?2, ?3)", params![name, desc, now()])?;
            let id = c.last_insert_rowid();
            Ok(c.query_row(&format!("{ALBUM_SQL} WHERE a.id=?1"), [id], map_album)?)
        })
        .await?;
    Ok(Json(a))
}

async fn update_album(State(app): State<Shared>, Path(id): Path<i64>, Json(b): Json<AlbumBody>) -> ApiResult<AlbumItem> {
    let name = match b.name.as_deref() {
        Some(n) => Some(clean_album_name(n)?),
        None => None,
    };
    let a = app
        .db
        .call(move |c| {
            let tx = c.transaction()?;
            let exists: bool = tx.query_row("SELECT 1 FROM albums WHERE id=?1", [id], |_| Ok(true)).optional()?.unwrap_or(false);
            if !exists {
                return Ok(None);
            }
            if let Some(n) = &name {
                tx.execute("UPDATE albums SET name=?2 WHERE id=?1", params![id, n])?;
            }
            if let Some(d) = &b.description {
                tx.execute("UPDATE albums SET description=?2 WHERE id=?1", params![id, d])?;
            }
            if let Some(cf) = b.cover_file_id {
                tx.execute(
                    "UPDATE albums SET cover_file_id=?2 WHERE id=?1 AND EXISTS (SELECT 1 FROM album_files WHERE album_id=?1 AND file_id=?2)",
                    params![id, cf],
                )?;
            }
            let a = tx.query_row(&format!("{ALBUM_SQL} WHERE a.id=?1"), [id], map_album)?;
            tx.commit()?;
            Ok(Some(a))
        })
        .await?;
    a.map(Json).ok_or_else(|| ApiError::not_found("album"))
}

async fn delete_album(State(app): State<Shared>, Path(id): Path<i64>) -> ApiResult<serde_json::Value> {
    let n = app.db.call(move |c| Ok(c.execute("DELETE FROM albums WHERE id=?1", [id])?)).await?;
    if n == 0 {
        return Err(ApiError::not_found("album"));
    }
    Ok(Json(json!({ "deleted": true })))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct FileIds {
    file_ids: Vec<i64>,
}

async fn album_add(State(app): State<Shared>, Path(id): Path<i64>, Json(b): Json<FileIds>) -> ApiResult<serde_json::Value> {
    let n = app
        .db
        .call(move |c| {
            let tx = c.transaction()?;
            let exists: bool = tx.query_row("SELECT 1 FROM albums WHERE id=?1", [id], |_| Ok(true)).optional()?.unwrap_or(false);
            if !exists {
                return Ok(None);
            }
            let mut added = 0;
            let t = now();
            for fid in &b.file_ids {
                added += tx.execute(
                    "INSERT OR IGNORE INTO album_files (album_id, file_id, added_at) SELECT ?1, id, ?3 FROM files WHERE id=?2 AND status!='missing'",
                    params![id, fid, t],
                )?;
            }
            tx.commit()?;
            Ok(Some(added))
        })
        .await?;
    n.map(|n| Json(json!({ "added": n }))).ok_or_else(|| ApiError::not_found("album"))
}

async fn album_remove(State(app): State<Shared>, Path(id): Path<i64>, Json(b): Json<FileIds>) -> ApiResult<serde_json::Value> {
    let n = app
        .db
        .call(move |c| {
            let tx = c.transaction()?;
            let mut removed = 0;
            for fid in &b.file_ids {
                removed += tx.execute("DELETE FROM album_files WHERE album_id=?1 AND file_id=?2", params![id, fid])?;
            }
            tx.execute("UPDATE albums SET cover_file_id=NULL WHERE id=?1 AND cover_file_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM album_files WHERE album_id=?1 AND file_id=cover_file_id)", [id])?;
            tx.commit()?;
            Ok(removed)
        })
        .await?;
    Ok(Json(json!({ "removed": n })))
}

// ---------------------------------------------------------------------------------------
// sync / thumbnails / status
// ---------------------------------------------------------------------------------------

async fn post_sync(State(app): State<Shared>) -> Response {
    if sync::trigger(&app) {
        (StatusCode::ACCEPTED, Json(json!({ "started": true }))).into_response()
    } else {
        (StatusCode::CONFLICT, Json(json!({ "error": "a sync is already running" }))).into_response()
    }
}

async fn sync_status(State(app): State<Shared>) -> ApiResult<serde_json::Value> {
    let st = app.sync.status();
    let counts = app
        .db
        .call(|c| {
            Ok(c.query_row(
                "SELECT COUNT(*), COALESCE(SUM(status!='missing'),0), COALESCE(SUM(status='missing'),0),
                        COALESCE(SUM(kind='image' AND status!='missing'),0), COALESCE(SUM(kind='video' AND status!='missing'),0)
                 FROM files",
                [],
                |r| Ok(json!({ "indexed": r.get::<_, i64>(0)?, "active": r.get::<_, i64>(1)?, "missing": r.get::<_, i64>(2)?, "images": r.get::<_, i64>(3)?, "videos": r.get::<_, i64>(4)? })),
            )?)
        })
        .await?;
    let mut v = serde_json::to_value(st).map_err(|e| anyhow::anyhow!(e))?;
    v["counts"] = counts;
    v["interval_secs"] = json!(app.cfg.sync_interval().map(|d| d.as_secs()));
    Ok(Json(v))
}

async fn thumb_generate(State(app): State<Shared>, body: axum::body::Bytes) -> Response {
    let scope: thumbs::Scope = if body.iter().all(|b| b.is_ascii_whitespace()) {
        thumbs::Scope::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(s) => s,
            Err(e) => return ApiError::bad(format!("invalid body: {e}")).into_response(),
        }
    };
    if thumbs::trigger(&app, scope) {
        (StatusCode::ACCEPTED, Json(json!({ "started": true }))).into_response()
    } else {
        (StatusCode::CONFLICT, Json(json!({ "error": "thumbnail generation is already running" }))).into_response()
    }
}

async fn thumb_status(State(app): State<Shared>) -> ApiResult<serde_json::Value> {
    let (counts, failures) = app
        .db
        .call(|c| {
            let mut counts = serde_json::Map::new();
            for s in ["pending", "processing", "ready", "failed", "none"] {
                counts.insert(s.into(), json!(0));
            }
            let mut st = c.prepare("SELECT thumbnail_status, COUNT(*) FROM files WHERE status!='missing' GROUP BY thumbnail_status")?;
            for row in st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
                let (k, v) = row?;
                counts.insert(k, json!(v));
            }
            let mut st = c.prepare(
                "SELECT id, path, thumb_error, thumb_retry, thumb_last_attempt FROM files
                 WHERE thumbnail_status='failed' AND status!='missing' ORDER BY thumb_last_attempt DESC LIMIT 20",
            )?;
            let failures = st
                .query_map([], |r| {
                    Ok(json!({ "id": r.get::<_, i64>(0)?, "path": r.get::<_, String>(1)?, "error": r.get::<_, Option<String>>(2)?,
                               "retry_count": r.get::<_, i64>(3)?, "last_attempt_at": r.get::<_, Option<i64>>(4)? }))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((serde_json::Value::Object(counts), failures))
        })
        .await?;
    Ok(Json(json!({
        "running": app.thumbs.is_running(),
        "run": app.thumbs.current_run(),
        "counts": counts,
        "recent_failures": failures,
        "workers": app.cfg.thumbnail.workers,
        "interval_secs": app.cfg.thumb_interval().map(|d| d.as_secs()),
    })))
}

async fn webdav_status(State(app): State<Shared>) -> Json<crate::webdav::DavStatus> {
    Json(app.dav.check().await)
}

// ---------------------------------------------------------------------------------------
// media + thumbnails
// ---------------------------------------------------------------------------------------

async fn media(State(app): State<Shared>, Path(id): Path<i64>, headers: HeaderMap) -> Result<Response, ApiError> {
    let row: Option<(String, String)> = app
        .db
        .call(move |c| Ok(c.query_row("SELECT path, mime FROM files WHERE id=?1 AND status!='missing'", [id], |r| Ok((r.get(0)?, r.get(1)?))).optional()?))
        .await?;
    let (path, mime) = row.ok_or_else(|| ApiError::not_found("file"))?;
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let resp = app
        .dav
        .get(&path, range)
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, format!("source unavailable: {e}")))?;
    let status = resp.status();
    if status == StatusCode::NOT_FOUND {
        return Err(ApiError::not_found("source file (will be reconciled on next sync)"));
    }
    if !(status.is_success() || status == StatusCode::RANGE_NOT_SATISFIABLE) {
        return Err(ApiError(StatusCode::BAD_GATEWAY, format!("source returned HTTP {status}")));
    }
    let mut out = Response::builder().status(status);
    let h = resp.headers();
    let ctype = h.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).filter(|v| !v.starts_with("application/octet")).map(|s| s.to_string()).unwrap_or(mime);
    out = out.header(header::CONTENT_TYPE, ctype).header(header::ACCEPT_RANGES, "bytes").header(header::CACHE_CONTROL, "private, max-age=3600");
    for name in [header::CONTENT_LENGTH, header::CONTENT_RANGE, header::LAST_MODIFIED, header::ETAG] {
        if let Some(v) = h.get(&name) {
            out = out.header(name, v.clone());
        }
    }
    out.body(Body::from_stream(resp.bytes_stream())).map_err(|e| ApiError::from(anyhow::anyhow!(e)))
}

async fn thumbnail(State(app): State<Shared>, Path(id): Path<i64>, headers: HeaderMap) -> Result<Response, ApiError> {
    let path = thumbs::thumb_path(&app, id);
    let meta = tokio::fs::metadata(&path).await.map_err(|_| ApiError::not_found("thumbnail"))?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let etag = format!("\"{:x}-{:x}\"", mtime, meta.len());
    if headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) == Some(etag.as_str()) {
        return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)]).into_response());
    }
    let bytes = tokio::fs::read(&path).await.map_err(|_| ApiError::not_found("thumbnail"))?;
    Ok((
        [
            (header::CONTENT_TYPE, "image/jpeg".to_string()),
            (header::CACHE_CONTROL, "public, max-age=86400".to_string()),
            (header::ETAG, etag),
        ],
        bytes,
    )
        .into_response())
}

// ---------------------------------------------------------------------------------------
// embedded UI
// ---------------------------------------------------------------------------------------

async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.starts_with("api/") || path.starts_with("media/") || path.starts_with("thumbnail/") {
        return ApiError::not_found("route").into_response();
    }
    let (file, name) = match Assets::get(if path.is_empty() { "index.html" } else { path }) {
        Some(f) => (f, if path.is_empty() { "index.html" } else { path }),
        None => match Assets::get("index.html") {
            Some(f) if !path.contains('.') => (f, "index.html"),
            _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
        },
    };
    let mime = mime_guess::from_path(name).first_or_octet_stream();
    let cache = if name.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "no-cache" };
    let mut resp = Response::new(Body::from(file.data.into_owned()));
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_str(mime.as_ref()).unwrap_or(HeaderValue::from_static("application/octet-stream")));
    resp.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    resp
}
