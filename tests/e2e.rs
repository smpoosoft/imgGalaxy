//! End-to-end tests against an in-process mock of a dufs WebDAV server.

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use axum::Router;
use gallery::app::{App, Shared};
use gallery::config::Config;
use gallery::db::Db;
use gallery::{sync, thumbs};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

const PREFIX: &str = "/dav/gallery";
const HREF_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'/').remove(b'-').remove(b'.').remove(b'_');

#[derive(Default)]
struct Inner {
    files: BTreeMap<String, (Vec<u8>, i64)>,
    methods: BTreeSet<String>,
    offline: bool,
}

#[derive(Clone, Default)]
struct Mock(Arc<Mutex<Inner>>);

impl Mock {
    fn put(&self, path: &str, data: &[u8], mtime: i64) {
        self.0.lock().unwrap().files.insert(path.to_string(), (data.to_vec(), mtime));
    }
    fn remove(&self, path: &str) {
        self.0.lock().unwrap().files.remove(path);
    }
    fn rename(&self, from: &str, to: &str) {
        let mut g = self.0.lock().unwrap();
        let v = g.files.remove(from).expect("source exists");
        g.files.insert(to.to_string(), v);
    }
    fn set_offline(&self, v: bool) {
        self.0.lock().unwrap().offline = v;
    }
    fn methods(&self) -> BTreeSet<String> {
        self.0.lock().unwrap().methods.clone()
    }
}

fn http_date(secs: i64) -> String {
    httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(secs as u64))
}

async fn handle(State(m): State<Mock>, method: Method, uri: Uri, headers: HeaderMap) -> Response {
    let mut g = m.0.lock().unwrap();
    g.methods.insert(method.to_string());
    let resp = |s: StatusCode, b: Vec<u8>| Response::builder().status(s).body(Body::from(b)).unwrap();
    if g.offline {
        return resp(StatusCode::SERVICE_UNAVAILABLE, vec![]);
    }
    let decoded = percent_decode_str(uri.path()).decode_utf8_lossy().into_owned();
    let Some(rel) = decoded.strip_prefix(PREFIX) else { return resp(StatusCode::NOT_FOUND, vec![]) };
    let rel = if rel.is_empty() { "/" } else { rel };
    match method.as_str() {
        "PROPFIND" => {
            let dir = rel.trim_end_matches('/');
            let prefix = format!("{dir}/");
            let mut dirs: BTreeSet<String> = BTreeSet::new();
            let mut files: Vec<(String, usize, i64)> = Vec::new();
            let mut exists = dir.is_empty();
            for (p, (data, mt)) in g.files.iter() {
                if let Some(rest) = p.strip_prefix(&prefix) {
                    exists = true;
                    match rest.split_once('/') {
                        Some((d, _)) => {
                            dirs.insert(format!("{prefix}{d}"));
                        }
                        None => files.push((p.clone(), data.len(), *mt)),
                    }
                }
            }
            if !exists {
                return resp(StatusCode::NOT_FOUND, vec![]);
            }
            let href = |p: &str, is_dir: bool| {
                format!("{PREFIX}{}{}", utf8_percent_encode(p, HREF_SET), if is_dir { "/" } else { "" })
            };
            let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8" ?><D:multistatus xmlns:D="DAV:">"#);
            xml.push_str(&format!(
                "<D:response><D:href>{}</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>",
                href(dir, true)
            ));
            let depth = headers.get("depth").and_then(|v| v.to_str().ok()).unwrap_or("1");
            if depth != "0" {
                for d in dirs {
                    xml.push_str(&format!("<D:response><D:href>{}</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop></D:propstat></D:response>", href(&d, true)));
                }
                for (p, len, mt) in files {
                    xml.push_str(&format!(
                        "<D:response><D:href>{}</D:href><D:propstat><D:prop><D:resourcetype></D:resourcetype><D:getcontentlength>{len}</D:getcontentlength><D:getlastmodified>{}</D:getlastmodified></D:prop></D:propstat></D:response>",
                        href(&p, false), http_date(mt)
                    ));
                }
            }
            xml.push_str("</D:multistatus>");
            Response::builder().status(207).header(header::CONTENT_TYPE, "application/xml; charset=utf-8").body(Body::from(xml)).unwrap()
        }
        "GET" | "HEAD" => {
            let Some((data, mt)) = g.files.get(rel) else { return resp(StatusCode::NOT_FOUND, vec![]) };
            let (status, body, range) = match headers.get(header::RANGE).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("bytes=")) {
                Some(r) => {
                    let (a, b) = r.split_once('-').unwrap();
                    let a: usize = a.parse().unwrap_or(0);
                    let b: usize = b.parse().unwrap_or(data.len() - 1).min(data.len() - 1);
                    (StatusCode::PARTIAL_CONTENT, data[a..=b].to_vec(), Some(format!("bytes {a}-{b}/{}", data.len())))
                }
                None => (StatusCode::OK, data.clone(), None),
            };
            let mut b = Response::builder().status(status).header(header::ACCEPT_RANGES, "bytes").header(header::LAST_MODIFIED, http_date(*mt)).header(header::CONTENT_LENGTH, body.len());
            if let Some(r) = range {
                b = b.header(header::CONTENT_RANGE, r);
            }
            b.body(if method == Method::HEAD { Body::empty() } else { Body::from(body) }).unwrap()
        }
        _ => resp(StatusCode::METHOD_NOT_ALLOWED, vec![]),
    }
}

struct Env {
    app: Shared,
    mock: Mock,
    _tmp: tempfile::TempDir,
}

async fn env() -> Env {
    let mock = Mock::default();
    let router = Router::new().fallback(handle).with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.webdav.url = format!("http://{addr}{PREFIX}/");
    cfg.thumbnail.directory = tmp.path().join("thumbs");
    cfg.thumbnail.workers = 2;
    cfg.sync.interval = "0".into();
    cfg.thumbnail.interval = "0".into();
    let app = App::new(cfg, Db::open_in_memory().unwrap()).unwrap();
    Env { app, mock, _tmp: tmp }
}

async fn sync_once(e: &Env) -> sync::SyncResult {
    let r = sync::run_guarded(&e.app).await.expect("not already running");
    assert!(r.error.is_none(), "sync error: {:?}", r.error);
    r
}

async fn q<T: Send + 'static>(e: &Env, sql: &'static str, arg: String, f: fn(&rusqlite::Row) -> rusqlite::Result<T>) -> T {
    e.app.db.call(move |c| Ok(c.query_row(sql, [arg], |r| f(r))?)).await.unwrap()
}

async fn id_of(e: &Env, path: &str) -> i64 {
    q(e, "SELECT id FROM files WHERE path=?1", path.to_string(), |r| r.get(0)).await
}

async fn status_of(e: &Env, path: &str) -> String {
    q(e, "SELECT status FROM files WHERE path=?1", path.to_string(), |r| r.get(0)).await
}

async fn thumb_status_of(e: &Env, path: &str) -> String {
    q(e, "SELECT thumbnail_status FROM files WHERE path=?1", path.to_string(), |r| r.get(0)).await
}

async fn run_thumbs(e: &Env) -> thumbs::RunInfo {
    assert!(thumbs::trigger(&e.app, thumbs::Scope::default()));
    for _ in 0..600 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !e.app.thumbs.is_running() {
            break;
        }
    }
    assert!(!e.app.thumbs.is_running(), "thumbnail run did not finish");
    e.app.thumbs.current_run().unwrap()
}

fn ffmpeg_ok() -> bool {
    std::process::Command::new("ffmpeg").arg("-version").output().map(|o| o.status.success()).unwrap_or(false)
}

fn make_media(dir: &std::path::Path, name: &str, args: &[&str]) -> Vec<u8> {
    let out = dir.join(name);
    let st = std::process::Command::new("ffmpeg")
        .args(["-v", "error", "-y"])
        .args(args)
        .arg(&out)
        .status()
        .expect("ffmpeg runs");
    assert!(st.success());
    std::fs::read(out).unwrap()
}

// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn incremental_sync_new_noop_delete() {
    let e = env().await;
    for i in 0..30 {
        e.mock.put(&format!("/photos/2026/img{i:02}.jpg"), format!("jpeg-{i}").as_bytes(), 1_700_000_000 + i);
    }
    e.mock.put("/photos/clip.mp4", b"video", 1_700_000_100);
    e.mock.put("/notes.txt", b"not media", 1_700_000_100);
    e.mock.put("/.hidden/secret.jpg", b"hidden", 1_700_000_100);
    e.mock.put("/@eaDir/x.jpg", b"synology", 1_700_000_100);

    // first scan indexes everything that is media
    let r = sync_once(&e).await;
    assert_eq!((r.new, r.removed, r.moved, r.changed), (31, 0, 0, 0));
    let ids_before: Vec<i64> = e.app.db.call(|c| {
        let mut st = c.prepare("SELECT id FROM files ORDER BY id")?;
        let v = st.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<Vec<i64>>>()?;
        Ok(v)
    }).await.unwrap();
    assert_eq!(ids_before.len(), 31);
    assert_eq!(thumb_status_of(&e, "/photos/clip.mp4").await, "pending");

    // second scan: nothing changes, no records recreated
    let r = sync_once(&e).await;
    assert_eq!((r.new, r.removed, r.moved, r.changed, r.unchanged), (0, 0, 0, 0, 31));
    let ids_after: Vec<i64> = e.app.db.call(|c| {
        let mut st = c.prepare("SELECT id FROM files ORDER BY id")?;
        let v = st.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<Vec<i64>>>()?;
        Ok(v)
    }).await.unwrap();
    assert_eq!(ids_before, ids_after);

    // new file
    e.mock.put("/new/a.jpg", b"fresh", 1_700_001_000);
    let r = sync_once(&e).await;
    assert_eq!(r.new, 1);
    assert_eq!(thumb_status_of(&e, "/new/a.jpg").await, "pending");

    // deletion marks missing and removes thumbnail dir
    let id = id_of(&e, "/new/a.jpg").await;
    let tdir = e.app.cfg.thumbnail.directory.join(id.to_string());
    std::fs::create_dir_all(&tdir).unwrap();
    std::fs::write(tdir.join("480.jpg"), b"x").unwrap();
    e.mock.remove("/new/a.jpg");
    let r = sync_once(&e).await;
    assert_eq!(r.removed, 1);
    assert_eq!(status_of(&e, "/new/a.jpg").await, "missing");
    assert!(!tdir.exists(), "thumbnail must be removed with the source file");

    // only read-only methods were ever used
    let used = e.mock.methods();
    assert!(used.iter().all(|m| ["PROPFIND", "GET", "HEAD"].contains(&m.as_str())), "methods used: {used:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_and_move_keep_identity_and_user_data() {
    let e = env().await;
    e.mock.put("/a/photo.jpg", b"AAAA", 1_700_000_001);
    e.mock.put("/a/other.jpg", b"BBBBBB", 1_700_000_002);
    sync_once(&e).await;
    let id = id_of(&e, "/a/photo.jpg").await;
    // user data: tag + album
    e.app
        .db
        .call(move |c| {
            c.execute("INSERT INTO tags (id,name,created_at) VALUES (1,'旅行',0)", [])?;
            c.execute("INSERT INTO file_tags VALUES (?1, 1)", [id])?;
            c.execute("INSERT INTO albums (id,name,created_at) VALUES (1,'Trip',0)", [])?;
            c.execute("INSERT INTO album_files VALUES (1, ?1, 0)", [id])?;
            Ok(())
        })
        .await
        .unwrap();
    let tdir = e.app.cfg.thumbnail.directory.join(id.to_string());
    std::fs::create_dir_all(&tdir).unwrap();
    std::fs::write(tdir.join("480.jpg"), b"thumb").unwrap();
    e.app.db.call(move |c| Ok(c.execute("UPDATE files SET thumbnail_status='ready' WHERE id=?1", [id])?)).await.unwrap();

    // rename within the directory
    e.mock.rename("/a/photo.jpg", "/a/renamed.jpg");
    let r = sync_once(&e).await;
    assert_eq!((r.moved, r.new, r.removed), (1, 0, 0));
    assert_eq!(id_of(&e, "/a/renamed.jpg").await, id);

    // move to another directory
    e.mock.rename("/a/renamed.jpg", "/b/renamed.jpg");
    let r = sync_once(&e).await;
    assert_eq!((r.moved, r.new, r.removed), (1, 0, 0));
    assert_eq!(id_of(&e, "/b/renamed.jpg").await, id);
    assert_eq!(q(&e, "SELECT old_path FROM files WHERE path=?1", "/b/renamed.jpg".into(), |r| r.get::<_, String>(0)).await, "/a/renamed.jpg");

    // tags / albums / thumbnail survive
    let tags: i64 = e.app.db.call(move |c| Ok(c.query_row("SELECT COUNT(*) FROM file_tags WHERE file_id=?1", [id], |r| r.get(0))?)).await.unwrap();
    let albums: i64 = e.app.db.call(move |c| Ok(c.query_row("SELECT COUNT(*) FROM album_files WHERE file_id=?1", [id], |r| r.get(0))?)).await.unwrap();
    assert_eq!((tags, albums), (1, 1));
    assert!(tdir.join("480.jpg").exists());
    assert_eq!(thumb_status_of(&e, "/b/renamed.jpg").await, "ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_moves_are_resolved_by_content_hash() {
    let e = env().await;
    // two different files that agree on size and mtime
    e.mock.put("/x/one.jpg", b"1111111111", 1_700_000_500);
    e.mock.put("/x/two.jpg", b"2222222222", 1_700_000_500);
    let r = sync_once(&e).await;
    assert_eq!(r.hashed, 2, "duplicate size+mtime keys are hashed on demand");
    let (id1, id2) = (id_of(&e, "/x/one.jpg").await, id_of(&e, "/x/two.jpg").await);

    // both renamed to names that give no hint
    e.mock.rename("/x/one.jpg", "/y/aaa.jpg");
    e.mock.rename("/x/two.jpg", "/y/bbb.jpg");
    let r = sync_once(&e).await;
    assert_eq!((r.moved, r.new, r.removed), (2, 0, 0));
    assert_eq!(id_of(&e, "/y/aaa.jpg").await, id1);
    assert_eq!(id_of(&e, "/y/bbb.jpg").await, id2);
}

#[tokio::test(flavor = "multi_thread")]
async fn modified_file_invalidates_thumbnail() {
    let e = env().await;
    e.mock.put("/m.jpg", b"v1", 1_700_000_000);
    sync_once(&e).await;
    let id = id_of(&e, "/m.jpg").await;
    let tdir = e.app.cfg.thumbnail.directory.join(id.to_string());
    std::fs::create_dir_all(&tdir).unwrap();
    std::fs::write(tdir.join("480.jpg"), b"old").unwrap();
    e.app.db.call(move |c| Ok(c.execute("UPDATE files SET thumbnail_status='ready' WHERE id=?1", [id])?)).await.unwrap();

    e.mock.put("/m.jpg", b"version-two", 1_700_000_999);
    let r = sync_once(&e).await;
    assert_eq!(r.changed, 1);
    assert_eq!(id_of(&e, "/m.jpg").await, id);
    assert_eq!(thumb_status_of(&e, "/m.jpg").await, "pending");
    assert_eq!(status_of(&e, "/m.jpg").await, "changed");
    assert!(!tdir.exists());
    // next sync settles the transient state
    sync_once(&e).await;
    assert_eq!(status_of(&e, "/m.jpg").await, "active");
}

#[tokio::test(flavor = "multi_thread")]
async fn reappearing_file_is_revived_and_failed_scan_is_harmless() {
    let e = env().await;
    e.mock.put("/r.jpg", b"data", 1_700_000_000);
    e.mock.put("/keep.jpg", b"keep", 1_700_000_001);
    sync_once(&e).await;
    let id = id_of(&e, "/r.jpg").await;

    e.mock.remove("/r.jpg");
    sync_once(&e).await;
    assert_eq!(status_of(&e, "/r.jpg").await, "missing");
    e.mock.put("/r.jpg", b"data", 1_700_000_000);
    let r = sync_once(&e).await;
    assert_eq!(r.revived, 1);
    assert_eq!(status_of(&e, "/r.jpg").await, "active");
    assert_eq!(id_of(&e, "/r.jpg").await, id);

    // a failing source must never mark anything missing
    e.mock.set_offline(true);
    let r = sync::run_guarded(&e.app).await.unwrap();
    assert!(r.error.is_some());
    e.mock.set_offline(false);
    assert_eq!(status_of(&e, "/keep.jpg").await, "active");

    // an empty-looking source is refused by default
    e.mock.remove("/r.jpg");
    e.mock.remove("/keep.jpg");
    let r = sync::run_guarded(&e.app).await.unwrap();
    assert!(r.error.as_deref().unwrap_or("").contains("refusing"));
    assert_eq!(status_of(&e, "/keep.jpg").await, "active");
}

#[tokio::test(flavor = "multi_thread")]
async fn thumbnails_are_incremental_and_orphans_are_cleaned() {
    if !ffmpeg_ok() {
        eprintln!("ffmpeg not available, skipping");
        return;
    }
    let e = env().await;
    let work = tempfile::tempdir().unwrap();
    for i in 0..6 {
        let color = ["red", "green", "blue", "yellow", "white", "gray"][i];
        let png = make_media(work.path(), &format!("c{i}.png"), &["-f", "lavfi", "-i", &format!("color=c={color}:s=640x480"), "-frames:v", "1"]);
        e.mock.put(&format!("/imgs/c{i}.png"), &png, 1_700_000_000 + i as i64);
    }
    let mp4 = make_media(work.path(), "v.mp4", &["-f", "lavfi", "-i", "testsrc=d=2:s=320x240:r=10", "-pix_fmt", "yuv420p"]);
    e.mock.put("/imgs/v.mp4", &mp4, 1_700_000_100);
    e.mock.put("/imgs/broken.jpg", b"this is not an image", 1_700_000_101);
    sync_once(&e).await;

    let run = run_thumbs(&e).await;
    assert_eq!(run.processed, 8);
    assert_eq!(run.succeeded, 7);
    assert_eq!(run.failed, 1);
    assert_eq!(thumb_status_of(&e, "/imgs/c0.png").await, "ready");
    assert_eq!(thumb_status_of(&e, "/imgs/v.mp4").await, "ready");
    assert_eq!(thumb_status_of(&e, "/imgs/broken.jpg").await, "failed");
    let id = id_of(&e, "/imgs/c0.png").await;
    let jpg = e.app.cfg.thumbnail.directory.join(id.to_string()).join("480.jpg");
    assert!(jpg.exists());
    let (w, h): (i64, i64) = e.app.db.call(move |c| Ok(c.query_row("SELECT width,height FROM files WHERE id=?1", [id], |r| Ok((r.get(0)?, r.get(1)?)))?)).await.unwrap();
    assert_eq!((w, h), (640, 480));
    let vid_dur: f64 = q(&e, "SELECT duration FROM files WHERE path=?1", "/imgs/v.mp4".into(), |r| r.get(0)).await;
    assert!(vid_dur > 1.0);

    // running again does not regenerate anything (failed files need an explicit retry)
    let mtime_before = std::fs::metadata(&jpg).unwrap().modified().unwrap();
    let run = run_thumbs(&e).await;
    assert_eq!(run.processed, 0);
    assert_eq!(std::fs::metadata(&jpg).unwrap().modified().unwrap(), mtime_before);

    // source deleted -> thumbnail removed on next sync
    e.mock.remove("/imgs/c0.png");
    sync_once(&e).await;
    assert!(!jpg.parent().unwrap().exists());

    // orphan directory (e.g. left behind by a crash) is removed, foreign dirs are untouched
    let orphan = e.app.cfg.thumbnail.directory.join("999999");
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("480.jpg"), b"x").unwrap();
    let foreign = e.app.cfg.thumbnail.directory.join("not-ours");
    std::fs::create_dir_all(&foreign).unwrap();
    let r = sync_once(&e).await;
    assert_eq!(r.orphan_thumbnails_removed, 1);
    assert!(!orphan.exists());
    assert!(foreign.exists());
}

// ---------------------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------------------

async fn serve_api(e: &Env) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = gallery::api::router(e.app.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn jget(c: &reqwest::Client, url: String) -> serde_json::Value {
    let r = c.get(&url).send().await.unwrap();
    assert!(r.status().is_success(), "GET {url} -> {}", r.status());
    serde_json::from_str(&r.text().await.unwrap()).unwrap()
}

async fn jsend(c: &reqwest::Client, m: reqwest::Method, url: String, body: &str) -> (u16, serde_json::Value) {
    let r = c.request(m, &url).header("content-type", "application/json").body(body.to_string()).send().await.unwrap();
    let st = r.status().as_u16();
    let t = r.text().await.unwrap();
    (st, serde_json::from_str(&t).unwrap_or(serde_json::Value::Null))
}

#[tokio::test(flavor = "multi_thread")]
async fn api_tags_albums_search_and_media_proxy() {
    use reqwest::Method;
    let e = env().await;
    e.mock.put("/trip/cat.jpg", b"0123456789abcdef", 1_700_000_001);
    e.mock.put("/trip/dog.jpg", b"zzzzzzzzzzzzzzzzzz", 1_700_000_002);
    e.mock.put("/home/cat-sofa.png", b"png-bytes-here", 1_700_000_003);
    sync_once(&e).await;
    let base = serve_api(&e).await;
    let c = reqwest::Client::new();

    let all = jget(&c, format!("{base}/api/files")).await;
    assert_eq!(all["total"], 3);
    // default order: newest mtime first
    assert_eq!(all["items"][0]["name"], "cat-sofa.png");

    // search by file name / dir / kind
    assert_eq!(jget(&c, format!("{base}/api/files?q=cat")).await["total"], 2);
    assert_eq!(jget(&c, format!("{base}/api/files?dir=/trip")).await["total"], 2);
    assert_eq!(jget(&c, format!("{base}/api/files?kind=video")).await["total"], 0);
    assert_eq!(jget(&c, format!("{base}/api/files?sort=name&order=asc")).await["items"][0]["name"], "cat-sofa.png");

    // tags: create, bulk add, filter, remove
    let id_cat = id_of(&e, "/trip/cat.jpg").await;
    let id_dog = id_of(&e, "/trip/dog.jpg").await;
    let (st, t) = jsend(&c, Method::POST, format!("{base}/api/tags"), r##"{"name":"#旅行"}"##).await;
    assert_eq!(st, 200);
    assert_eq!(t["name"], "旅行");
    let (st, _) = jsend(&c, Method::POST, format!("{base}/api/files/bulk-tags"), &format!(r#"{{"file_ids":[{id_cat},{id_dog}],"add":["旅行","pets"]}}"#)).await;
    assert_eq!(st, 200);
    assert_eq!(jget(&c, format!("{base}/api/files?tag=旅行")).await["total"], 2);
    assert_eq!(jget(&c, format!("{base}/api/files?tag=%E6%97%85%E8%A1%8C,pets")).await["total"], 2);
    assert_eq!(jget(&c, format!("{base}/api/files?q=pets")).await["total"], 2, "free text also matches tags");
    let detail = jget(&c, format!("{base}/api/files/{id_cat}")).await;
    assert_eq!(detail["tags"].as_array().unwrap().len(), 2);
    let pets_id = detail["tags"].as_array().unwrap().iter().find(|t| t["name"] == "pets").unwrap()["id"].as_i64().unwrap();
    let (st, _) = jsend(&c, Method::DELETE, format!("{base}/api/files/{id_cat}/tags/{pets_id}"), "").await;
    assert_eq!(st, 200);
    assert_eq!(jget(&c, format!("{base}/api/files?tag=pets")).await["total"], 1);
    let tags = jget(&c, format!("{base}/api/tags")).await;
    assert_eq!(tags.as_array().unwrap().len(), 2);
    assert_eq!(jsend(&c, Method::POST, format!("{base}/api/tags"), r#"{"name":"  "}"#).await.0, 400);

    // albums
    let (st, album) = jsend(&c, Method::POST, format!("{base}/api/albums"), r#"{"name":"Trip","description":"d"}"#).await;
    assert_eq!(st, 200);
    let aid = album["id"].as_i64().unwrap();
    let (st, r) = jsend(&c, Method::POST, format!("{base}/api/albums/{aid}/files"), &format!(r#"{{"file_ids":[{id_cat},{id_dog},999999]}}"#)).await;
    assert_eq!((st, r["added"].as_i64()), (200, Some(2)));
    assert_eq!(jget(&c, format!("{base}/api/files?album={aid}")).await["total"], 2);
    let albums = jget(&c, format!("{base}/api/albums")).await;
    assert_eq!(albums[0]["count"], 2);
    assert!(albums[0]["cover_file_id"].is_i64());
    let (st, r) = jsend(&c, Method::PUT, format!("{base}/api/albums/{aid}"), r#"{"name":"Trip 2026"}"#).await;
    assert_eq!((st, r["name"].as_str()), (200, Some("Trip 2026")));
    let (st, _) = jsend(&c, Method::DELETE, format!("{base}/api/albums/{aid}/files"), &format!(r#"{{"file_ids":[{id_dog}]}}"#)).await;
    assert_eq!(st, 200);
    assert_eq!(jget(&c, format!("{base}/api/files?album={aid}")).await["total"], 1);
    assert_eq!(jsend(&c, Method::DELETE, format!("{base}/api/albums/{aid}"), "").await.0, 200);
    assert_eq!(jsend(&c, Method::DELETE, format!("{base}/api/albums/{aid}"), "").await.0, 404);
    assert_eq!(jget(&c, format!("{base}/api/files")).await["total"], 3, "deleting an album never touches files");

    // original proxied from WebDAV, with Range support and without leaking the source URL
    let r = c.get(format!("{base}/media/{id_cat}")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "image/jpeg");
    assert_eq!(r.bytes().await.unwrap().as_ref(), b"0123456789abcdef");
    let r = c.get(format!("{base}/media/{id_cat}")).header("range", "bytes=2-5").send().await.unwrap();
    assert_eq!(r.status(), 206);
    assert_eq!(r.headers()["content-range"], "bytes 2-5/16");
    assert_eq!(r.bytes().await.unwrap().as_ref(), b"2345");
    assert_eq!(c.get(format!("{base}/media/424242")).send().await.unwrap().status(), 404);
    assert_eq!(c.get(format!("{base}/thumbnail/{id_cat}")).send().await.unwrap().status(), 404);

    // status endpoints
    let s = jget(&c, format!("{base}/api/sync/status")).await;
    assert_eq!(s["running"], false);
    assert_eq!(s["counts"]["active"], 3);
    let t = jget(&c, format!("{base}/api/thumbnails/status")).await;
    assert_eq!(t["counts"]["pending"], 3);
    assert_eq!(jget(&c, format!("{base}/api/webdav/status")).await["ok"], true);
    let (st, _) = jsend(&c, Method::POST, format!("{base}/api/sync"), "").await;
    assert_eq!(st, 202);
    let dirs = jget(&c, format!("{base}/api/dirs")).await;
    assert_eq!(dirs.as_array().unwrap().len(), 2);
}
