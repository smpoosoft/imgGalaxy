//! Minimal read-only WebDAV client (PROPFIND / GET / HEAD) tailored to dufs.
//!
//! This module deliberately exposes no way to issue any other method.

use crate::config::WebdavCfg;
use crate::media;
use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::{Method, StatusCode};
use std::time::{Duration, Instant};

/// Characters left unescaped when building request paths.
const PATH_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

#[derive(Debug, Clone)]
pub struct Entry {
    /// Normalised path relative to the WebDAV root: "/a/b.jpg" (no trailing slash).
    pub path: String,
    pub is_dir: bool,
    pub size: i64,
    pub mtime: i64,
    pub content_type: Option<String>,
}

/// A media file discovered while crawling.
#[derive(Debug, Clone)]
pub struct RemoteFile {
    pub path: String,
    pub name: String,
    pub dir: String,
    pub ext: String,
    pub kind: media::Kind,
    pub size: i64,
    pub mtime: i64,
    pub mime: String,
}

pub struct Client {
    http: reqwest::Client,
    /// Base URL without trailing slash.
    base: String,
    /// Decoded base path with trailing slash, e.g. "/dav/gallery/".
    base_path: String,
    username: String,
    password: String,
    timeout: Duration,
    exclude: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DavStatus {
    pub ok: bool,
    pub latency_ms: u64,
    pub error: Option<String>,
}

impl Client {
    pub fn new(cfg: &WebdavCfg, exclude: Vec<String>) -> Result<Client> {
        let mut url = reqwest::Url::parse(cfg.url.trim()).context("invalid webdav.url")?;
        url.set_query(None);
        url.set_fragment(None);
        let decoded = percent_decode_str(url.path()).decode_utf8_lossy().into_owned();
        let mut base_path = decoded;
        if !base_path.ends_with('/') {
            base_path.push('/');
        }
        let base = url.as_str().trim_end_matches('/').to_string();
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(concat!("gallery/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Client {
            http,
            base,
            base_path,
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            timeout: Duration::from_secs(cfg.timeout_secs.max(5)),
            exclude,
        })
    }

    /// Absolute URL for a relative path (each segment percent-encoded).
    pub fn url_for(&self, rel: &str) -> String {
        let mut out = self.base.clone();
        for seg in rel.split('/').filter(|s| !s.is_empty()) {
            out.push('/');
            out.push_str(&utf8_percent_encode(seg, PATH_SET).to_string());
        }
        out
    }

    /// `Authorization` header value if credentials are configured (used for ffmpeg).
    pub fn auth_header(&self) -> Option<String> {
        if self.username.is_empty() {
            return None;
        }
        let raw = format!("{}:{}", self.username, self.password);
        Some(format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(raw)))
    }

    fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        let rb = self.http.request(method, url);
        if self.username.is_empty() {
            rb
        } else {
            rb.basic_auth(&self.username, Some(&self.password))
        }
    }

    /// PROPFIND with `Depth: 1` on a directory.
    pub async fn propfind(&self, dir: &str, depth: u8) -> Result<Vec<Entry>> {
        const BODY: &str = r#"<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/><getcontentlength/><getlastmodified/><getcontenttype/></prop></propfind>"#;
        let mut url = self.url_for(dir);
        if !url.ends_with('/') {
            url.push('/');
        }
        let method = Method::from_bytes(b"PROPFIND").expect("valid method");
        let resp = self
            .request(method, &url)
            .header("Depth", depth.to_string())
            .header("Content-Type", "application/xml; charset=utf-8")
            .timeout(self.timeout)
            .body(BODY)
            .send()
            .await
            .with_context(|| format!("PROPFIND {dir}: request failed"))?;
        let status = resp.status();
        if status != StatusCode::MULTI_STATUS && !status.is_success() {
            bail!("PROPFIND {dir}: unexpected HTTP status {status}");
        }
        let text = resp.text().await.with_context(|| format!("PROPFIND {dir}: reading body"))?;
        let mut entries = parse_multistatus(&text, &self.base_path).with_context(|| format!("PROPFIND {dir}: bad response"))?;
        // Drop the directory's own entry.
        let me = normalize(dir);
        entries.retain(|e| e.path != me);
        Ok(entries)
    }

    /// Crawl the whole tree (breadth-first, one request at a time) and return media files only.
    /// Any directory failure aborts the crawl so callers never act on a partial listing.
    pub async fn crawl(&self) -> Result<Vec<RemoteFile>> {
        let mut out = Vec::new();
        let mut queue: Vec<String> = vec!["/".to_string()];
        while let Some(dir) = queue.pop() {
            let entries = self.propfind(&dir, 1).await?;
            for e in entries {
                let (parent, name) = media::split_path(&e.path);
                if name.starts_with('.') || self.exclude.iter().any(|x| x == &name) {
                    continue;
                }
                if e.is_dir {
                    queue.push(e.path);
                    continue;
                }
                let ext = media::extension(&name);
                let Some(kind) = media::classify(&ext) else { continue };
                let mime = media::mime_for(&ext, e.content_type.as_deref(), kind);
                out.push(RemoteFile { path: e.path, name, dir: parent, ext, kind, size: e.size, mtime: e.mtime, mime });
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out.dedup_by(|a, b| a.path == b.path);
        Ok(out)
    }

    /// GET a file, optionally with a Range header. The response is returned unconsumed (streaming).
    pub async fn get(&self, rel: &str, range: Option<&str>) -> Result<reqwest::Response> {
        let mut rb = self.request(Method::GET, &self.url_for(rel));
        if let Some(r) = range {
            rb = rb.header("Range", r);
        }
        rb.send().await.with_context(|| format!("GET {rel}"))
    }

    /// HEAD a file.
    pub async fn head(&self, rel: &str) -> Result<reqwest::Response> {
        self.request(Method::HEAD, &self.url_for(rel)).timeout(self.timeout).send().await.with_context(|| format!("HEAD {rel}"))
    }

    /// Read `len` bytes at `offset` (used for cheap partial content hashing).
    pub async fn get_range_bytes(&self, rel: &str, offset: i64, len: i64) -> Result<bytes::Bytes> {
        let range = format!("bytes={}-{}", offset, offset + len - 1);
        let resp = self.get(rel, Some(&range)).await?;
        let status = resp.status();
        if status != StatusCode::PARTIAL_CONTENT && status != StatusCode::OK {
            bail!("GET {rel} range: HTTP {status}");
        }
        let body = resp.bytes().await?;
        // A server ignoring Range returns the full body; trim it ourselves.
        if status == StatusCode::OK && (body.len() as i64) > len {
            let start = offset.min(body.len() as i64) as usize;
            let end = ((offset + len) as usize).min(body.len());
            return Ok(body.slice(start..end));
        }
        Ok(body)
    }

    /// Cheap connectivity check: PROPFIND Depth 0 on the root.
    pub async fn check(&self) -> DavStatus {
        let t = Instant::now();
        let url = format!("{}/", self.base);
        let method = Method::from_bytes(b"PROPFIND").expect("valid method");
        let r = self.request(method, &url).header("Depth", "0").timeout(Duration::from_secs(10)).send().await;
        let latency_ms = t.elapsed().as_millis() as u64;
        match r {
            Ok(resp) if resp.status() == StatusCode::MULTI_STATUS || resp.status().is_success() => {
                DavStatus { ok: true, latency_ms, error: None }
            }
            Ok(resp) => DavStatus { ok: false, latency_ms, error: Some(format!("HTTP {}", resp.status())) },
            Err(e) => DavStatus { ok: false, latency_ms, error: Some(e.to_string()) },
        }
    }
}

fn normalize(path: &str) -> String {
    let t = path.trim_end_matches('/');
    if t.is_empty() {
        "/".to_string()
    } else if t.starts_with('/') {
        t.to_string()
    } else {
        format!("/{t}")
    }
}

/// Convert an `href` to a path relative to the WebDAV root.
fn rel_from_href(href: &str, base_path: &str) -> Option<String> {
    let mut p = href.trim();
    if let Some(rest) = p.strip_prefix("http://").or_else(|| p.strip_prefix("https://")) {
        p = rest.find('/').map(|i| &rest[i..]).unwrap_or("/");
    }
    let p = p.split(['?', '#']).next().unwrap_or("");
    let decoded = percent_decode_str(p).decode_utf8_lossy().into_owned();
    let with_slash = if decoded.ends_with('/') { decoded.clone() } else { format!("{decoded}/") };
    if with_slash == base_path {
        return Some("/".to_string());
    }
    let rest = decoded.strip_prefix(base_path)?;
    Some(normalize(rest))
}

pub fn parse_multistatus(xml: &str, base_path: &str) -> Result<Vec<Entry>> {
    #[derive(Default)]
    struct Cur {
        href: String,
        is_dir: bool,
        size: i64,
        mtime: i64,
        ctype: Option<String>,
    }
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut out = Vec::new();
    let mut cur: Option<Cur> = None;
    let mut field: Option<&'static str> = None;
    loop {
        match reader.read_event().map_err(|e| anyhow!("xml error at {}: {e}", reader.buffer_position()))? {
            Event::Start(e) => {
                let local = e.local_name();
                match local.as_ref() {
                    b"response" => cur = Some(Cur::default()),
                    b"href" => field = Some("href"),
                    b"getcontentlength" => field = Some("len"),
                    b"getlastmodified" => field = Some("mod"),
                    b"getcontenttype" => field = Some("ctype"),
                    b"collection" => {
                        if let Some(c) = cur.as_mut() {
                            c.is_dir = true;
                        }
                    }
                    _ => {}
                }
            }
            Event::Empty(e) => {
                if e.local_name().as_ref() == b"collection" {
                    if let Some(c) = cur.as_mut() {
                        c.is_dir = true;
                    }
                }
            }
            Event::Text(t) => {
                if let (Some(c), Some(f)) = (cur.as_mut(), field) {
                    let v = t.unescape().map_err(|e| anyhow!("xml text: {e}"))?.trim().to_string();
                    match f {
                        "href" => c.href.push_str(&v),
                        "len" => c.size = v.parse().unwrap_or(0),
                        "mod" => {
                            c.mtime = httpdate::parse_http_date(&v)
                                .ok()
                                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or(0)
                        }
                        "ctype" => c.ctype = Some(v),
                        _ => {}
                    }
                }
            }
            Event::End(e) => {
                let local = e.local_name();
                match local.as_ref() {
                    b"response" => {
                        if let Some(c) = cur.take() {
                            if let Some(path) = rel_from_href(&c.href, base_path) {
                                out.push(Entry { path, is_dir: c.is_dir, size: c.size, mtime: c.mtime, content_type: c.ctype });
                            }
                        }
                        field = None;
                    }
                    b"href" | b"getcontentlength" | b"getlastmodified" | b"getcontenttype" => field = None,
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dufs_style_response() {
        let xml = r#"<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
<D:response><D:href>/dav/gallery/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype><D:getlastmodified>Thu, 08 Oct 2026 09:00:00 GMT</D:getlastmodified></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
<D:response><D:href>/dav/gallery/%E6%97%85%E8%A1%8C/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop></D:propstat></D:response>
<D:response><D:href>/dav/gallery/a%20b.jpg</D:href><D:propstat><D:prop><D:resourcetype></D:resourcetype><D:getcontentlength>1234</D:getcontentlength><D:getlastmodified>Thu, 08 Oct 2026 09:00:00 GMT</D:getlastmodified><D:getcontenttype>image/jpeg</D:getcontenttype></D:prop></D:propstat></D:response>
</D:multistatus>"#;
        let e = parse_multistatus(xml, "/dav/gallery/").unwrap();
        assert_eq!(e.len(), 3);
        assert_eq!(e[0].path, "/");
        assert!(e[0].is_dir);
        assert_eq!(e[1].path, "/旅行");
        assert!(e[1].is_dir);
        assert_eq!(e[2].path, "/a b.jpg");
        assert_eq!(e[2].size, 1234);
        assert_eq!(e[2].mtime, 1791450000);
        assert_eq!(e[2].content_type.as_deref(), Some("image/jpeg"));
    }

    #[test]
    fn href_variants() {
        assert_eq!(rel_from_href("http://h:1/x/y/a.jpg", "/x/y/").as_deref(), Some("/a.jpg"));
        assert_eq!(rel_from_href("/x/y", "/x/y/").as_deref(), Some("/"));
        assert_eq!(rel_from_href("/other/a.jpg", "/x/y/"), None);
    }
}
