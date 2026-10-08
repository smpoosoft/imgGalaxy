//! Configuration loading (TOML file + a few environment overrides).

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerCfg,
    pub database: DatabaseCfg,
    pub webdav: WebdavCfg,
    pub sync: SyncCfg,
    pub thumbnail: ThumbCfg,
    pub log: LogCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerCfg {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DatabaseCfg {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebdavCfg {
    pub url: String,
    pub username: String,
    pub password: String,
    /// Must stay `true`: the gallery only ever issues PROPFIND / GET / HEAD.
    pub readonly: bool,
    /// Per-request timeout for PROPFIND, in seconds.
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SyncCfg {
    /// Sync interval such as "10m". "0" / "off" disables periodic sync.
    pub interval: String,
    /// Run one sync at startup.
    pub on_startup: bool,
    /// Days to keep records of vanished files (so moves/misdetections can be healed).
    pub retention_days: i64,
    /// Upper bound on partial content hashes computed per sync run.
    pub max_hash_per_run: usize,
    /// Directory / file names that are never indexed.
    pub exclude: Vec<String>,
    /// Accept a scan that finds zero media files even though the index is not empty.
    /// Off by default so an unmounted/misconfigured source can never wipe the library view.
    pub allow_empty_remote: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ThumbCfg {
    pub workers: usize,
    /// Periodic generation interval such as "30m". "0" / "off" disables it.
    pub interval: String,
    /// Files processed per periodic run.
    pub batch_size: usize,
    pub ffmpeg: String,
    pub ffprobe: String,
    pub directory: PathBuf,
    /// Long edge of the thumbnail in pixels.
    pub size: u32,
    pub timeout_secs: u64,
    pub max_retries: i64,
    /// Seconds to wait before a failed thumbnail is retried by the periodic job.
    pub retry_backoff_secs: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogCfg {
    pub level: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerCfg::default(),
            database: DatabaseCfg::default(),
            webdav: WebdavCfg::default(),
            sync: SyncCfg::default(),
            thumbnail: ThumbCfg::default(),
            log: LogCfg::default(),
        }
    }
}
impl Default for ServerCfg {
    fn default() -> Self {
        Self { host: "0.0.0.0".into(), port: 8080 }
    }
}
impl Default for DatabaseCfg {
    fn default() -> Self {
        Self { path: "./data/gallery.db".into() }
    }
}
impl Default for WebdavCfg {
    fn default() -> Self {
        Self { url: String::new(), username: String::new(), password: String::new(), readonly: true, timeout_secs: 120 }
    }
}
impl Default for SyncCfg {
    fn default() -> Self {
        Self {
            interval: "10m".into(),
            on_startup: true,
            retention_days: 30,
            max_hash_per_run: 200,
            allow_empty_remote: false,
            exclude: [".DS_Store", "@eaDir", "#recycle", "Thumbs.db", "$RECYCLE.BIN", "lost+found"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}
impl Default for ThumbCfg {
    fn default() -> Self {
        Self {
            workers: 1,
            interval: "30m".into(),
            batch_size: 200,
            ffmpeg: "ffmpeg".into(),
            ffprobe: String::new(),
            directory: "./data/thumbnails".into(),
            size: 480,
            timeout_secs: 120,
            max_retries: 3,
            retry_backoff_secs: 3600,
        }
    }
}
impl Default for LogCfg {
    fn default() -> Self {
        Self { level: "info".into() }
    }
}

impl Config {
    /// Load configuration. A missing file at the *default* path is tolerated (defaults + env).
    pub fn load(path: &Path, explicit: bool) -> Result<Config> {
        let mut cfg: Config = if path.exists() {
            let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?
        } else if explicit {
            bail!("config file {} not found", path.display());
        } else {
            Config::default()
        };
        cfg.apply_env();
        cfg.validate()?;
        Ok(cfg)
    }

    fn apply_env(&mut self) {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if let Some(v) = get("GALLERY_WEBDAV_URL") {
            self.webdav.url = v;
        }
        if let Some(v) = get("GALLERY_WEBDAV_USERNAME") {
            self.webdav.username = v;
        }
        if let Some(v) = get("GALLERY_WEBDAV_PASSWORD") {
            self.webdav.password = v;
        }
        if let Some(v) = get("GALLERY_PORT").and_then(|v| v.parse().ok()) {
            self.server.port = v;
        }
        if let Some(v) = get("GALLERY_HOST") {
            self.server.host = v;
        }
        if let Some(v) = get("GALLERY_LOG") {
            self.log.level = v;
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.webdav.url.trim().is_empty() {
            bail!("webdav.url is not configured (set it in config.toml or GALLERY_WEBDAV_URL)");
        }
        if !self.webdav.url.starts_with("http://") && !self.webdav.url.starts_with("https://") {
            bail!("webdav.url must start with http:// or https://");
        }
        if !self.webdav.readonly {
            bail!("webdav.readonly must be true: the gallery never writes to the source");
        }
        if self.thumbnail.workers == 0 {
            bail!("thumbnail.workers must be >= 1");
        }
        if self.thumbnail.size < 32 {
            bail!("thumbnail.size must be >= 32");
        }
        parse_interval(&self.sync.interval).context("sync.interval")?;
        parse_interval(&self.thumbnail.interval).context("thumbnail.interval")?;
        Ok(())
    }

    pub fn sync_interval(&self) -> Option<Duration> {
        parse_interval(&self.sync.interval).ok().flatten()
    }
    pub fn thumb_interval(&self) -> Option<Duration> {
        parse_interval(&self.thumbnail.interval).ok().flatten()
    }

    pub fn ffprobe_path(&self) -> String {
        if !self.thumbnail.ffprobe.is_empty() {
            return self.thumbnail.ffprobe.clone();
        }
        let ff = Path::new(&self.thumbnail.ffmpeg);
        match ff.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(dir) => dir.join("ffprobe").to_string_lossy().into_owned(),
            None => "ffprobe".into(),
        }
    }

    /// Directory for scratch files (`<db dir>/cache`).
    pub fn cache_dir(&self) -> PathBuf {
        self.database.path.parent().map(|p| p.join("cache")).unwrap_or_else(|| PathBuf::from("./data/cache"))
    }
}

/// Parse "30s", "10m", "2h", "1d", plain seconds, or "0"/"off"/"" (= disabled).
pub fn parse_interval(s: &str) -> Result<Option<Duration>> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() || s == "0" || s == "off" || s == "never" || s == "false" {
        return Ok(None);
    }
    let (num, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1u64),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('d') => (&s[..s.len() - 1], 86400),
        Some(c) if c.is_ascii_digit() => (&s[..], 1),
        _ => bail!("invalid duration '{s}' (use e.g. 30s, 10m, 2h, 1d)"),
    };
    let n: u64 = num.trim().parse().with_context(|| format!("invalid duration '{s}'"))?;
    if n == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs(n * mult)))
}

pub const EXAMPLE_CONFIG: &str = r##"# gallery configuration

[server]
host = "0.0.0.0"
port = 8080

[database]
path = "./data/gallery.db"

[webdav]
# dufs WebDAV root that contains your media. The gallery only issues PROPFIND / GET / HEAD.
url = "http://192.168.1.200/dav/gallery/"
username = ""
password = ""
readonly = true

[sync]
interval = "10m"        # "0" disables periodic sync
on_startup = true
retention_days = 30     # keep records of vanished files this long
max_hash_per_run = 200  # cap on on-demand content hashes per sync
exclude = [".DS_Store", "@eaDir", "#recycle", "Thumbs.db"]
allow_empty_remote = false  # refuse to mark everything missing if the source looks empty

[thumbnail]
workers = 1
interval = "30m"        # "0" disables the periodic job (manual trigger still works)
batch_size = 200        # files handled per periodic run
ffmpeg = "ffmpeg"       # e.g. "/usr/bin/ffmpeg"
directory = "./data/thumbnails"
size = 480              # long edge in pixels

[log]
level = "info"
"##;
