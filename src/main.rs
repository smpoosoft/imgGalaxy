use anyhow::{Context, Result};
use gallery::app::App;
use gallery::config::{Config, EXAMPLE_CONFIG};
use gallery::{api, db::Db, sync, thumbs};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use tracing::{error, info};

const HELP: &str = "gallery - lightweight single-binary gallery for a read-only WebDAV (dufs) source

USAGE:
    gallery [OPTIONS]

OPTIONS:
    -c, --config <FILE>   Path to config file (default: ./config.toml)
        --print-config    Print an example configuration and exit
    -V, --version         Print version
    -h, --help            Print this help

Environment overrides: GALLERY_WEBDAV_URL, GALLERY_WEBDAV_USERNAME, GALLERY_WEBDAV_PASSWORD,
GALLERY_HOST, GALLERY_PORT, GALLERY_LOG (e.g. info, debug)";

#[tokio::main]
async fn main() {
    if let Err(e) = real_main().await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut cfg_path: Option<PathBuf> = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => {
                println!("{HELP}");
                return Ok(());
            }
            "-V" | "--version" => {
                println!("gallery {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--print-config" => {
                print!("{EXAMPLE_CONFIG}");
                return Ok(());
            }
            "-c" | "--config" => cfg_path = Some(args.next().context("--config needs a path")?.into()),
            other => anyhow::bail!("unknown argument '{other}' (see --help)"),
        }
    }
    let explicit = cfg_path.is_some();
    let cfg = Config::load(&cfg_path.unwrap_or_else(|| "config.toml".into()), explicit)?;

    let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log.level).unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();

    std::fs::create_dir_all(&cfg.thumbnail.directory).with_context(|| format!("creating {}", cfg.thumbnail.directory.display()))?;
    std::fs::create_dir_all(cfg.cache_dir()).ok();
    let db = Db::open(&cfg.database.path)?;
    let addr = format!("{}:{}", cfg.server.host, cfg.server.port);
    let app = App::new(cfg, db)?;

    thumbs::recover(&app).await?;
    let dav = app.dav.check().await;
    if dav.ok {
        info!("WebDAV connection ok ({} ms)", dav.latency_ms);
    } else {
        error!("WebDAV connection failed: {} (will keep retrying on each sync)", dav.error.unwrap_or_default());
    }

    tokio::spawn(sync::scheduler(app.clone()));
    tokio::spawn(thumbs::scheduler(app.clone()));

    let listener = tokio::net::TcpListener::bind(&addr).await.with_context(|| format!("binding {addr}"))?;
    info!("gallery {} listening on http://{addr}", env!("CARGO_PKG_VERSION"));
    let app2 = app.clone();
    axum::serve(listener, api::router(app.clone()))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            info!("shutting down");
            app2.shutdown.store(true, Ordering::SeqCst);
            app2.thumbs.cancel();
        })
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}
