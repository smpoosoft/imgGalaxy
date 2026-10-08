//! Shared application state.

use crate::config::Config;
use crate::db::Db;
use crate::sync::SyncCtl;
use crate::thumbs::ThumbCtl;
use crate::webdav::Client;
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct App {
    pub cfg: Config,
    pub db: Db,
    pub dav: Client,
    pub sync: SyncCtl,
    pub thumbs: ThumbCtl,
    pub shutdown: AtomicBool,
}

pub type Shared = Arc<App>;

impl App {
    pub fn new(cfg: Config, db: Db) -> Result<Shared> {
        let dav = Client::new(&cfg.webdav, cfg.sync.exclude.clone())?;
        Ok(Arc::new(App { cfg, db, dav, sync: SyncCtl::default(), thumbs: ThumbCtl::default(), shutdown: AtomicBool::new(false) }))
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}
