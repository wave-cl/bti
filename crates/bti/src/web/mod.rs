pub mod search;
pub mod server;
pub mod sync_child;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;

use tracing::info;

/// The five counters a crawler publishes about itself.
///
/// **A struct because five consecutive `Arc<AtomicU64>` parameters are a latent
/// bug at every call site.** They were threaded positionally through three
/// functions, and swapping any two — disk-used for disk-total, say — compiles
/// silently and is wrong only on a dashboard nobody diffs. Named fields make
/// the mapping explicit at each call rather than by counting commas.
#[derive(Clone)]
pub struct CrawlerStats {
    pub db_size: Arc<AtomicU64>,
    pub total: Arc<AtomicU64>,
    pub mem_rss: Arc<AtomicU64>,
    pub disk_used: Arc<AtomicU64>,
    pub disk_total: Arc<AtomicU64>,
}

pub struct WebConfig {
    pub crawl: bool,
    pub sync_target: Option<(SocketAddr, [u8; 32])>,
    pub db_path: PathBuf,
    pub listen: SocketAddr,
}

pub async fn run(config: WebConfig) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(bti_core::storage::open_db(&config.db_path)?);
    info!("database opened at {}", config.db_path.display());

    // Mode 1: embedded crawler (same-machine)
    if config.crawl {
        info!("starting embedded DHT crawler");
        let crawl_db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::crawl::start_crawler_only(crawl_db).await {
                tracing::error!("crawler error: {}", e);
            }
        });
    }

    // Mode 2: sync from remote crawler
    let (sync_status, sync_info, crawler) = if let Some((addr, key)) = config.sync_target {
        info!("starting sync from {}", addr);
        let status = Arc::new(AtomicU8::new(0));
        let stats = CrawlerStats {
            db_size: Arc::new(AtomicU64::new(0)),
            total: Arc::new(AtomicU64::new(0)),
            mem_rss: Arc::new(AtomicU64::new(0)),
            disk_used: Arc::new(AtomicU64::new(0)),
            disk_total: Arc::new(AtomicU64::new(0)),
        };
        let pubkey_b58 = bs58::encode(&key).into_string();
        let label = format!("magnet:?xt=urn:sqc:{}", pubkey_b58);

        let sync_db = db.clone();
        let sync_status = status.clone();
        let sync_stats = stats.clone();
        tokio::spawn(async move {
            sync_child::run_sync_loop(addr, &key, sync_db, sync_status, sync_stats).await;
        });

        (Some(status), Some(label), Some(stats))
    } else {
        (None, None, None)
    };

    // Always run classifier
    let classify_db = db.clone();
    tokio::spawn(async move {
        search::run_classifier_loop(classify_db).await;
    });

    // Start HTTP server
    server::run_server(
        config.listen,
        db,
        config.db_path.clone(),
        sync_status,
        sync_info,
        crawler,
    )
    .await?;

    Ok(())
}
