pub mod server;
pub mod sync_child;
pub mod search;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;

use tracing::info;

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
    let (sync_status, sync_info, crawler_db_size, crawler_total, crawler_mem_rss, crawler_disk_used, crawler_disk_total) =
        if let Some((addr, key)) = config.sync_target {
            info!("starting sync from {}", addr);
            let status = Arc::new(AtomicU8::new(0));
            let db_size = Arc::new(AtomicU64::new(0));
            let total = Arc::new(AtomicU64::new(0));
            let mem_rss = Arc::new(AtomicU64::new(0));
            let disk_used = Arc::new(AtomicU64::new(0));
            let disk_total = Arc::new(AtomicU64::new(0));
            let pubkey_b58 = bs58::encode(&key).into_string();
            let label = format!("{}/{}", addr, pubkey_b58);

            let sync_db = db.clone();
            let sync_status_clone = status.clone();
            let db_size_clone = db_size.clone();
            let total_clone = total.clone();
            let mem_rss_clone = mem_rss.clone();
            let disk_used_clone = disk_used.clone();
            let disk_total_clone = disk_total.clone();
            tokio::spawn(async move {
                sync_child::run_sync_loop(addr, &key, sync_db, sync_status_clone, db_size_clone, total_clone, mem_rss_clone, disk_used_clone, disk_total_clone).await;
            });

            (Some(status), Some(label), Some(db_size), Some(total), Some(mem_rss), Some(disk_used), Some(disk_total))
        } else {
            (None, None, None, None, None, None, None)
        };

    // Always run classifier
    let classify_db = db.clone();
    tokio::spawn(async move {
        search::run_classifier_loop(classify_db).await;
    });

    // Start HTTP server
    server::run_server(config.listen, db, config.db_path.clone(), sync_status, sync_info, crawler_db_size, crawler_total, crawler_mem_rss, crawler_disk_used, crawler_disk_total).await?;

    Ok(())
}
