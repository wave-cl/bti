pub mod server;
pub mod sync_child;
pub mod search;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU8;
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
    let (sync_status, sync_info) = if let Some((addr, key)) = config.sync_target {
        info!("starting sync from {}", addr);
        let status = Arc::new(AtomicU8::new(0));
        let pubkey_b58 = bs58::encode(&key).into_string();
        let label = format!("{}/{}", addr, pubkey_b58);

        let sync_db = db.clone();
        let sync_status_clone = status.clone();
        tokio::spawn(async move {
            sync_child::run_sync_loop(addr, &key, sync_db, sync_status_clone).await;
        });

        (Some(status), Some(label))
    } else {
        (None, None)
    };

    // Always run classifier
    let classify_db = db.clone();
    tokio::spawn(async move {
        search::run_classifier_loop(classify_db).await;
    });

    // Start HTTP server
    server::run_server(config.listen, db, config.db_path.clone(), sync_status, sync_info).await?;

    Ok(())
}
