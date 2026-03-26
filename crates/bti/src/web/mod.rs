pub mod server;
pub mod sync_child;
pub mod search;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

pub struct WebConfig {
    pub crawler_addr: SocketAddr,
    pub crawler_key: [u8; 32],
    pub db_path: PathBuf,
    pub listen: SocketAddr,
}

pub async fn run(config: WebConfig) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(bti_core::storage::open_db(&config.db_path)?);
    info!("web database opened at {}", config.db_path.display());

    // Start background sync
    let sync_db = db.clone();
    let crawler_addr = config.crawler_addr;
    let crawler_key = config.crawler_key;
    tokio::spawn(async move {
        sync_child::run_sync_loop(crawler_addr, &crawler_key, sync_db).await;
    });

    // Start classification worker
    let classify_db = db.clone();
    tokio::spawn(async move {
        search::run_classifier_loop(classify_db).await;
    });

    // Start HTTP server
    server::run_server(config.listen, db).await?;

    Ok(())
}
