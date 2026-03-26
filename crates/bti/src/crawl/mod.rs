pub mod config;
pub mod crawler;
pub mod sync_server;

use std::sync::Arc;

use tracing::info;

use config::CrawlConfig;

pub async fn run(migrate_path: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let config = CrawlConfig::from_env();

    let db = Arc::new(bti_core::storage::open_db(&config.db_path)?);
    info!("database opened at {}", config.db_path.display());

    if let Some(path) = migrate_path {
        crate::migrate::run_migrate(&path, &db)?;
        return Ok(());
    }

    // Start sync server
    let sync_db = db.clone();
    let sync_addr = config.sync_addr;
    let sync_key = config.sync_key_file.clone();
    tokio::spawn(async move {
        if let Err(e) = sync_server::run_sync_server(sync_addr, &sync_key, sync_db).await {
            tracing::error!("sync server error: {}", e);
        }
    });

    // Run crawler (blocks)
    crawler::run_crawler(&config, db).await?;

    Ok(())
}
