use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info};

pub struct SyncConfig {
    pub server_addr: SocketAddr,
    pub server_key: [u8; 32],
    pub db_path: PathBuf,
    pub once: bool,
}

pub async fn run(config: SyncConfig) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(bti_core::storage::open_db(&config.db_path)?);
    info!("sync database opened at {}", config.db_path.display());

    loop {
        match sync_once(&config, &db).await {
            Ok(count) => {
                info!("synced {} entries", count);
            }
            Err(e) => {
                error!("sync error: {}", e);
            }
        }

        if config.once {
            break;
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    Ok(())
}

async fn sync_once(
    config: &SyncConfig,
    db: &redb::Database,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    // Determine last sync timestamp
    let since = {
        let rtx = db.begin_read()?;
        bti_core::storage::latest_timestamp(&rtx)?
    };

    info!("connecting to {} (since={})", config.server_addr, since);

    let conn = squic::dial(
        config.server_addr,
        &config.server_key,
        squic::Config::default(),
    )
    .await?;

    let (mut send, mut recv) = conn.open_bi().await?;

    // Send sync request
    bti_core::sync_proto::write_sync_request(&mut send, since).await?;
    send.finish()?;

    // Read entries
    let mut count = 0u64;
    let mut batch_count = 0u32;

    loop {
        match bti_core::sync_proto::read_sync_entry(&mut recv).await? {
            Some((infohash, entry)) => {
                let wtx = db.begin_write()?;
                if bti_core::storage::put_entry(&wtx, &infohash, &entry)? {
                    count += 1;
                }
                wtx.commit()?;

                batch_count += 1;
                if batch_count % 1000 == 0 {
                    info!("synced {} entries so far...", batch_count);
                }
            }
            None => break, // EOF
        }
    }

    Ok(count)
}
