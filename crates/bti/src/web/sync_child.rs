use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use redb::Database;
use tracing::{error, info};

pub async fn run_sync_loop(
    crawler_addr: SocketAddr,
    crawler_key: &[u8; 32],
    db: Arc<Database>,
) {
    loop {
        match sync_once(crawler_addr, crawler_key, &db).await {
            Ok(count) => {
                if count > 0 {
                    info!("web sync: {} new entries", count);
                }
            }
            Err(e) => {
                error!("web sync error: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

async fn sync_once(
    crawler_addr: SocketAddr,
    crawler_key: &[u8; 32],
    db: &Database,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let since = {
        let rtx = db.begin_read()?;
        bti_core::storage::latest_timestamp(&rtx)?
    };

    let conn = squic::dial(crawler_addr, crawler_key, squic::Config::default()).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    bti_core::sync_proto::write_sync_request(&mut send, since).await?;
    send.finish()?;

    let mut count = 0u64;
    loop {
        match bti_core::sync_proto::read_sync_entry(&mut recv).await? {
            Some((infohash, entry)) => {
                let wtx = db.begin_write()?;
                if bti_core::storage::put_entry(&wtx, &infohash, &entry)? {
                    count += 1;
                }
                wtx.commit()?;
            }
            None => break,
        }
    }

    Ok(count)
}
