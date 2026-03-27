use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use redb::Database;
use tracing::{info, warn};

pub async fn run_sync_server(
    addr: SocketAddr,
    key_file: &Path,
    db: Arc<Database>,
    db_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let signing_key = load_or_generate_key(key_file)?;
    let pub_key = signing_key.verifying_key();

    info!(
        "sync server listening on {} (pubkey: {})",
        addr,
        hex::encode(pub_key.to_bytes())
    );

    let listener = squic::listen(addr, &signing_key, squic::Config::default()).await?;

    loop {
        let incoming = match listener.accept().await {
            Some(inc) => inc,
            None => {
                info!("sync server stopped");
                return Ok(());
            }
        };

        let db = db.clone();
        let db_path = db_path.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_sync_connection(conn, db, db_path).await {
                        warn!("sync connection error: {}", e);
                    }
                }
                Err(e) => {
                    warn!("sync accept error: {}", e);
                }
            }
        });
    }
}

async fn handle_sync_connection(
    conn: quinn::Connection,
    db: Arc<Database>,
    db_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut send, mut recv) = conn.accept_bi().await?;

    let since = bti_core::sync_proto::read_sync_request(&mut recv).await?;
    info!("sync request from {}: since={}", conn.remote_address(), since);

    let rtx = db.begin_read()?;
    let db_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    let total = bti_core::storage::count(&rtx)?;
    let mem_rss = read_mem_rss();
    let (disk_used, disk_total) = disk_usage(&db_path);
    bti_core::sync_proto::write_sync_header(&mut send, &bti_core::sync_proto::SyncHeader {
        db_size, total, mem_rss, disk_used, disk_total,
    }).await?;

    let mut entries = Vec::new();
    bti_core::storage::entries_since(&rtx, since, |infohash, entry| {
        entries.push((infohash, entry));
        entries.len() < 100_000 // cap at 100k per sync
    })?;
    drop(rtx);

    for (infohash, entry) in &entries {
        bti_core::sync_proto::write_sync_entry(&mut send, infohash, entry).await?;
    }

    bti_core::sync_proto::write_sync_eof(&mut send).await?;
    send.finish()?;

    info!("sync complete: sent {} entries to {}", entries.len(), conn.remote_address());
    conn.closed().await;
    Ok(())
}

fn read_mem_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
        * 1024
}

fn disk_usage(db_path: &Path) -> (u64, u64) {
    let dir = db_path.parent().unwrap_or(db_path);
    let path_str = dir.to_str().unwrap_or("/");
    let c_path = match std::ffi::CString::new(path_str) {
        Ok(p) => p,
        Err(_) => return (0, 0),
    };
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let block = stat.f_frsize as u64;
            let total = stat.f_blocks as u64 * block;
            let free = stat.f_bfree as u64 * block;
            (total.saturating_sub(free), total)
        } else {
            (0, 0)
        }
    }
}

fn load_or_generate_key(path: &Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    if path.exists() {
        let hex_str = std::fs::read_to_string(path)?.trim().to_string();
        let bytes = hex::decode(&hex_str)?;
        if bytes.len() != 32 {
            return Err("invalid key file: expected 32 bytes".into());
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&bytes);
        Ok(SigningKey::from_bytes(&key_bytes))
    } else {
        let (signing_key, pub_key_bytes) = squic::generate_keypair();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, hex::encode(signing_key.to_bytes()))?;
        info!(
            "generated new sync key: {} (saved to {})",
            hex::encode(pub_key_bytes),
            path.display()
        );
        Ok(signing_key)
    }
}
