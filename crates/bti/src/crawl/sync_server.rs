use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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

    let listener = squic::listen(addr, &signing_key, squic::Config {
        keep_alive: Some(Duration::from_secs(10)),
        ..Default::default()
    }).await?;

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

    // Send historical entries
    let mut batch = Vec::new();
    bti_core::storage::entries_since(&rtx, since, |infohash, entry| {
        batch.push((infohash, entry));
        true
    })?;
    drop(rtx);

    let peer = conn.remote_address();
    for (infohash, entry) in &batch {
        bti_core::sync_proto::write_sync_entry(&mut send, infohash, entry).await?;
    }
    info!("sync: sent {} historical entries to {}", batch.len(), peer);

    // Track cursor as latest timestamp sent
    let mut cursor = batch.last().map(|(_, e)| e.discovered_at).unwrap_or(since);

    // Persistent push: poll for new entries every 2s
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let rtx = db.begin_read()?;
        let mut new_batch = Vec::new();
        bti_core::storage::entries_since(&rtx, cursor, |infohash, entry| {
            new_batch.push((infohash, entry));
            true
        })?;
        drop(rtx);

        for (infohash, entry) in &new_batch {
            bti_core::sync_proto::write_sync_entry(&mut send, infohash, entry).await?;
            if entry.discovered_at > cursor {
                cursor = entry.discovered_at;
            }
        }
    }
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
