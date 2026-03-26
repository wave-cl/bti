use std::collections::HashSet;
use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bti_core::model::{TorrentEntry, EPOCH_OFFSET};
use bti_dht::bloom::StableBloomFilter;
use bti_dht::client::DhtClient;
use bti_dht::fetcher::MetadataFetcher;
use bti_dht::krpc::Server;
use bti_dht::ktable::{self, KTable};
use bti_dht::responder::DhtResponder;
use redb::Database;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, info, trace, warn};

use super::config::CrawlConfig;

pub struct CrawlerStats {
    pub queries_received: AtomicU64,
    pub infohashes_found: AtomicU64,
    pub meta_requests: AtomicU64,
    pub meta_success: AtomicU64,
    pub meta_failed: AtomicU64,
    pub persisted: AtomicU64,
}

impl CrawlerStats {
    fn new() -> Self {
        Self {
            queries_received: AtomicU64::new(0),
            infohashes_found: AtomicU64::new(0),
            meta_requests: AtomicU64::new(0),
            meta_success: AtomicU64::new(0),
            meta_failed: AtomicU64::new(0),
            persisted: AtomicU64::new(0),
        }
    }
}

struct TriageItem {
    infohash: [u8; 20],
    peer: SocketAddr,
}

struct DiscoveredNode {
    id: [u8; 20],
    addr: SocketAddrV4,
}

struct MetaResult {
    infohash: [u8; 20],
    name: String,
    size: u64,
}

pub async fn run_crawler(
    config: &CrawlConfig,
    db: Arc<Database>,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = ktable::random_node_id();
    let ktable = Arc::new(KTable::new(node_id));
    let stats = Arc::new(CrawlerStats::new());
    let bloom = Arc::new(Mutex::new(StableBloomFilter::new(10_000_000, 0.001)));
    let sf = config.scaling_factor;

    // Channel for discovered nodes from incoming DHT queries
    let (discovered_tx, mut discovered_rx) = mpsc::channel::<DiscoveredNode>(100 * sf);

    // Start KRPC server with callback that feeds the pipeline
    let responder = Arc::new(DhtResponder::new(node_id, ktable.clone()));
    let kt = ktable.clone();
    let disc_tx = discovered_tx.clone();
    let stats_cb = stats.clone();
    let on_node_discovered: Arc<dyn Fn([u8; 20], SocketAddr) + Send + Sync> =
        Arc::new(move |id, addr| {
            stats_cb.queries_received.fetch_add(1, Ordering::Relaxed);
            if let SocketAddr::V4(v4) = addr {
                kt.put_node(id, v4, false);
                let _ = disc_tx.try_send(DiscoveredNode { id, addr: v4 });
            }
        });

    let server =
        Server::start(config.dht_port, responder, Some(on_node_discovered)).await?;
    info!("DHT server started on {}", server.local_addr());

    let client = Arc::new(DhtClient::new(node_id, server.clone()));
    let fetcher = Arc::new(MetadataFetcher::new(Duration::from_secs(6)));

    // Channels
    let (triage_tx, mut triage_rx) = mpsc::channel::<TriageItem>(100 * sf);
    let (meta_tx, mut meta_rx) = mpsc::channel::<MetaResult>(1000);

    // Bootstrap
    info!("bootstrapping DHT...");
    bootstrap(&client, &ktable, &config.bootstrap_nodes).await;
    info!("bootstrap complete, {} nodes in routing table", ktable.node_count());

    // Spawn stats logger
    let stats_ref = stats.clone();
    let kt_ref = ktable.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            info!(
                "stats: nodes={} queries_in={} found={} meta_req={} meta_ok={} meta_fail={} persisted={}",
                kt_ref.node_count(),
                stats_ref.queries_received.load(Ordering::Relaxed),
                stats_ref.infohashes_found.load(Ordering::Relaxed),
                stats_ref.meta_requests.load(Ordering::Relaxed),
                stats_ref.meta_success.load(Ordering::Relaxed),
                stats_ref.meta_failed.load(Ordering::Relaxed),
                stats_ref.persisted.load(Ordering::Relaxed),
            );
        }
    });

    // Spawn discovered nodes handler — probes incoming nodes with sample_infohashes
    let client_dn = client.clone();
    let ktable_dn = ktable.clone();
    let triage_tx_dn = triage_tx.clone();
    let stats_dn = stats.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(20 * sf));
        let mut seen_ips: HashSet<std::net::Ipv4Addr> = HashSet::new();

        while let Some(node) = discovered_rx.recv().await {
            // Dedup by IP
            if !seen_ips.insert(*node.addr.ip()) {
                continue;
            }
            if seen_ips.len() > 50000 {
                seen_ips.clear();
            }

            let sem = sem.clone();
            let client = client_dn.clone();
            let kt = ktable_dn.clone();
            let tx = triage_tx_dn.clone();
            let stats = stats_dn.clone();
            let target = ktable::random_node_id();

            tokio::spawn(async move {
                let _permit = match sem.try_acquire() {
                    Ok(p) => p,
                    Err(_) => return,
                };
                let addr = SocketAddr::V4(node.addr);
                match client.sample_infohashes(addr, target).await {
                    Ok(result) => {
                        let support = !result.samples.is_empty();
                        let next_at = if result.interval > 0 {
                            Instant::now() + Duration::from_secs(result.interval as u64)
                        } else {
                            Instant::now() + Duration::from_secs(60)
                        };
                        kt.put_node_bep51(result.id, node.addr, support, next_at);

                        for hash in result.samples {
                            stats.infohashes_found.fetch_add(1, Ordering::Relaxed);
                            let _ = tx.try_send(TriageItem {
                                infohash: hash,
                                peer: addr,
                            });
                        }
                        for n in result.nodes {
                            kt.put_node(n.id, n.addr, false);
                        }
                    }
                    Err(_) => {}
                }
            });
        }
    });

    // Spawn find_node feeder (expands routing table)
    let client_fn = client.clone();
    let ktable_fn = ktable.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(10 * sf));
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let cutoff = Instant::now() - Duration::from_secs(900);
            let nodes = ktable_fn.get_oldest_nodes(cutoff, 10);
            for node in nodes {
                let sem = sem.clone();
                let client = client_fn.clone();
                let kt = ktable_fn.clone();
                let target = ktable::random_node_id();
                tokio::spawn(async move {
                    let _permit = match sem.try_acquire() {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    match client.find_node(SocketAddr::V4(node.addr), target).await {
                        Ok(result) => {
                            for n in result.nodes {
                                kt.put_node(n.id, n.addr, false);
                            }
                        }
                        Err(_) => {
                            kt.drop_node(&node.id);
                        }
                    }
                });
            }
        }
    });

    // Spawn sample_infohashes feeder — queries both confirmed BEP 51 nodes AND untested nodes
    let client_si = client.clone();
    let ktable_si = ktable.clone();
    let triage_tx_si = triage_tx.clone();
    let stats_si = stats.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(10 * sf));
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            // Get confirmed BEP 51 nodes + untested nodes (oldest, never responded to sample_infohashes)
            let mut nodes = ktable_si.get_nodes_for_sample_infohashes(60);
            if nodes.len() < 60 {
                // Fill with oldest untested nodes
                let cutoff = Instant::now() - Duration::from_secs(300);
                let extra = ktable_si.get_oldest_nodes(cutoff, 60 - nodes.len());
                nodes.extend(extra);
            }
            for node in nodes {
                let sem = sem.clone();
                let client = client_si.clone();
                let kt = ktable_si.clone();
                let tx = triage_tx_si.clone();
                let stats = stats_si.clone();
                let target = ktable::random_node_id();
                tokio::spawn(async move {
                    let _permit = match sem.try_acquire() {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let addr = SocketAddr::V4(node.addr);
                    match client.sample_infohashes(addr, target).await {
                        Ok(result) => {
                            let support = !result.samples.is_empty();
                            let next_at = if result.interval > 0 {
                                Instant::now() + Duration::from_secs(result.interval as u64)
                            } else {
                                Instant::now() + Duration::from_secs(60)
                            };
                            kt.put_node_bep51(result.id, node.addr, support, next_at);

                            for hash in result.samples {
                                stats.infohashes_found.fetch_add(1, Ordering::Relaxed);
                                let _ = tx.try_send(TriageItem {
                                    infohash: hash,
                                    peer: addr,
                                });
                            }
                            for n in result.nodes {
                                kt.put_node(n.id, n.addr, false);
                            }
                        }
                        Err(_) => {
                            kt.drop_node(&node.id);
                        }
                    }
                });
            }
        }
    });

    // Spawn triage -> get_peers -> metadata fetch pipeline
    let client_gp = client.clone();
    let stats_gp = stats.clone();
    let bloom_gp = bloom.clone();
    let db_gp = db.clone();
    let fetcher_gp = fetcher.clone();
    let meta_tx_gp = meta_tx.clone();
    tokio::spawn(async move {
        let meta_sem = Arc::new(Semaphore::new(40 * sf));
        let mut seen_batch = HashSet::new();

        while let Some(item) = triage_rx.recv().await {
            // Dedup within batch
            if !seen_batch.insert(item.infohash) {
                continue;
            }
            if seen_batch.len() > 10000 {
                seen_batch.clear();
            }

            // Bloom filter check
            {
                let mut bloom = bloom_gp.lock().unwrap();
                if bloom.test_and_add(&item.infohash) {
                    continue;
                }
            }

            // DB check
            {
                let rtx = match db_gp.begin_read() {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                if bti_core::storage::has_entry(&rtx, &item.infohash).unwrap_or(false) {
                    continue;
                }
            }

            // Get peers then fetch metadata
            let client = client_gp.clone();
            let fetcher = fetcher_gp.clone();
            let stats = stats_gp.clone();
            let tx = meta_tx_gp.clone();
            let sem = meta_sem.clone();
            let infohash = item.infohash;
            let peer = item.peer;

            tokio::spawn(async move {
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                // Try get_peers first to find actual torrent peers
                let mut peers = Vec::new();
                if let Ok(result) = client.get_peers(peer, infohash).await {
                    for v in result.values {
                        if peers.len() < 10 {
                            peers.push(v);
                        }
                    }
                }
                // Only fall back to DHT node if no real peers found
                if peers.is_empty() {
                    peers.push(peer);
                }
                debug!("fetching {} with {} peers", hex::encode(&infohash[..6]), peers.len());

                stats.meta_requests.fetch_add(1, Ordering::Relaxed);

                // Try each peer
                for p in peers {
                    match fetcher.fetch(infohash, p).await {
                        Ok(result) => {
                            stats.meta_success.fetch_add(1, Ordering::Relaxed);
                            let _ = tx.try_send(MetaResult {
                                infohash,
                                name: result.name,
                                size: result.size,
                            });
                            return;
                        }
                        Err(e) => {
                            trace!("meta fetch {} from {} failed: {}", hex::encode(&infohash[..6]), p, e);
                            continue;
                        }
                    }
                }
                stats.meta_failed.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    // Persist loop — batch writes to redb
    let stats_p = stats.clone();
    let mut batch = Vec::with_capacity(1000);
    let mut flush_interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        tokio::select! {
            Some(result) = meta_rx.recv() => {
                batch.push(result);
                if batch.len() >= 1000 {
                    persist_batch(&db, &mut batch, &stats_p);
                }
            }
            _ = flush_interval.tick() => {
                if !batch.is_empty() {
                    persist_batch(&db, &mut batch, &stats_p);
                }
            }
        }
    }
}

fn persist_batch(db: &Database, batch: &mut Vec<MetaResult>, stats: &CrawlerStats) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let discovered_at = (now.saturating_sub(EPOCH_OFFSET)) as u32;

    match db.begin_write() {
        Ok(wtx) => {
            let mut count = 0u64;
            for item in batch.drain(..) {
                let entry = TorrentEntry {
                    name: item.name,
                    size: item.size,
                    discovered_at,
                };
                match bti_core::storage::put_entry(&wtx, &item.infohash, &entry) {
                    Ok(true) => count += 1,
                    Ok(false) => {}
                    Err(e) => {
                        warn!("persist error: {}", e);
                    }
                }
            }
            if let Err(e) = wtx.commit() {
                error!("commit error: {}", e);
            } else if count > 0 {
                stats.persisted.fetch_add(count, Ordering::Relaxed);
                debug!("persisted {} new entries", count);
            }
        }
        Err(e) => {
            error!("begin_write error: {}", e);
            batch.clear();
        }
    }
}

async fn bootstrap(
    client: &Arc<DhtClient>,
    ktable: &KTable,
    bootstrap_nodes: &[String],
) {
    // Resolve and ping bootstrap nodes
    for node in bootstrap_nodes {
        let addrs: Vec<SocketAddr> = match node.to_socket_addrs() {
            Ok(a) => a.collect(),
            Err(e) => {
                warn!("failed to resolve {}: {}", node, e);
                continue;
            }
        };

        for addr in addrs {
            match client.ping(addr).await {
                Ok(id) => {
                    if let SocketAddr::V4(v4) = addr {
                        ktable.put_node(id, v4, true);
                        info!("bootstrapped from {} (id: {})", addr, hex::encode(&id[..8]));
                    }
                }
                Err(e) => {
                    debug!("bootstrap ping {} failed: {}", addr, e);
                }
            }
        }
    }

    // Run find_node rounds — query closest nodes concurrently, exit early at 50+
    for round in 0..20 {
        let target = ktable::random_node_id();
        let nodes = ktable.get_closest_nodes(&target);
        let mut handles = Vec::new();
        for node in nodes.iter().take(8) {
            let c = Arc::clone(client);
            let addr = SocketAddr::V4(node.addr);
            let t = target;
            handles.push(tokio::spawn(async move {
                c.find_node(addr, t).await
            }));
        }
        for handle in handles {
            if let Ok(Ok(result)) = handle.await {
                for n in result.nodes {
                    ktable.put_node(n.id, n.addr, false);
                }
            }
        }
        if ktable.node_count() > 50 {
            info!("bootstrap early exit at round {} with {} nodes", round + 1, ktable.node_count());
            break;
        }
    }
}
