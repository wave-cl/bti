use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs};
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

#[derive(Clone)]
struct DiscoveredNode {
    addr: SocketAddrV4,
}

struct PeerItem {
    infohash: [u8; 20],
    peers: Vec<SocketAddr>,
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
    raise_fd_limit();

    let node_id = ktable::random_node_id();
    let ktable = Arc::new(KTable::new(node_id));
    let stats = Arc::new(CrawlerStats::new());
    let bloom = Arc::new(Mutex::new(StableBloomFilter::new(10_000_000, 0.001)));
    let sf = config.scaling_factor;

    // Rotating sought node ID (bt-mcp: rotates every 10s)
    let sought_id = Arc::new(Mutex::new(ktable::random_node_id()));
    let sought_id_rotate = sought_id.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            *sought_id_rotate.lock().unwrap() = ktable::random_node_id();
        }
    });

    // Channels — matching bt-mcp sizing
    let (discovered_tx, mut discovered_rx) = mpsc::channel::<DiscoveredNode>(100 * sf);
    let (find_node_tx, mut find_node_rx) = mpsc::channel::<DiscoveredNode>(10 * sf);
    let (sample_tx, mut sample_rx) = mpsc::channel::<DiscoveredNode>(10 * sf);
    let (triage_tx, mut triage_rx) = mpsc::channel::<TriageItem>(10 * sf);
    let (peers_tx, mut peers_rx) = mpsc::channel::<PeerItem>(10 * sf);
    let (meta_tx, mut meta_rx) = mpsc::channel::<MetaResult>(1000);

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
                let _ = disc_tx.try_send(DiscoveredNode { addr: v4 });
            }
        });

    let server =
        Server::start(config.dht_port, responder, Some(on_node_discovered)).await?;
    info!("DHT server started on {}", server.local_addr());

    let client = Arc::new(DhtClient::new(node_id, server.clone()));
    let fetcher = Arc::new(MetadataFetcher::new(Duration::from_secs(6)));

    // Bootstrap
    info!("bootstrapping DHT...");
    bootstrap(&client, &ktable, &config.bootstrap_nodes).await;
    info!("bootstrap complete, {} nodes in routing table", ktable.node_count());

    // --- Stats logger (every 30s) ---
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

    // --- Bootstrap reseeding (bt-mcp: every 60s) ---
    let client_reseed = client.clone();
    let ktable_reseed = ktable.clone();
    let bootstrap_nodes = config.bootstrap_nodes.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await; // skip immediate
        loop {
            interval.tick().await;
            for node in &bootstrap_nodes {
                let addrs: Vec<SocketAddr> = match node.to_socket_addrs() {
                    Ok(a) => a.collect(),
                    Err(_) => continue,
                };
                for addr in addrs {
                    if let Ok(id) = client_reseed.ping(addr).await {
                        if let SocketAddr::V4(v4) = addr {
                            ktable_reseed.put_node(id, v4, true);
                        }
                    }
                }
            }
            debug!("reseeded bootstrap nodes");
        }
    });

    // --- Old node pruning (bt-mcp: every 10s, 15min cutoff) ---
    // Bounded: max 20 nodes per tick, acquire before spawn
    let client_prune = client.clone();
    let ktable_prune = ktable.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(sf));
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let cutoff = Instant::now() - Duration::from_secs(900);
            let old_nodes = ktable_prune.get_oldest_nodes(cutoff, 20);
            for node in old_nodes {
                let permit = match sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return,
                };
                let client = client_prune.clone();
                let kt = ktable_prune.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    match client.ping(SocketAddr::V4(node.addr)).await {
                        Ok(id) => {
                            kt.put_node(id, node.addr, true);
                        }
                        Err(_) => {
                            kt.drop_node(&node.id);
                        }
                    }
                });
            }
        }
    });

    // --- Discovered nodes router (bt-mcp: routes to find_node OR sample_infohashes) ---
    let find_node_tx_dn = find_node_tx.clone();
    let sample_tx_dn = sample_tx.clone();
    tokio::spawn(async move {
        let mut seen_ips: HashMap<Ipv4Addr, Instant> = HashMap::new();

        while let Some(node) = discovered_rx.recv().await {
            // Time-based dedup: skip if seen in last 60s
            let now = Instant::now();
            if let Some(&last) = seen_ips.get(node.addr.ip()) {
                if now.duration_since(last) < Duration::from_secs(60) {
                    continue;
                }
            }
            seen_ips.insert(*node.addr.ip(), now);

            // Periodic cleanup
            if seen_ips.len() > 100_000 {
                seen_ips.retain(|_, ts| now.duration_since(*ts) < Duration::from_secs(60));
            }

            // Route to whichever stage has capacity (unbiased)
            let n = node.clone();
            tokio::select! {
                r = find_node_tx_dn.send(n) => { let _ = r; },
                r = sample_tx_dn.send(node) => { let _ = r; },
            }
        }
    });

    // --- find_node handler — acquire permit BEFORE spawn (bounded tasks) ---
    let client_fn = client.clone();
    let ktable_fn = ktable.clone();
    let disc_tx_fn = discovered_tx.clone();
    let sought_id_fn = sought_id.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(10 * sf));
        while let Some(node) = find_node_rx.recv().await {
            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let client = client_fn.clone();
            let kt = ktable_fn.clone();
            let disc_tx = disc_tx_fn.clone();
            let target = *sought_id_fn.lock().unwrap();
            tokio::spawn(async move {
                let _permit = permit;
                match client.find_node(SocketAddr::V4(node.addr), target).await {
                    Ok(result) => {
                        for n in result.nodes {
                            kt.put_node(n.id, n.addr, false);
                            let _ = disc_tx.try_send(DiscoveredNode { addr: n.addr });
                        }
                    }
                    Err(_) => {
                        kt.drop_addr(std::net::IpAddr::V4(*node.addr.ip()));
                    }
                }
            });
        }
    });

    // --- find_node periodic feeder (bt-mcp: 5s cutoff) ---
    let ktable_fnf = ktable.clone();
    let find_node_tx_fnf = find_node_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let cutoff = Instant::now() - Duration::from_secs(5);
            let nodes = ktable_fnf.get_oldest_nodes(cutoff, 10);
            for node in nodes {
                let _ = find_node_tx_fnf.try_send(DiscoveredNode { addr: node.addr });
            }
        }
    });

    // --- sample_infohashes handler — acquire permit BEFORE spawn (bounded tasks) ---
    let client_si = client.clone();
    let ktable_si = ktable.clone();
    let triage_tx_si = triage_tx.clone();
    let stats_si = stats.clone();
    let disc_tx_si = discovered_tx.clone();
    let sought_id_si = sought_id.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(10 * sf));
        while let Some(node) = sample_rx.recv().await {
            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let client = client_si.clone();
            let kt = ktable_si.clone();
            let tx = triage_tx_si.clone();
            let stats = stats_si.clone();
            let disc_tx = disc_tx_si.clone();
            let target = *sought_id_si.lock().unwrap();
            tokio::spawn(async move {
                let _permit = permit;
                let addr = SocketAddr::V4(node.addr);
                match client.sample_infohashes(addr, target).await {
                    Ok(result) => {
                        let support = !result.samples.is_empty();

                        // Interval capping (bt-mcp: cap to 60 when samples found, min 10)
                        let mut interval_secs = if result.interval > 0 {
                            result.interval as u64
                        } else {
                            60
                        };
                        if support && interval_secs > 60 {
                            interval_secs = 60;
                        }
                        if !support {
                            interval_secs = 300; // 5-min backoff for non-BEP51
                        }
                        if interval_secs < 10 {
                            interval_secs = 10;
                        }

                        let next_at = Instant::now() + Duration::from_secs(interval_secs);
                        kt.put_node_bep51(result.id, node.addr, support, next_at);

                        for hash in result.samples {
                            stats.infohashes_found.fetch_add(1, Ordering::Relaxed);
                            let _ = tx.send(TriageItem {
                                infohash: hash,
                                peer: addr,
                            }).await;
                        }
                        for n in result.nodes {
                            kt.put_node(n.id, n.addr, false);
                            let _ = disc_tx.try_send(DiscoveredNode { addr: n.addr });
                        }
                    }
                    Err(_) => {
                        kt.drop_addr(std::net::IpAddr::V4(*node.addr.ip()));
                    }
                }
            });
        }
    });

    // --- sample_infohashes periodic feeder (confirmed BEP 51 nodes + untested nodes) ---
    let ktable_sif = ktable.clone();
    let sample_tx_sif = sample_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut nodes = ktable_sif.get_nodes_for_sample_infohashes(60);
            if nodes.len() < 60 {
                let cutoff = Instant::now() - Duration::from_secs(300);
                let extra = ktable_sif.get_oldest_nodes(cutoff, 60 - nodes.len());
                nodes.extend(extra);
            }
            for node in nodes {
                let _ = sample_tx_sif.try_send(DiscoveredNode { addr: node.addr });
            }
        }
    });

    // --- get_peers stage — acquire permit BEFORE spawn (bounded tasks) ---
    let client_gp = client.clone();
    let stats_gp = stats.clone();
    let bloom_gp = bloom.clone();
    let db_gp = db.clone();
    let peers_tx_gp = peers_tx.clone();
    let disc_tx_gp = discovered_tx.clone();
    tokio::spawn(async move {
        let gp_sem = Arc::new(Semaphore::new(20 * sf));
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

            let permit = match gp_sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let client = client_gp.clone();
            let tx = peers_tx_gp.clone();
            let disc_tx = disc_tx_gp.clone();
            let infohash = item.infohash;
            let peer = item.peer;

            tokio::spawn(async move {
                let _permit = permit;

                let mut peers = Vec::new();
                match client.get_peers(peer, infohash).await {
                    Ok(result) => {
                        for v in result.values {
                            if peers.len() < 10 {
                                peers.push(v);
                            }
                        }
                        // Feed discovered nodes back
                        for n in result.nodes {
                            let _ = disc_tx.try_send(DiscoveredNode { addr: n.addr });
                        }
                    }
                    Err(_) => {}
                }
                if peers.is_empty() {
                    peers.push(peer);
                }
                let _ = tx.send(PeerItem { infohash, peers }).await;
            });
        }
    });

    // --- requestMetaInfo stage — acquire permit BEFORE spawn (bounded tasks) ---
    let stats_meta = stats.clone();
    let fetcher_meta = fetcher.clone();
    let meta_tx_meta = meta_tx.clone();
    tokio::spawn(async move {
        let meta_sem = Arc::new(Semaphore::new(40 * sf));

        while let Some(item) = peers_rx.recv().await {
            let permit = match meta_sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let fetcher = fetcher_meta.clone();
            let stats = stats_meta.clone();
            let tx = meta_tx_meta.clone();

            tokio::spawn(async move {
                let _permit = permit;

                stats.meta_requests.fetch_add(1, Ordering::Relaxed);

                for p in item.peers {
                    match fetcher.fetch(item.infohash, p).await {
                        Ok(result) => {
                            stats.meta_success.fetch_add(1, Ordering::Relaxed);
                            let _ = tx.send(MetaResult {
                                infohash: item.infohash,
                                name: result.name,
                                size: result.size,
                            }).await;
                            return;
                        }
                        Err(e) => {
                            trace!("meta fetch {} from {} failed: {}", hex::encode(&item.infohash[..6]), p, e);
                            continue;
                        }
                    }
                }
                stats.meta_failed.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    // --- Persist loop — batch writes to redb ---
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

    // Run find_node rounds
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

fn raise_fd_limit() {
    #[cfg(unix)]
    {
        use std::io;
        let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        unsafe {
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
                let target = rlim.rlim_max.min(65536);
                if rlim.rlim_cur < target {
                    rlim.rlim_cur = target;
                    if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) == 0 {
                        info!("raised fd limit to {}", target);
                    } else {
                        warn!("failed to raise fd limit: {}", io::Error::last_os_error());
                    }
                }
            }
        }
    }
}
