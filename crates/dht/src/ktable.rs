use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::sync::RwLock;
use std::time::Instant;

use rand::Rng;

const K: usize = 80;

#[derive(Clone)]
struct KNode {
    id: [u8; 20],
    addr: SocketAddrV4,
    last_responded: Instant,
    discovered_at: Instant,
    dropped: bool,
    bep51_support: bool,
    bep51_next_at: Instant,
}

struct KHash {
    id: [u8; 20],
    peers: Vec<SocketAddr>,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: [u8; 20],
    pub addr: SocketAddrV4,
    pub last_responded: Instant,
    pub dropped: bool,
    pub bep51_support: bool,
    pub bep51_next_at: Instant,
}

struct Inner {
    origin: [u8; 20],
    nodes: Vec<KNode>,
    hashes: Vec<KHash>,
}

pub struct KTable {
    inner: RwLock<Inner>,
}

pub struct GetHashResult {
    pub found: bool,
    pub peers: Vec<SocketAddr>,
    pub closest_nodes: Vec<Node>,
}

pub struct SampleResult {
    pub hashes: Vec<[u8; 20]>,
    pub nodes: Vec<Node>,
    pub total_hashes: usize,
}

impl KTable {
    pub fn new(origin: [u8; 20]) -> Self {
        Self {
            inner: RwLock::new(Inner {
                origin,
                nodes: Vec::with_capacity(K),
                hashes: Vec::new(),
            }),
        }
    }

    pub fn origin(&self) -> [u8; 20] {
        self.inner.read().unwrap().origin
    }

    pub fn put_node(&self, id: [u8; 20], addr: SocketAddrV4, responded: bool) {
        let mut inner = self.inner.write().unwrap();
        Self::put_node_locked(&mut inner, id, addr, responded, None, None);
    }

    pub fn put_node_bep51(
        &self,
        id: [u8; 20],
        addr: SocketAddrV4,
        support: bool,
        next_at: Instant,
    ) {
        let mut inner = self.inner.write().unwrap();
        Self::put_node_locked(
            &mut inner,
            id,
            addr,
            true,
            Some(support),
            Some(next_at),
        );
    }

    fn put_node_locked(
        inner: &mut Inner,
        id: [u8; 20],
        addr: SocketAddrV4,
        responded: bool,
        bep51_support: Option<bool>,
        bep51_next_at: Option<Instant>,
    ) {
        let now = Instant::now();

        // Check for existing node by ID or address
        for node in inner.nodes.iter_mut() {
            if node.id == id || node.addr == addr {
                node.id = id;
                node.addr = addr;
                node.dropped = false;
                if responded {
                    node.last_responded = now;
                }
                if let Some(s) = bep51_support {
                    node.bep51_support = s;
                }
                if let Some(t) = bep51_next_at {
                    node.bep51_next_at = t;
                }
                return;
            }
        }

        // New node
        let new_node = KNode {
            id,
            addr,
            last_responded: if responded { now } else { Instant::now() - std::time::Duration::from_secs(3600) },
            discovered_at: now,
            dropped: false,
            bep51_support: bep51_support.unwrap_or(false),
            bep51_next_at: bep51_next_at.unwrap_or(now),
        };

        if inner.nodes.len() < K {
            inner.nodes.push(new_node);
        } else {
            // Evict furthest non-dropped node if new node is closer
            let new_dist = xor_distance(&inner.origin, &id);
            let mut worst_idx = None;
            let mut worst_dist = new_dist;

            for (i, node) in inner.nodes.iter().enumerate() {
                let d = xor_distance(&inner.origin, &node.id);
                if compare_dist(&d, &worst_dist) > 0 {
                    worst_dist = d;
                    worst_idx = Some(i);
                }
            }

            if let Some(idx) = worst_idx {
                inner.nodes[idx] = new_node;
            }
        }
    }

    pub fn drop_node(&self, id: &[u8; 20]) {
        let mut inner = self.inner.write().unwrap();
        inner.nodes.retain(|n| n.id != *id);
    }

    pub fn drop_addr(&self, addr: IpAddr) {
        let mut inner = self.inner.write().unwrap();
        inner.nodes.retain(|n| IpAddr::V4(*n.addr.ip()) != addr);
    }

    pub fn put_hash(&self, id: [u8; 20], mut peers: Vec<SocketAddr>) {
        // Cap peers per hash to prevent unbounded growth
        peers.truncate(20);
        let mut inner = self.inner.write().unwrap();
        for h in inner.hashes.iter_mut() {
            if h.id == id {
                h.peers = peers;
                return;
            }
        }
        if inner.hashes.len() < K {
            inner.hashes.push(KHash { id, peers });
        }
    }

    pub fn get_closest_nodes(&self, target: &[u8; 20]) -> Vec<Node> {
        let inner = self.inner.read().unwrap();
        let mut live: Vec<&KNode> = inner.nodes.iter().filter(|n| !n.dropped).collect();
        live.sort_by(|a, b| {
            let da = xor_distance(target, &a.id);
            let db = xor_distance(target, &b.id);
            compare_dist(&da, &db).cmp(&0)
        });
        live.iter()
            .take(K)
            .map(|n| node_from_knode(n))
            .collect()
    }

    pub fn get_oldest_nodes(&self, cutoff: Instant, n: usize) -> Vec<Node> {
        let inner = self.inner.read().unwrap();
        inner
            .nodes
            .iter()
            .filter(|node| !node.dropped && node.last_responded < cutoff)
            .take(n)
            .map(node_from_knode)
            .collect()
    }

    pub fn get_nodes_for_sample_infohashes(&self, n: usize) -> Vec<Node> {
        let now = Instant::now();
        let inner = self.inner.read().unwrap();
        inner
            .nodes
            .iter()
            .filter(|node| !node.dropped && node.bep51_support && now >= node.bep51_next_at)
            .take(n)
            .map(node_from_knode)
            .collect()
    }

    pub fn get_hash_or_closest_nodes(&self, id: &[u8; 20]) -> GetHashResult {
        let inner = self.inner.read().unwrap();
        for h in &inner.hashes {
            if h.id == *id {
                return GetHashResult {
                    found: true,
                    peers: h.peers.clone(),
                    closest_nodes: Vec::new(),
                };
            }
        }
        drop(inner);
        GetHashResult {
            found: false,
            peers: Vec::new(),
            closest_nodes: self.get_closest_nodes(id),
        }
    }

    pub fn sample_hashes_and_nodes(&self) -> SampleResult {
        let inner = self.inner.read().unwrap();
        let mut rng = rand::thread_rng();

        let total = inner.hashes.len();
        let sample_count = total.min(8);
        let hashes: Vec<[u8; 20]> = random_indices(&mut rng, total, sample_count)
            .iter()
            .map(|&i| inner.hashes[i].id)
            .collect();

        let live: Vec<&KNode> = inner.nodes.iter().filter(|n| !n.dropped).collect();
        let node_count = live.len().min(20);
        let nodes: Vec<Node> = random_indices(&mut rng, live.len(), node_count)
            .iter()
            .map(|&i| node_from_knode(live[i]))
            .collect();

        SampleResult {
            hashes,
            nodes,
            total_hashes: total,
        }
    }

    pub fn node_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.nodes.iter().filter(|n| !n.dropped).count()
    }

    pub fn filter_known_addrs(&self, addrs: &[SocketAddrV4]) -> Vec<SocketAddrV4> {
        let inner = self.inner.read().unwrap();
        addrs
            .iter()
            .filter(|addr| !inner.nodes.iter().any(|n| &n.addr == *addr))
            .copied()
            .collect()
    }
}

fn node_from_knode(n: &KNode) -> Node {
    Node {
        id: n.id,
        addr: n.addr,
        last_responded: n.last_responded,
        dropped: n.dropped,
        bep51_support: n.bep51_support,
        bep51_next_at: n.bep51_next_at,
    }
}

pub fn xor_distance(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = a[i] ^ b[i];
    }
    result
}

fn compare_dist(a: &[u8; 20], b: &[u8; 20]) -> i32 {
    for i in 0..20 {
        if a[i] < b[i] {
            return -1;
        }
        if a[i] > b[i] {
            return 1;
        }
    }
    0
}

fn random_indices(rng: &mut impl Rng, total: usize, n: usize) -> Vec<usize> {
    if n >= total {
        return (0..total).collect();
    }
    let mut indices = Vec::with_capacity(n);
    let mut used = std::collections::HashSet::new();
    while indices.len() < n {
        let i = rng.gen_range(0..total);
        if used.insert(i) {
            indices.push(i);
        }
    }
    indices
}

pub fn random_node_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    rand::thread_rng().fill(&mut id);
    id
}
