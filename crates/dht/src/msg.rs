use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use bt_bencode::Value;

pub const Q_PING: &str = "ping";
pub const Q_FIND_NODE: &str = "find_node";
pub const Q_GET_PEERS: &str = "get_peers";
pub const Q_ANNOUNCE_PEER: &str = "announce_peer";
pub const Q_SAMPLE_INFOHASHES: &str = "sample_infohashes";

pub const Y_QUERY: &[u8] = b"q";
pub const Y_RESPONSE: &[u8] = b"r";
pub const Y_ERROR: &[u8] = b"e";

#[derive(Debug)]
pub struct Msg {
    pub y: Vec<u8>,
    pub t: Vec<u8>,
    pub q: Option<String>,
    pub a: Option<MsgArgs>,
    pub r: Option<MsgReturn>,
    pub e: Option<KrpcError>,
}

#[derive(Debug)]
pub struct MsgArgs {
    pub id: [u8; 20],
    pub info_hash: Option<[u8; 20]>,
    pub target: Option<[u8; 20]>,
    pub token: Option<Vec<u8>>,
    pub port: Option<u16>,
    pub implied_port: bool,
}

#[derive(Debug)]
pub struct MsgReturn {
    pub id: [u8; 20],
    pub nodes: Vec<NodeInfo>,
    pub token: Option<Vec<u8>>,
    pub values: Vec<SocketAddr>,
    pub samples: Vec<[u8; 20]>,
    pub num: Option<i64>,
    pub interval: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct KrpcError {
    pub code: i64,
    pub msg: String,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeInfo {
    pub id: [u8; 20],
    pub addr: SocketAddrV4,
}

#[derive(Debug)]
pub struct RecvMsg {
    pub msg: Msg,
    pub from: SocketAddr,
}

impl RecvMsg {
    pub fn announce_port(&self) -> u16 {
        if let Some(ref a) = self.msg.a {
            if a.implied_port {
                return self.from.port();
            }
            if let Some(port) = a.port {
                return port;
            }
        }
        self.from.port()
    }
}

// ── Bencode helpers using str-based indexing ──

fn get_bytes<'a>(val: &'a Value, key: &str) -> Option<&'a [u8]> {
    val.get(key).and_then(|v| v.as_byte_str()).map(|b| b.as_slice())
}

fn get_id(val: &Value, key: &str) -> Option<[u8; 20]> {
    get_bytes(val, key).and_then(|b| b.try_into().ok())
}

fn get_int(val: &Value, key: &str) -> Option<i64> {
    val.get(key).and_then(|v| v.as_i64())
}

fn get_str(val: &Value, key: &str) -> Option<String> {
    get_bytes(val, key).map(|b| String::from_utf8_lossy(b).into_owned())
}

pub fn parse_msg(data: &[u8]) -> Option<Msg> {
    let val: Value = bt_bencode::from_slice(data).ok()?;

    let y = get_bytes(&val, "y")?.to_vec();
    let t = get_bytes(&val, "t")?.to_vec();
    let q = get_str(&val, "q");

    let a = val.get("a").and_then(|a_val| {
        let id = get_id(a_val, "id")?;
        Some(MsgArgs {
            id,
            info_hash: get_id(a_val, "info_hash"),
            target: get_id(a_val, "target"),
            token: get_bytes(a_val, "token").map(|b| b.to_vec()),
            port: get_int(a_val, "port").map(|v| v as u16),
            implied_port: get_int(a_val, "implied_port").unwrap_or(0) != 0,
        })
    });

    let r = val.get("r").and_then(|r_val| {
        let id = get_id(r_val, "id")?;
        let nodes = get_bytes(r_val, "nodes")
            .map(parse_compact_nodes)
            .unwrap_or_default();
        let token = get_bytes(r_val, "token").map(|b| b.to_vec());
        let values = r_val
            .get("values")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_byte_str())
                    .filter_map(|b| parse_node_addr(b.as_slice()))
                    .collect()
            })
            .unwrap_or_default();
        let samples = get_bytes(r_val, "samples")
            .map(parse_compact_infohashes)
            .unwrap_or_default();
        let num = get_int(r_val, "num");
        let interval = get_int(r_val, "interval");

        Some(MsgReturn {
            id,
            nodes,
            token,
            values,
            samples,
            num,
            interval,
        })
    });

    let e = val.get("e").and_then(|e_val| {
        let arr = e_val.as_array()?;
        let code = arr.first()?.as_i64()?;
        let msg = arr
            .get(1)
            .and_then(|v| v.as_byte_str())
            .map(|b| String::from_utf8_lossy(b.as_slice()).into_owned())
            .unwrap_or_default();
        Some(KrpcError { code, msg })
    });

    Some(Msg { y, t, q, a, r, e })
}

fn parse_compact_nodes(data: &[u8]) -> Vec<NodeInfo> {
    data.chunks_exact(26)
        .filter_map(|chunk| {
            let mut id = [0u8; 20];
            id.copy_from_slice(&chunk[..20]);
            let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
            let port = u16::from_be_bytes([chunk[24], chunk[25]]);
            if port == 0 {
                return None;
            }
            Some(NodeInfo {
                id,
                addr: SocketAddrV4::new(ip, port),
            })
        })
        .collect()
}

fn parse_compact_infohashes(data: &[u8]) -> Vec<[u8; 20]> {
    data.chunks_exact(20)
        .map(|chunk| {
            let mut hash = [0u8; 20];
            hash.copy_from_slice(chunk);
            hash
        })
        .collect()
}

fn parse_node_addr(data: &[u8]) -> Option<SocketAddr> {
    if data.len() != 6 {
        return None;
    }
    let ip = Ipv4Addr::new(data[0], data[1], data[2], data[3]);
    let port = u16::from_be_bytes([data[4], data[5]]);
    Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
}

// ── Raw bencode encoding (avoids bt_bencode::Value API issues) ──

/// Encode a query message as raw bencode.
pub fn encode_query(tx_id: &[u8], method: &str, args: &EncodeMsgArgs) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.push(b'd'); // outer dict

    // "a" key
    bencode_bytes(&mut buf, b"a");
    buf.push(b'd'); // args dict
    bencode_bytes(&mut buf, b"id");
    bencode_bytes(&mut buf, &args.id);
    if args.implied_port {
        bencode_bytes(&mut buf, b"implied_port");
        bencode_int(&mut buf, 1);
    }
    if let Some(ref ih) = args.info_hash {
        bencode_bytes(&mut buf, b"info_hash");
        bencode_bytes(&mut buf, ih);
    }
    if let Some(port) = args.port {
        bencode_bytes(&mut buf, b"port");
        bencode_int(&mut buf, port as i64);
    }
    if let Some(ref t) = args.target {
        bencode_bytes(&mut buf, b"target");
        bencode_bytes(&mut buf, t);
    }
    if let Some(ref token) = args.token {
        bencode_bytes(&mut buf, b"token");
        bencode_bytes(&mut buf, token);
    }
    buf.push(b'e'); // end args dict

    // "q" key
    bencode_bytes(&mut buf, b"q");
    bencode_bytes(&mut buf, method.as_bytes());

    // "t" key
    bencode_bytes(&mut buf, b"t");
    bencode_bytes(&mut buf, tx_id);

    // "y" key
    bencode_bytes(&mut buf, b"y");
    bencode_bytes(&mut buf, b"q");

    buf.push(b'e'); // end outer dict
    buf
}

/// Encode a response message as raw bencode.
pub fn encode_response(tx_id: &[u8], ret: &EncodeReturn) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);
    buf.push(b'd');

    // "r" key
    bencode_bytes(&mut buf, b"r");
    buf.push(b'd');

    bencode_bytes(&mut buf, b"id");
    bencode_bytes(&mut buf, &ret.id);

    if let Some(interval) = ret.interval {
        bencode_bytes(&mut buf, b"interval");
        bencode_int(&mut buf, interval);
    }

    if !ret.nodes.is_empty() {
        bencode_bytes(&mut buf, b"nodes");
        let mut nodes_buf = Vec::with_capacity(ret.nodes.len() * 26);
        for n in &ret.nodes {
            nodes_buf.extend_from_slice(&n.id);
            nodes_buf.extend_from_slice(&n.addr.ip().octets());
            nodes_buf.extend_from_slice(&n.addr.port().to_be_bytes());
        }
        bencode_bytes(&mut buf, &nodes_buf);
    }

    if let Some(num) = ret.num {
        bencode_bytes(&mut buf, b"num");
        bencode_int(&mut buf, num);
    }

    if !ret.samples.is_empty() {
        bencode_bytes(&mut buf, b"samples");
        let mut samples_buf = Vec::with_capacity(ret.samples.len() * 20);
        for s in &ret.samples {
            samples_buf.extend_from_slice(s);
        }
        bencode_bytes(&mut buf, &samples_buf);
    }

    if let Some(ref token) = ret.token {
        bencode_bytes(&mut buf, b"token");
        bencode_bytes(&mut buf, token);
    }

    if !ret.values.is_empty() {
        bencode_bytes(&mut buf, b"values");
        buf.push(b'l');
        for addr in &ret.values {
            if let SocketAddr::V4(v4) = addr {
                let mut peer_buf = [0u8; 6];
                peer_buf[..4].copy_from_slice(&v4.ip().octets());
                peer_buf[4..6].copy_from_slice(&v4.port().to_be_bytes());
                bencode_bytes(&mut buf, &peer_buf);
            }
        }
        buf.push(b'e');
    }

    buf.push(b'e'); // end r dict

    // "t" key
    bencode_bytes(&mut buf, b"t");
    bencode_bytes(&mut buf, tx_id);

    // "y" key
    bencode_bytes(&mut buf, b"y");
    bencode_bytes(&mut buf, b"r");

    buf.push(b'e');
    buf
}

/// Encode an error response as raw bencode.
pub fn encode_error(tx_id: &[u8], code: i64, msg: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    buf.push(b'd');

    // "e" key
    bencode_bytes(&mut buf, b"e");
    buf.push(b'l');
    bencode_int(&mut buf, code);
    bencode_bytes(&mut buf, msg.as_bytes());
    buf.push(b'e');

    // "t" key
    bencode_bytes(&mut buf, b"t");
    bencode_bytes(&mut buf, tx_id);

    // "y" key
    bencode_bytes(&mut buf, b"y");
    bencode_bytes(&mut buf, b"e");

    buf.push(b'e');
    buf
}

fn bencode_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(data.len().to_string().as_bytes());
    buf.push(b':');
    buf.extend_from_slice(data);
}

fn bencode_int(buf: &mut Vec<u8>, val: i64) {
    buf.push(b'i');
    buf.extend_from_slice(val.to_string().as_bytes());
    buf.push(b'e');
}

pub struct EncodeMsgArgs {
    pub id: [u8; 20],
    pub info_hash: Option<[u8; 20]>,
    pub target: Option<[u8; 20]>,
    pub token: Option<Vec<u8>>,
    pub port: Option<u16>,
    pub implied_port: bool,
}

pub struct EncodeReturn {
    pub id: [u8; 20],
    pub nodes: Vec<NodeInfo>,
    pub token: Option<Vec<u8>>,
    pub values: Vec<SocketAddr>,
    pub samples: Vec<[u8; 20]>,
    pub num: Option<i64>,
    pub interval: Option<i64>,
}

impl Default for EncodeReturn {
    fn default() -> Self {
        Self {
            id: [0; 20],
            nodes: Vec::new(),
            token: None,
            values: Vec::new(),
            samples: Vec::new(),
            num: None,
            interval: None,
        }
    }
}
