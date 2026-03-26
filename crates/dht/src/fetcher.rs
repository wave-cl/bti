use std::net::SocketAddr;
use std::time::Duration;

use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::trace;

const MAX_METADATA_SIZE: usize = 10 * 1024 * 1024;
const PIECE_SIZE: usize = 16 * 1024;
const BT_PROTOCOL: &[u8] = b"\x13BitTorrent protocol";
// byte[5]=0x10: LTEP (BEP 10), byte[7]=0x01: DHT (BEP 5)
const EXTENSION_BITS: [u8; 8] = [0, 0, 0, 0, 0, 0x10, 0, 0x01];

#[derive(Debug)]
pub struct MetainfoResult {
    pub name: String,
    pub size: u64,
}

pub struct MetadataFetcher {
    timeout: Duration,
}

impl MetadataFetcher {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    pub async fn fetch(
        &self,
        info_hash: [u8; 20],
        addr: SocketAddr,
    ) -> Result<MetainfoResult, FetchError> {
        let result = tokio::time::timeout(self.timeout, self.fetch_inner(info_hash, addr)).await;
        match result {
            Ok(r) => r,
            Err(_) => Err(FetchError::Timeout),
        }
    }

    async fn fetch_inner(
        &self,
        info_hash: [u8; 20],
        addr: SocketAddr,
    ) -> Result<MetainfoResult, FetchError> {
        let connect_timeout = Duration::from_secs(3);
        let stream = tokio::time::timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| FetchError::Timeout)?
            .map_err(FetchError::Io)?;

        // Disable Nagle, set linger to 0
        stream.set_nodelay(true).ok();

        let mut stream = stream;

        // BitTorrent handshake
        bt_handshake(&mut stream, &info_hash).await?;

        // Extension handshake
        let (metadata_size, ut_metadata_id) = ex_handshake(&mut stream).await?;

        if metadata_size == 0 || metadata_size > MAX_METADATA_SIZE as u32 {
            return Err(FetchError::Protocol("invalid metadata size".into()));
        }

        // Request all pieces
        request_all_pieces(&mut stream, metadata_size, ut_metadata_id).await?;

        // Read all pieces
        let metadata = read_all_pieces(&mut stream, metadata_size).await?;

        // Verify SHA1
        let hash: [u8; 20] = Sha1::digest(&metadata).into();
        if hash != info_hash {
            return Err(FetchError::Protocol("infohash mismatch".into()));
        }

        // Parse info dict
        parse_meta_info(&metadata)
    }
}

async fn bt_handshake(
    stream: &mut TcpStream,
    info_hash: &[u8; 20],
) -> Result<(), FetchError> {
    let peer_id = *b"-BT0000-bti000000000";

    // Send handshake: protocol + extension bits + infohash + peer_id
    let mut handshake = Vec::with_capacity(68);
    handshake.extend_from_slice(BT_PROTOCOL);
    handshake.extend_from_slice(&EXTENSION_BITS);
    handshake.extend_from_slice(info_hash);
    handshake.extend_from_slice(&peer_id);

    stream.write_all(&handshake).await.map_err(FetchError::Io)?;

    // Read response (68 bytes)
    let mut resp = [0u8; 68];
    stream
        .read_exact(&mut resp)
        .await
        .map_err(FetchError::Io)?;

    // Verify protocol string
    if &resp[..20] != BT_PROTOCOL {
        return Err(FetchError::Protocol("bad protocol string".into()));
    }

    // Check LTEP support (byte 25, bit 4 = 0x10)
    if resp[25] & 0x10 == 0 {
        return Err(FetchError::Protocol("no LTEP support".into()));
    }

    // Verify infohash
    let mut resp_hash = [0u8; 20];
    resp_hash.copy_from_slice(&resp[28..48]);
    if resp_hash != *info_hash {
        return Err(FetchError::Protocol("infohash mismatch in handshake".into()));
    }

    Ok(())
}

async fn ex_handshake(stream: &mut TcpStream) -> Result<(u32, u8), FetchError> {
    // Send extension handshake: d1:md11:ut_metadatai1eee
    let payload = b"d1:md11:ut_metadatai1eee";
    let msg_len = (2 + payload.len()) as u32;

    let mut msg = Vec::with_capacity(4 + msg_len as usize);
    msg.extend_from_slice(&msg_len.to_be_bytes());
    msg.push(20); // Extended message type
    msg.push(0); // Handshake (extension ID 0)
    msg.extend_from_slice(payload);

    stream.write_all(&msg).await.map_err(FetchError::Io)?;

    // Read extension handshake response
    let resp = read_ex_message(stream).await?;

    // Parse bencode response (skip first 2 bytes: msg_type + ext_id)
    let payload = &resp[2..];
    let val: bt_bencode::Value =
        bt_bencode::from_slice(payload).map_err(|_| FetchError::Protocol("bad bencode".into()))?;

    let m = val
        .get("m")
        .ok_or_else(|| FetchError::Protocol("missing m dict".into()))?;
    let ut_metadata = m
        .get("ut_metadata")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| FetchError::Protocol("missing ut_metadata".into()))?;

    let metadata_size = val
        .get("metadata_size")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| FetchError::Protocol("missing metadata_size".into()))?;

    if ut_metadata < 1 || ut_metadata > 254 {
        return Err(FetchError::Protocol("invalid ut_metadata id".into()));
    }

    Ok((metadata_size as u32, ut_metadata as u8))
}

async fn request_all_pieces(
    stream: &mut TcpStream,
    metadata_size: u32,
    ut_metadata: u8,
) -> Result<(), FetchError> {
    let pieces = (metadata_size as usize + PIECE_SIZE - 1) / PIECE_SIZE;

    for i in 0..pieces {
        let payload = format!("d8:msg_typei0e5:piecei{}ee", i);
        let payload = payload.as_bytes();
        let msg_len = (2 + payload.len()) as u32;

        let mut msg = Vec::with_capacity(4 + msg_len as usize);
        msg.extend_from_slice(&msg_len.to_be_bytes());
        msg.push(20);
        msg.push(ut_metadata);
        msg.extend_from_slice(payload);

        stream.write_all(&msg).await.map_err(FetchError::Io)?;
    }

    Ok(())
}

async fn read_all_pieces(
    stream: &mut TcpStream,
    metadata_size: u32,
) -> Result<Vec<u8>, FetchError> {
    let mut metadata = vec![0u8; metadata_size as usize];
    let mut received = 0usize;

    while received < metadata_size as usize {
        let msg_data = read_um_message(stream).await?;

        // Parse bencode dict from the message (after ext header bytes)
        let payload = &msg_data[2..]; // skip msg_type byte + ext_id byte

        // Find where bencode dict ends and raw piece data begins
        // The dict is like d8:msg_typei1e5:piecei0ee followed by raw data
        let (dict_end, piece_dict) = parse_piece_dict(payload)?;

        if piece_dict.msg_type == 2 {
            return Err(FetchError::Protocol("piece rejected".into()));
        }
        if piece_dict.msg_type != 1 {
            continue;
        }

        let piece_data = &payload[dict_end..];
        let offset = piece_dict.piece as usize * PIECE_SIZE;

        if offset + piece_data.len() > metadata_size as usize {
            return Err(FetchError::Protocol("piece overflows metadata".into()));
        }

        metadata[offset..offset + piece_data.len()].copy_from_slice(piece_data);
        received += piece_data.len();

        trace!("received piece {} ({} bytes, total {}/{})",
            piece_dict.piece, piece_data.len(), received, metadata_size);
    }

    Ok(metadata)
}

struct PieceDict {
    msg_type: i32,
    piece: i32,
}

fn parse_piece_dict(data: &[u8]) -> Result<(usize, PieceDict), FetchError> {
    // Find the exact end of the bencode dict by scanning the raw bytes.
    // We can't re-encode because key ordering may differ.
    let dict_end = bencode_end(data)
        .ok_or_else(|| FetchError::Protocol("malformed piece bencode".into()))?;

    let dict_bytes = &data[..dict_end];
    let val: bt_bencode::Value =
        bt_bencode::from_slice(dict_bytes).map_err(|_| FetchError::Protocol("bad piece bencode".into()))?;

    let msg_type = val
        .get("msg_type")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1) as i32;
    let piece = val
        .get("piece")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;

    Ok((dict_end, PieceDict { msg_type, piece }))
}

/// Find the byte length of a single bencode value starting at data[0].
/// Returns the index one past the end of the value.
fn bencode_end(data: &[u8]) -> Option<usize> {
    if data.is_empty() {
        return None;
    }
    let mut i = 0;
    match data[0] {
        b'd' | b'l' => {
            // Dict or list: starts with 'd'/'l', contains values, ends with 'e'
            i += 1;
            while i < data.len() && data[i] != b'e' {
                i = bencode_end_at(data, i)?;
                if data[0] == b'd' {
                    // Dicts have key-value pairs, so consume the value too
                    i = bencode_end_at(data, i)?;
                }
            }
            if i < data.len() && data[i] == b'e' {
                Some(i + 1)
            } else {
                None
            }
        }
        b'i' => {
            // Integer: i<digits>e
            i += 1;
            while i < data.len() && data[i] != b'e' {
                i += 1;
            }
            if i < data.len() {
                Some(i + 1)
            } else {
                None
            }
        }
        b'0'..=b'9' => {
            // String: <length>:<data>
            bencode_string_end(data, 0)
        }
        _ => None,
    }
}

fn bencode_end_at(data: &[u8], offset: usize) -> Option<usize> {
    bencode_end(&data[offset..]).map(|len| offset + len)
}

fn bencode_string_end(data: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i < data.len() && data[i].is_ascii_digit() {
        i += 1;
    }
    if i >= data.len() || data[i] != b':' {
        return None;
    }
    let len_str = std::str::from_utf8(&data[start..i]).ok()?;
    let len: usize = len_str.parse().ok()?;
    i += 1; // skip ':'
    if i + len > data.len() {
        return None;
    }
    Some(i + len)
}

async fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>, FetchError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(FetchError::Io)?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_METADATA_SIZE {
        return Err(FetchError::Protocol("message too large".into()));
    }
    if len == 0 {
        // Keep-alive
        return Ok(Vec::new());
    }

    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(FetchError::Io)?;

    Ok(buf)
}

async fn read_ex_message(stream: &mut TcpStream) -> Result<Vec<u8>, FetchError> {
    loop {
        let msg = read_message(stream).await?;
        if msg.is_empty() {
            continue;
        }
        if msg[0] == 20 {
            return Ok(msg);
        }
        // Not an extended message, skip
    }
}

async fn read_um_message(stream: &mut TcpStream) -> Result<Vec<u8>, FetchError> {
    loop {
        let msg = read_ex_message(stream).await?;
        if msg.len() >= 2 && msg[1] == 1 {
            // ut_metadata response (ext_id=1 since we requested it as 1)
            return Ok(msg);
        }
    }
}

fn parse_meta_info(data: &[u8]) -> Result<MetainfoResult, FetchError> {
    let val: bt_bencode::Value =
        bt_bencode::from_slice(data).map_err(|_| FetchError::Protocol("bad info dict".into()))?;

    let name = val
        .get("name")
        .and_then(|v| v.as_byte_str())
        .map(|b| String::from_utf8_lossy(b.as_slice()).into_owned())
        .unwrap_or_default();

    // Single-file torrent
    if let Some(length) = val.get("length").and_then(|v| v.as_i64()) {
        return Ok(MetainfoResult {
            name,
            size: length as u64,
        });
    }

    // Multi-file torrent
    let mut total_size: u64 = 0;
    if let Some(files) = val.get("files").and_then(|v| v.as_array()) {
        for f in files {
            if let Some(length) = f.get("length").and_then(|v| v.as_i64()) {
                total_size += length as u64;
            }
        }
    }

    Ok(MetainfoResult {
        name,
        size: total_size,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("timeout")]
    Timeout,
    #[error("io: {0}")]
    Io(std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
}
