use quinn::{RecvStream, SendStream};

use crate::model::{decode_u48, encode_u48, InfoHash, TorrentEntry};
use crate::Error;

/// Write sync request: 4 bytes u32 BE timestamp.
pub async fn write_sync_request(send: &mut SendStream, since: u32) -> Result<(), Error> {
    send.write_all(&since.to_be_bytes()).await?;
    Ok(())
}

/// Read sync request: 4 bytes u32 BE timestamp.
pub async fn read_sync_request(recv: &mut RecvStream) -> Result<u32, Error> {
    let mut buf = [0u8; 4];
    recv.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

/// Write a single sync entry frame.
/// Layout: [2B name_len][20B infohash][4B discovered_at][6B size][name bytes]
pub async fn write_sync_entry(
    send: &mut SendStream,
    infohash: &InfoHash,
    entry: &TorrentEntry,
) -> Result<(), Error> {
    let name_bytes = entry.name.as_bytes();
    let name_len = name_bytes.len().min(u16::MAX as usize) as u16;

    let mut frame = Vec::with_capacity(32 + name_len as usize);
    frame.extend_from_slice(&name_len.to_be_bytes());
    frame.extend_from_slice(infohash);
    frame.extend_from_slice(&entry.discovered_at.to_be_bytes());
    frame.extend_from_slice(&encode_u48(entry.size));
    frame.extend_from_slice(&name_bytes[..name_len as usize]);

    send.write_all(&frame).await?;
    Ok(())
}

/// Write EOF marker: 2 zero bytes.
pub async fn write_sync_eof(send: &mut SendStream) -> Result<(), Error> {
    send.write_all(&[0u8; 2]).await?;
    Ok(())
}

/// Read a single sync entry. Returns None on EOF marker.
pub async fn read_sync_entry(
    recv: &mut RecvStream,
) -> Result<Option<(InfoHash, TorrentEntry)>, Error> {
    let mut len_buf = [0u8; 2];
    recv.read_exact(&mut len_buf).await?;
    let name_len = u16::from_be_bytes(len_buf);

    if name_len == 0 {
        return Ok(None);
    }

    let mut header = [0u8; 30]; // 20 infohash + 4 discovered_at + 6 size
    recv.read_exact(&mut header).await?;

    let mut infohash = [0u8; 20];
    infohash.copy_from_slice(&header[..20]);
    let discovered_at =
        u32::from_be_bytes([header[20], header[21], header[22], header[23]]);
    let size = decode_u48(&[
        header[24], header[25], header[26], header[27], header[28], header[29],
    ]);

    let mut name_buf = vec![0u8; name_len as usize];
    recv.read_exact(&mut name_buf).await?;
    let name = String::from_utf8_lossy(&name_buf).into_owned();

    Ok(Some((
        infohash,
        TorrentEntry {
            name,
            size,
            discovered_at,
        },
    )))
}
