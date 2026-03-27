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

/// Crawler stats returned in the sync header.
pub struct SyncHeader {
    pub db_size: u64,
    pub total: u64,
    pub mem_rss: u64,
    pub disk_used: u64,
    pub disk_total: u64,
}

/// Write crawler stats header: [8B db_size][8B total][8B mem_rss][8B disk_used][8B disk_total].
pub async fn write_sync_header(send: &mut SendStream, h: &SyncHeader) -> Result<(), Error> {
    let mut buf = [0u8; 40];
    buf[0..8].copy_from_slice(&h.db_size.to_be_bytes());
    buf[8..16].copy_from_slice(&h.total.to_be_bytes());
    buf[16..24].copy_from_slice(&h.mem_rss.to_be_bytes());
    buf[24..32].copy_from_slice(&h.disk_used.to_be_bytes());
    buf[32..40].copy_from_slice(&h.disk_total.to_be_bytes());
    send.write_all(&buf).await?;
    Ok(())
}

/// Read crawler stats header.
pub async fn read_sync_header(recv: &mut RecvStream) -> Result<SyncHeader, Error> {
    let mut buf = [0u8; 40];
    recv.read_exact(&mut buf).await?;
    Ok(SyncHeader {
        db_size:    u64::from_be_bytes(buf[0..8].try_into().unwrap()),
        total:      u64::from_be_bytes(buf[8..16].try_into().unwrap()),
        mem_rss:    u64::from_be_bytes(buf[16..24].try_into().unwrap()),
        disk_used:  u64::from_be_bytes(buf[24..32].try_into().unwrap()),
        disk_total: u64::from_be_bytes(buf[32..40].try_into().unwrap()),
    })
}

/// Write a single sync entry frame.
/// Layout: [2B name_len][20B infohash][4B discovered_at][6B size][name bytes][2B file_count][per file: 8B size + 2B path_len + path bytes]
pub async fn write_sync_entry(
    send: &mut SendStream,
    infohash: &InfoHash,
    entry: &TorrentEntry,
) -> Result<(), Error> {
    let name_bytes = entry.name.as_bytes();
    let name_len = name_bytes.len().min(u16::MAX as usize) as u16;
    let file_count = entry.files.len().min(u16::MAX as usize) as u16;

    let mut frame = Vec::with_capacity(34 + name_len as usize);
    frame.extend_from_slice(&name_len.to_be_bytes());
    frame.extend_from_slice(infohash);
    frame.extend_from_slice(&entry.discovered_at.to_be_bytes());
    frame.extend_from_slice(&encode_u48(entry.size));
    frame.extend_from_slice(&name_bytes[..name_len as usize]);
    frame.extend_from_slice(&file_count.to_be_bytes());
    for f in entry.files.iter().take(file_count as usize) {
        let path_bytes = f.path.as_bytes();
        let path_len = path_bytes.len().min(u16::MAX as usize) as u16;
        frame.extend_from_slice(&f.size.to_be_bytes());
        frame.extend_from_slice(&path_len.to_be_bytes());
        frame.extend_from_slice(&path_bytes[..path_len as usize]);
    }

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

    let mut file_count_buf = [0u8; 2];
    recv.read_exact(&mut file_count_buf).await?;
    let file_count = u16::from_be_bytes(file_count_buf) as usize;

    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        let mut fhdr = [0u8; 10]; // 8B size + 2B path_len
        recv.read_exact(&mut fhdr).await?;
        let fsize = u64::from_be_bytes([fhdr[0],fhdr[1],fhdr[2],fhdr[3],fhdr[4],fhdr[5],fhdr[6],fhdr[7]]);
        let path_len = u16::from_be_bytes([fhdr[8], fhdr[9]]) as usize;
        let mut path_buf = vec![0u8; path_len];
        recv.read_exact(&mut path_buf).await?;
        files.push(crate::model::FileInfo {
            path: String::from_utf8_lossy(&path_buf).into_owned(),
            size: fsize,
        });
    }

    Ok(Some((
        infohash,
        TorrentEntry { name, size, discovered_at, files },
    )))
}
