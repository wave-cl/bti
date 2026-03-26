pub type InfoHash = [u8; 20];

/// Epoch offset: seconds since 2020-01-01T00:00:00Z.
/// Stored as u32, giving range until ~2156.
pub const EPOCH_OFFSET: u64 = 1_577_836_800;

#[derive(Clone, Debug)]
pub struct TorrentEntry {
    pub name: String,
    pub size: u64,
    pub discovered_at: u32, // epoch-offset seconds
}

impl TorrentEntry {
    pub fn unix_timestamp(&self) -> u64 {
        self.discovered_at as u64 + EPOCH_OFFSET
    }

    pub fn from_unix(name: String, size: u64, unix_secs: u64) -> Self {
        let discovered_at = (unix_secs.saturating_sub(EPOCH_OFFSET)) as u32;
        Self {
            name,
            size,
            discovered_at,
        }
    }
}

/// Encode a u64 as 6-byte big-endian (u48).
pub fn encode_u48(val: u64) -> [u8; 6] {
    let bytes = val.to_be_bytes();
    [bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]
}

/// Decode 6-byte big-endian (u48) to u64.
pub fn decode_u48(bytes: &[u8; 6]) -> u64 {
    u64::from_be_bytes([0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]])
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Category {
    Other = 0,
    Movie = 1,
    Tv = 2,
    Music = 3,
    Software = 4,
    Game = 5,
    Book = 6,
    Xxx = 7,
    Audiobook = 8,
    Anime = 9,
    Course = 10,
}

impl Category {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Movie,
            2 => Self::Tv,
            3 => Self::Music,
            4 => Self::Software,
            5 => Self::Game,
            6 => Self::Book,
            7 => Self::Xxx,
            8 => Self::Audiobook,
            9 => Self::Anime,
            10 => Self::Course,
            _ => Self::Other,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Other => "other",
            Self::Movie => "movie",
            Self::Tv => "tv",
            Self::Music => "music",
            Self::Software => "software",
            Self::Game => "game",
            Self::Book => "book",
            Self::Xxx => "xxx",
            Self::Audiobook => "audiobook",
            Self::Anime => "anime",
            Self::Course => "course",
        }
    }

    pub const ALL: [Category; 11] = [
        Self::Other,
        Self::Movie,
        Self::Tv,
        Self::Music,
        Self::Software,
        Self::Game,
        Self::Book,
        Self::Xxx,
        Self::Audiobook,
        Self::Anime,
        Self::Course,
    ];
}
