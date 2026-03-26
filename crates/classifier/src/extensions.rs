// Extension-to-category weight mappings.
// Not used in name-only classification mode, but available for future file-list support.

use std::collections::HashMap;
use bti_core::model::Category;

pub type ExtWeights = HashMap<&'static str, Vec<(Category, f64)>>;

pub fn extension_weights() -> ExtWeights {
    let mut m = HashMap::new();

    // Video
    m.insert(".mkv", vec![(Category::Movie, 0.4), (Category::Tv, 0.3), (Category::Anime, 0.2), (Category::Xxx, 0.1)]);
    m.insert(".avi", vec![(Category::Movie, 0.4), (Category::Tv, 0.3), (Category::Anime, 0.2), (Category::Xxx, 0.1)]);
    m.insert(".mp4", vec![(Category::Movie, 0.3), (Category::Tv, 0.25), (Category::Xxx, 0.2), (Category::Anime, 0.1), (Category::Course, 0.15)]);
    m.insert(".wmv", vec![(Category::Movie, 0.3), (Category::Tv, 0.2), (Category::Xxx, 0.4), (Category::Anime, 0.1)]);
    m.insert(".ts", vec![(Category::Movie, 0.3), (Category::Tv, 0.4), (Category::Anime, 0.2), (Category::Xxx, 0.1)]);

    // Audio
    m.insert(".mp3", vec![(Category::Music, 0.7), (Category::Audiobook, 0.2), (Category::Course, 0.1)]);
    m.insert(".flac", vec![(Category::Music, 0.9), (Category::Audiobook, 0.1)]);
    m.insert(".m4a", vec![(Category::Music, 0.5), (Category::Audiobook, 0.4), (Category::Course, 0.1)]);
    m.insert(".m4b", vec![(Category::Audiobook, 1.0)]);

    // Ebook
    m.insert(".epub", vec![(Category::Book, 1.0)]);
    m.insert(".mobi", vec![(Category::Book, 1.0)]);
    m.insert(".azw3", vec![(Category::Book, 1.0)]);
    m.insert(".cbr", vec![(Category::Book, 1.0)]);
    m.insert(".cbz", vec![(Category::Book, 1.0)]);
    m.insert(".pdf", vec![(Category::Book, 0.5), (Category::Course, 0.3), (Category::Software, 0.2)]);

    // Software/Game
    m.insert(".exe", vec![(Category::Software, 0.6), (Category::Game, 0.4)]);
    m.insert(".msi", vec![(Category::Software, 0.8), (Category::Game, 0.2)]);
    m.insert(".dmg", vec![(Category::Software, 0.8), (Category::Game, 0.2)]);
    m.insert(".iso", vec![(Category::Game, 0.4), (Category::Software, 0.4), (Category::Movie, 0.2)]);
    m.insert(".nsp", vec![(Category::Game, 1.0)]);
    m.insert(".xci", vec![(Category::Game, 1.0)]);

    m
}
