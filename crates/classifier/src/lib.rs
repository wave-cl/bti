use std::collections::HashMap;
use std::sync::OnceLock;

use bti_core::model::Category;

mod keywords;
mod extensions;

const MIN_SCORE: f64 = 0.3;

/// Classify a torrent by name only (no file list).
pub fn classify(name: &str) -> Category {
    let mut scores: HashMap<Category, f64> = HashMap::new();

    // Phase 1: keyword scoring
    let kw_scores = keywords::score_keywords(name);
    for (&cat, &score) in &kw_scores {
        *scores.entry(cat).or_default() += score;
    }

    // Phase 2: skip file extension scoring (no file list in crawl-only mode)

    // Phase 3: disambiguation
    disambiguate(name, &mut scores, &kw_scores);

    // Phase 5: pick winner
    pick_best(&scores)
}

fn disambiguate(
    name: &str,
    scores: &mut HashMap<Category, f64>,
    kw_scores: &HashMap<Category, f64>,
) {
    // XXX vs Movie: XXX keywords override quality tags
    if kw_scores.get(&Category::Xxx).copied().unwrap_or(0.0) > 0.0 {
        let max_other = scores
            .iter()
            .filter(|(&k, _)| k != Category::Xxx)
            .map(|(_, &v)| v)
            .fold(0.0_f64, f64::max);
        let xxx = scores.entry(Category::Xxx).or_default();
        *xxx = xxx.max(max_other) + 0.5;
    }

    // TV vs Movie: S01E01 pattern
    let has_tv = scores.contains_key(&Category::Tv);
    let has_movie = scores.contains_key(&Category::Movie);
    if has_tv && has_movie {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| regex::Regex::new(r"(?i)S\d{2}E\d{2}").unwrap());
        if re.is_match(name) {
            *scores.entry(Category::Tv).or_default() += 0.5;
        }
    }

    // Anime vs Movie: fansub brackets
    let has_anime = scores.contains_key(&Category::Anime);
    if has_anime && has_movie {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| regex::Regex::new(r"\[.+\]").unwrap());
        if re.is_match(name) {
            *scores.entry(Category::Anime).or_default() += 0.5;
        }
    }

    // Game vs Software: scene groups
    let has_game = scores.contains_key(&Category::Game);
    let has_software = scores.contains_key(&Category::Software);
    if has_game && has_software {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            regex::Regex::new(r"(?i)\b(CODEX|SKIDROW|FitGirl|RELOADED|GOG|PLAZA|TENOKE|DODI|ElAmigos|EMPRESS|RUNE|TiNYiSO|DARKSIDERS|RAZOR1911|HOODLUM|PROPHET|CPY)\b").unwrap()
        });
        if re.is_match(name) {
            *scores.entry(Category::Game).or_default() += 0.5;
        }
    }

    // Course vs Software: unconditional boost
    let has_course = scores.contains_key(&Category::Course);
    if has_course && has_software {
        *scores.entry(Category::Course).or_default() += 0.3;
    }
}

fn pick_best(scores: &HashMap<Category, f64>) -> Category {
    let mut best_cat = Category::Other;
    let mut best_score = 0.0_f64;

    for (&cat, &score) in scores {
        if score > best_score {
            best_score = score;
            best_cat = cat;
        }
    }

    if best_score < MIN_SCORE {
        Category::Other
    } else {
        best_cat
    }
}
