use std::sync::OnceLock;

use bti_core::model::Category;

mod keywords;
mod extensions;

const MIN_SCORE: f64 = 0.3;
const NUM_CATEGORIES: usize = 11;

/// Classify a torrent by name only (no file list).
pub fn classify(name: &str) -> Category {
    let mut scores = keywords::score_keywords(name);
    disambiguate(name, &mut scores);
    pick_best(&scores)
}

fn disambiguate(name: &str, scores: &mut [f64; NUM_CATEGORIES]) {
    let xxx = Category::Xxx as usize;
    let movie = Category::Movie as usize;
    let tv = Category::Tv as usize;
    let anime = Category::Anime as usize;
    let game = Category::Game as usize;
    let software = Category::Software as usize;
    let course = Category::Course as usize;

    // XXX vs Movie: XXX keywords override quality tags
    if scores[xxx] > 0.0 {
        let max_other = scores.iter().enumerate()
            .filter(|&(i, _)| i != xxx)
            .map(|(_, &v)| v)
            .fold(0.0_f64, f64::max);
        scores[xxx] = scores[xxx].max(max_other) + 0.5;
    }

    // TV vs Movie: S01E01 pattern
    if scores[tv] > 0.0 && scores[movie] > 0.0 {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| regex::Regex::new(r"(?i)S\d{2}E\d{2}").unwrap());
        if re.is_match(name) {
            scores[tv] += 0.5;
        }
    }

    // Anime vs Movie: fansub brackets
    if scores[anime] > 0.0 && scores[movie] > 0.0 {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| regex::Regex::new(r"\[.+\]").unwrap());
        if re.is_match(name) {
            scores[anime] += 0.5;
        }
    }

    // Game vs Software: scene groups
    if scores[game] > 0.0 && scores[software] > 0.0 {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            regex::Regex::new(r"(?i)\b(CODEX|SKIDROW|FitGirl|RELOADED|GOG|PLAZA|TENOKE|DODI|ElAmigos|EMPRESS|RUNE|TiNYiSO|DARKSIDERS|RAZOR1911|HOODLUM|PROPHET|CPY)\b").unwrap()
        });
        if re.is_match(name) {
            scores[game] += 0.5;
        }
    }

    // Course vs Software: unconditional boost
    if scores[course] > 0.0 && scores[software] > 0.0 {
        scores[course] += 0.3;
    }
}

fn pick_best(scores: &[f64; NUM_CATEGORIES]) -> Category {
    let mut best_idx = 0;
    let mut best_score = 0.0_f64;

    for (i, &score) in scores.iter().enumerate() {
        if score > best_score {
            best_score = score;
            best_idx = i;
        }
    }

    if best_score < MIN_SCORE {
        Category::Other
    } else {
        Category::from_u8(best_idx as u8)
    }
}
