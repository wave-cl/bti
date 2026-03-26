use std::collections::HashMap;
use std::sync::OnceLock;

use bti_core::model::Category;
use regex::Regex;

struct Pattern {
    regex: Regex,
    category: Category,
    weight: f64,
}

fn patterns() -> &'static [Pattern] {
    static PATTERNS: OnceLock<Vec<Pattern>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        let p = |pat: &str, cat: Category, w: f64| Pattern {
            regex: Regex::new(&format!("(?i){}", pat)).unwrap(),
            category: cat,
            weight: w,
        };

        vec![
            // XXX
            p(r"\b(xxx|porn|brazzers|naughty[\s._-]?america|bangbros|realitykings|pornhub|xvideos|onlyfans|playboy|hustler|penthouse)\b", Category::Xxx, 1.0),
            p(r"\b(blacked|tushy|vixen|mofos|fakehub|faketaxi|digital[\s._-]?playground|wicked[\s._-]?pictures|evil[\s._-]?angel)\b", Category::Xxx, 1.0),
            p(r"\b(nubiles|babes\.com|met[\s._-]?art|x[\s._-]?art|hegre|femjoy|watch4beauty|sexart|joymii)\b", Category::Xxx, 1.0),
            p(r"\b(jav|hentai|javhd|caribbeancom|tokyo[\s._-]?hot|s[\s._-]?cute|prestige|ideapocket|moodyz)\b", Category::Xxx, 0.9),
            p(r"\b(erotic|nsfw|adult[\s._-]?only|18\+|explicit)\b", Category::Xxx, 0.5),
            p(r"\b(milf|stepmom|stepsister|stepbrother|stepdad|creampie|gangbang|threesome|orgy|blowjob|handjob|cumshot)\b", Category::Xxx, 0.9),

            // Audiobook
            p(r"\b(audiobook|audio[\s._-]?book)\b", Category::Audiobook, 1.0),
            p(r"\b(narrated[\s._]by|read[\s._]by|unabridged|abridged)\b", Category::Audiobook, 1.0),
            p(r"\b(audible|librivox|libro\.fm|libby|overdrive|chirp|kobo[\s._]audiobook)\b", Category::Audiobook, 0.9),

            // Anime
            p(r"\[(subsplease|erai[\s._-]?raws|judas|ember|horriblesubs|commie|coalgirls|doki|asw|yameii|sakurato)\]", Category::Anime, 1.0),
            p(r"\[(hs|davinci|tenrai|kawaiika[\s._-]?raws|anime[\s._-]?time|sallysubs|asenshi|vivid|underwater)\]", Category::Anime, 1.0),
            p(r"\[(kaleido|tsundere|hiryuu|chihiro|mazui|funi|crunchyroll|animekaizoku|bonkai77|reaperz)\]", Category::Anime, 1.0),
            p(r"\[(moodkiller|kawaiisubs|cerberus|kuchikirukia|beatrice[\s._-]?raws|anidl|gskanime)\]", Category::Anime, 1.0),
            p(r"\[(ohys[\s._-]?raws|leopard[\s._-]?raws|a[\s._-]?c|dmonhiro|cleo|nep|psa|sgt|sxales)\]", Category::Anime, 1.0),
            p(r"\b(anime|donghua|nyaa|mal[\s._]id)\b", Category::Anime, 0.7),
            p(r"\[.+\]\s+.+\s+-\s+\d+\s+\[(1080p|720p|480p)", Category::Anime, 0.9),
            p(r"\[[0-9A-Fa-f]{8}\]", Category::Anime, 0.3),

            // Course
            p(r"\b(udemy|coursera|skillshare|lynda|linkedin[\s._]learning|masterclass|pluralsight)\b", Category::Course, 1.0),
            p(r"\b(udacity|edx|brilliant|codecademy|treehouse|packt|o'reilly[\s._]?media)\b", Category::Course, 1.0),
            p(r"\b(khan[\s._]academy|wondrium|great[\s._]courses|mit[\s._]ocw|stanford[\s._]online)\b", Category::Course, 1.0),
            p(r"\b(datacamp|freecodecamp|egghead|laracasts|frontendmasters|zerotomastery|academind)\b", Category::Course, 0.9),
            p(r"\b(tutorial|course|bootcamp|lecture|workshop|training|lesson)\b", Category::Course, 0.5),

            // TV
            p(r"S\d{2}E\d{2}", Category::Tv, 1.0),
            p(r"S\d{2}\b", Category::Tv, 0.7),
            p(r"\bseason[\s._]\d+", Category::Tv, 0.9),
            p(r"\bcomplete[\s._](series|season)", Category::Tv, 0.9),
            p(r"\d+x\d{2}\b", Category::Tv, 0.8),
            p(r"E\d{2}[\s._-]?E\d{2}", Category::Tv, 0.9),
            p(r"\b(miniseries|mini[\s._-]?series|limited[\s._-]?series)\b", Category::Tv, 0.8),
            p(r"\b(season[\s._]?pack|complete[\s._]?pack)\b", Category::Tv, 0.8),
            p(r"\b(HDTV|PDTV|DSR|TVRip)\b", Category::Tv, 0.6),

            // Game
            p(r"\b(CODEX|SKIDROW|FitGirl|RELOADED|GOG|PLAZA|TENOKE|DODI|ElAmigos|EMPRESS|RUNE)\b", Category::Game, 1.0),
            p(r"\b(TiNYiSO|DARKSIDERS|RAZOR1911|HOODLUM|PROPHET|CPY|REPACK|DARKSiDERS)\b", Category::Game, 1.0),
            p(r"\b(CHRONOS|ANOMALY|I_KnoW|SiMPLEX|ALI213|POSTMORTEM|3DM)\b", Category::Game, 1.0),
            p(r"\b(game[\s._]of[\s._]the[\s._]year|GOTY)\b", Category::Game, 0.8),
            p(r"\bDLC[\s._]", Category::Game, 0.6),
            p(r"\bupdate[\s._]v\d", Category::Game, 0.6),
            p(r"\b(repack|repacked)\b", Category::Game, 0.6),
            p(r"\b(PS[2-5]|PS[\s._]?Vita|PSP|Xbox[\s._]?360|Xbox[\s._]?One|Xbox[\s._]?Series)\b", Category::Game, 0.8),
            p(r"\b(Nintendo[\s._]?Switch|NSW|Wii[\s._]?U|GameCube|3DS|NDS|GBA)\b", Category::Game, 0.8),

            // Movie
            p(r"\b(1080p|2160p|4k|720p)\b", Category::Movie, 0.5),
            p(r"\b(bluray|blu[\s._-]?ray|brrip|dvdrip|webrip|web[\s._-]?dl|hdtv|hdrip|remux)\b", Category::Movie, 0.6),
            p(r"\b(camrip|hdcam|telesync|telecine|dvdscr|screener|workprint)\b", Category::Movie, 0.6),
            p(r"\b(x264|x265|hevc|h\.?264|h\.?265|xvid|divx)\b", Category::Movie, 0.3),
            p(r"\b(aac|dts|dts[\s._-]?hd|atmos|truehd|10bit|hdr|hdr10|dolby[\s._]?vision)\b", Category::Movie, 0.3),
            p(r"\b(SPARKS|YIFY|YTS|RARBG|EVO|AMIABLE|FGT|GECKOS|FLUX|CMRG)\b", Category::Movie, 0.5),
            p(r"\(\d{4}\)", Category::Movie, 0.2),
            p(r"\b(multi[\s._-]?subs?|eng[\s._-]?sub|subtitles?)\b", Category::Movie, 0.3),

            // Music
            p(r"\b(discography|album|vinyl|greatest[\s._]hits|compilation|anthology)\b", Category::Music, 0.8),
            p(r"\b(EP|single|mixtape|bootleg|live[\s._]at|live[\s._]in|concert[\s._]recording)\b", Category::Music, 0.6),
            p(r"\bFLAC\b", Category::Music, 0.7),
            p(r"\b(320kbps|256kbps|192kbps|V0|CBR|VBR|lossless)\b", Category::Music, 0.7),
            p(r"\b(24bit|16bit|44\.1kHz|48kHz|96kHz|192kHz|hi[\s._-]?res)\b", Category::Music, 0.7),
            p(r"\b(records|recordings|label|soundtrack|OST|score)\b", Category::Music, 0.4),

            // Software
            p(r"\.(exe|msi|dmg|pkg|deb|rpm|apk)\b", Category::Software, 0.5),
            p(r"\b(windows|macos|mac[\s._]os[\s._]x|linux|portable)\b", Category::Software, 0.4),
            p(r"\b(crack|cracked|keygen|patch|serial|activator|loader|pre[\s._-]?activated)\b", Category::Software, 0.5),
            p(r"\b(x64|x86|arm64|64[\s._-]?bit|32[\s._-]?bit)\b", Category::Software, 0.3),
            p(r"\b(adobe|autodesk|jetbrains|microsoft[\s._](office|visual|windows))\b", Category::Software, 0.7),
            p(r"\b(vmware|parallels|solidworks|autocad|matlab|photoshop|illustrator|premiere)\b", Category::Software, 0.7),
            p(r"\bv\d+\.\d+(\.\d+)?\b", Category::Software, 0.15),

            // Book
            p(r"\b(ebook|e[\s._-]?book|epub|kindle[\s._]edition)\b", Category::Book, 0.9),
            p(r"\bpdf[\s._](collection|pack|bundle)\b", Category::Book, 0.8),
            p(r"\b(o'reilly|wiley|springer|elsevier|penguin|harper[\s._]?collins|macmillan|mcgraw[\s._]?hill)\b", Category::Book, 0.6),
            p(r"\b(apress|manning|pragmatic|addison[\s._]?wesley|pearson|oxford[\s._]university[\s._]press)\b", Category::Book, 0.6),
            p(r"\b(isbn|978[\s._-]?\d)\b", Category::Book, 0.8),
            p(r"\b(calibre|libgen|library[\s._]genesis|z[\s._-]?library|sci[\s._-]?hub)\b", Category::Book, 0.7),
            p(r"\b(comic|manga|graphic[\s._]novel|marvel|dc[\s._]comics|dark[\s._]horse)\b", Category::Book, 0.6),
        ]
    })
}

pub fn score_keywords(name: &str) -> HashMap<Category, f64> {
    let mut scores: HashMap<Category, f64> = HashMap::new();

    for pattern in patterns() {
        if pattern.regex.is_match(name) {
            *scores.entry(pattern.category).or_default() += pattern.weight;
        }
    }

    scores
}
