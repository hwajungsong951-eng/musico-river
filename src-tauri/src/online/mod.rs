pub mod client;
pub mod models;
pub mod parse;
pub mod search;
pub mod stream;
pub mod ytsearch;

pub mod catalog {
    pub fn has_premium_gate(_: &serde_json::Value) -> bool { false }
    pub fn mark_region_gated() {}
    pub fn region_gated() -> bool { false }
}

pub mod saavn {
    pub const SOURCE: &str = "saavn";
    pub fn resolve_track(_: &super::models::OnlineTrack, _: bool) -> Result<std::path::PathBuf, String> {
        Err("saavn disabled".into())
    }
    pub fn try_cross_source_resolve(_: &super::models::OnlineTrack, _: bool) -> Option<std::path::PathBuf> {
        None
    }
}

pub mod audius {
    pub const SOURCE: &str = "audius";
    pub fn resolve_track(_: &super::models::OnlineTrack, _: bool) -> Result<std::path::PathBuf, String> {
        Err("audius disabled".into())
    }
}

pub mod sources {
    pub fn try_fast_path(_: &str) -> Result<std::path::PathBuf, String> {
        Err("sources fast path disabled".into())
    }
}

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use musico_playback::SongInfo;
use crate::state::TrackMeta;

const SEARCH_TTL: Duration = Duration::from_secs(75);
const SEARCH_CACHE_CAP: usize = 20;

static SEARCH_CACHE: LazyLock<Mutex<HashMap<String, (Instant, Vec<TrackMeta>)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static LAST_SEARCH_QUERY: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(String::new()));

/// 검색 결과 5곡 반환 (60~90초 TTL, 최대 20개 캐시)
pub async fn search_top_5(query: &str) -> Result<Vec<TrackMeta>, String> {
    let q = query.trim().to_lowercase();
    if q.is_empty() {
        return Ok(Vec::new());
    }
    *LAST_SEARCH_QUERY.lock().unwrap() = q.clone();

    {
        let mut cache = SEARCH_CACHE.lock().unwrap();
        if let Some((at, items)) = cache.get(&q) {
            if at.elapsed() < SEARCH_TTL {
                return Ok(items.clone());
            }
            cache.remove(&q);
        }
    }

    let q_owned = query.trim().to_string();
    let results: Vec<TrackMeta> = tokio::task::spawn_blocking(move || {
        let mut tracks: Vec<models::OnlineTrack> = search::search(&q_owned, false, None)
            .map(|p| p.tracks)
            .unwrap_or_default()
            .into_iter()
            .filter(|t| matches!(t.duration_secs, Some(d) if d > 0 && d <= 600))
            .collect();
        if tracks.is_empty() {
            tracks = youtube_web_search_fallback(&q_owned);
        }
        tracks
    })
    .await
    .map_err(|e| e.to_string())?
    .into_iter()
    .take(5)
    .map(|t| TrackMeta {
        id: t.video_id,
        title: t.title,
        artist: t.artist,
    })
    .collect();

    {
        let mut cache = SEARCH_CACHE.lock().unwrap();
        if cache.len() >= SEARCH_CACHE_CAP {
            cache.clear();
        }
        cache.insert(q, (Instant::now(), results.clone()));
    }

    Ok(results)
}

/// 원곡 기준 비슷한 곡 5곡 검색 (YouTube Related 1순위 + 커버곡/동일 아티스트 차단)
pub async fn fetch_similar_5(
    original: &TrackMeta,
    recent_ids: &[String],
) -> Result<Vec<TrackMeta>, String> {
    let orig_id = original.id.clone();
    let orig_title = original.title.to_lowercase();
    let orig_artist = original.artist.clone();
    let recent = recent_ids.to_vec();
    let last_query = LAST_SEARCH_QUERY.lock().unwrap().clone();

    let picked = tokio::task::spawn_blocking(move || {
        // 1차: 일반 YouTube Related (youtube_next) -> 지역 제한 없이 20~30개 관련 곡 확보
        let mut candidates = client::youtube_next(&orig_id)
            .map(|rel| parse::parse_related_response(&rel))
            .unwrap_or_default();

        // 2차: 비어 있으면 YouTube Music 라디오 (RD<video_id>)
        if candidates.is_empty() {
            let body = serde_json::json!({
                "context": client::innertube_context(),
                "videoId": orig_id,
                "playlistId": format!("RD{orig_id}"),
            });
            candidates = client::innertube_post(client::NEXT_URL, body)
                .map(|j| parse::parse_radio_response(&j))
                .unwrap_or_default();
        }

        // 3차: 그래도 부족하면 아티스트 기반 검색 폴백
        if candidates.len() < 8 && !orig_artist.is_empty() {
            let extra = youtube_web_search_fallback(&format!("{orig_artist} 노래"));
            candidates.extend(extra);
        }

        let core_tokens = extract_core_tokens(&orig_title, &orig_artist.to_lowercase());
        let query_tokens = extract_core_tokens(&last_query, "");

        let mut seen_titles = std::collections::HashSet::new();
        let mut seen_artists = std::collections::HashSet::new();
        seen_titles.insert(orig_title.clone());

        let mut out = Vec::new();
        for t in candidates {
            if t.video_id == orig_id || recent.contains(&t.video_id) {
                continue;
            }
            if !matches!(t.duration_secs, Some(d) if d > 0 && d <= 600) {
                continue;
            }

            let title_key = t.title.to_lowercase();
            let artist_key = t.artist.trim().to_lowercase();

            // 원곡과 제목이 같거나 핵심 곡명 토큰/검색어가 겹치는 커버곡(가수만 다른 동일 곡) 제외
            if is_cover_or_same(&title_key, &orig_title, &core_tokens, &query_tokens) {
                continue;
            }
            if !seen_titles.insert(title_key) {
                continue;
            }
            // 5곡 내 동일 아티스트 중복 제한 (다양성 보장)
            if !artist_key.is_empty() && !seen_artists.insert(artist_key) {
                continue;
            }

            out.push(TrackMeta {
                id: t.video_id,
                title: t.title,
                artist: t.artist,
            });
            if out.len() >= 5 {
                break;
            }
        }
        out
    })
    .await
    .map_err(|e| e.to_string())?;

    Ok(picked)
}

fn extract_core_tokens(title: &str, artist: &str) -> Vec<String> {
    const NOISE: &[&str] = &[
        "official", "video", "audio", "mv", "m/v", "lyrics", "lyric", "live",
        "cover", "ver", "version", "ost", "hd", "4k", "hq", "feat", "ft",
        "가사", "노래", "라이브", "커버", "공식", "뮤비",
    ];
    let mut cleaned = String::with_capacity(title.len());
    let mut depth = 0usize;
    for ch in title.chars() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                if ch.is_alphanumeric() || ch.is_whitespace() {
                    cleaned.push(ch);
                } else {
                    cleaned.push(' ');
                }
            }
            _ => {}
        }
    }
    cleaned
        .split_whitespace()
        .map(|w| w.trim().to_string())
        .filter(|w| w.chars().count() >= 2)
        .filter(|w| !NOISE.contains(&w.as_str()))
        .filter(|w| artist.is_empty() || !artist.contains(w.as_str()))
        .collect()
}

fn is_cover_or_same(
    cand_title: &str,
    orig_title: &str,
    core_tokens: &[String],
    query_tokens: &[String],
) -> bool {
    if cand_title == orig_title {
        return true;
    }
    if !core_tokens.is_empty() {
        let hits = core_tokens
            .iter()
            .filter(|tok| cand_title.contains(tok.as_str()))
            .count();
        // 핵심 곡명 단어가 2개 이상 겹치거나, 1단어 곡명인데 포함되면 커버곡으로 간주
        if (core_tokens.len() == 1 && hits == 1) || hits >= 2 {
            return true;
        }
    }
    if !query_tokens.is_empty() {
        let q_hits = query_tokens
            .iter()
            .filter(|tok| cand_title.contains(tok.as_str()))
            .count();
        if (query_tokens.len() == 1 && q_hits == 1) || q_hits >= 2 {
            return true;
        }
    }
    false
}

/// 재생 시점에만 스트림 해석 (캐시하지 않음)
pub async fn resolve_song_info(track_id: &str) -> Result<SongInfo, String> {
    let id = track_id.to_string();
    let path = tokio::task::spawn_blocking(move || {
        stream::resolve_stream(&id, Duration::from_secs(15))
    })
    .await
    .map_err(|e| e.to_string())??;

    Ok(SongInfo {
        id: track_id.to_string(),
        file_path: path.to_string_lossy().into_owned(),
        title: String::new(),
        artist: String::new(),
        album: String::new(),
        duration_secs: 0.0,
        cover_art: None,
    })
}

/// InnerTube가 비어 있을 때 yt-dlp 및 YouTube 웹 검색에서 직접 상위 5곡 추출
fn youtube_web_search_fallback(query: &str) -> Vec<models::OnlineTrack> {
    let mut cmd = std::process::Command::new("yt-dlp");
    cmd.args([
        "--flat-playlist",
        "--dump-single-json",
        "--no-warnings",
        "--quiet",
        &format!("ytsearch12:{query}"),
    ]);
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    if let Ok(out) = cmd.output()
    {
        if out.status.success() {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                if let Some(entries) = v.get("entries").and_then(|e| e.as_array()) {
                    let tracks: Vec<models::OnlineTrack> = entries
                        .iter()
                        .filter_map(|e| {
                            // 10분(600초) 초과 장시간 영상(설교/합본/라이브) 제외
                            let dur = e.get("duration").and_then(|d| d.as_f64()).unwrap_or(0.0);
                            if dur <= 0.0 || dur > 600.0 {
                                return None;
                            }
                            let id = e.get("id")?.as_str()?.to_string();
                            let title = e.get("title")?.as_str()?.to_string();
                            let artist = e
                                .get("uploader")
                                .or_else(|| e.get("channel"))
                                .and_then(|u| u.as_str())
                                .unwrap_or("YouTube")
                                .to_string();
                            Some(models::OnlineTrack {
                                video_id: id,
                                title,
                                artist,
                                album: String::new(),
                                duration_secs: Some(dur as u32),
                                thumbnail: None,
                                source: "youtube".to_string(),
                                cached: false,
                                artist_browse_id: None,
                            })
                        })
                        .take(5)
                        .collect();
                    if !tracks.is_empty() {
                        return tracks;
                    }
                }
            }
        }
    }

    let client = match reqwest::blocking::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .timeout(Duration::from_secs(8))
        .build()
    {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let resp = match client
        .get("https://www.youtube.com/results")
        .query(&[("search_query", query)])
        .send()
    {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let html = match resp.text() {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };

    let marker = "var ytInitialData = ";
    let start = match html.find(marker) {
        Some(idx) => idx + marker.len(),
        None => return Vec::new(),
    };
    let rest = &html[start..];
    let end = match rest.find(";</script>") {
        Some(idx) => idx,
        None => return Vec::new(),
    };
    let json: serde_json::Value = match serde_json::from_str(&rest[..end]) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    collect_video_renderers(&json, &mut out);
    out
}

fn collect_video_renderers(val: &serde_json::Value, out: &mut Vec<models::OnlineTrack>) {
    if out.len() >= 5 {
        return;
    }
    match val {
        serde_json::Value::Object(map) => {
            if let Some(vr) = map.get("videoRenderer") {
                if let Some(vid) = vr.get("videoId").and_then(|v| v.as_str()) {
                    let length_text = vr
                        .pointer("/lengthText/simpleText")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let dur = parse_hms_secs(length_text);
                    // 10분(600초) 초과 또는 길이 불명(라이브 등) 영상 제외
                    if !matches!(dur, Some(d) if d > 0 && d <= 600) {
                        return;
                    }
                    let title = vr
                        .pointer("/title/runs/0/text")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown")
                        .to_string();
                    let artist = vr
                        .pointer("/ownerText/runs/0/text")
                        .and_then(|v| v.as_str())
                        .unwrap_or("YouTube")
                        .to_string();
                    out.push(models::OnlineTrack {
                        video_id: vid.to_string(),
                        title,
                        artist,
                        album: String::new(),
                        duration_secs: dur,
                        thumbnail: None,
                        source: "youtube".to_string(),
                        cached: false,
                        artist_browse_id: None,
                    });
                }
                return;
            }
            for v in map.values() {
                collect_video_renderers(v, out);
                if out.len() >= 5 {
                    break;
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_video_renderers(v, out);
                if out.len() >= 5 {
                    break;
                }
            }
        }
        _ => {}
    }
}

fn parse_hms_secs(text: &str) -> Option<u32> {
    let parts: Vec<u32> = text
        .trim()
        .split(':')
        .map(|p| p.trim().parse::<u32>().ok())
        .collect::<Option<Vec<_>>>()?;
    match parts.as_slice() {
        [m, s] => Some(m * 60 + s),
        [h, m, s] => Some(h * 3600 + m * 60 + s),
        _ => None,
    }
}
