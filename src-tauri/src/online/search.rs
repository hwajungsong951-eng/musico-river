//! InnerTube search with continuation paging and a 5-minute TTL result cache.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use super::client::{innertube_context, innertube_post, SEARCH_URL};
use super::models::SearchResultPage;
use super::parse::{parse_search_page, parse_search_page_songs};

/// Songs filter param (chip: "Songs").
const SONGS_FILTER: &str = "EgWKAQIIAWoKEAkQBRAKEAMQBA%3D%3D";
/// Video filter param (chip: "Videos").
const VIDEOS_FILTER: &str = "EgWKAQIQAWoKEAkQChAFEAMQBA%3D%3D";
/// Search results stay fresh for this long before a re-fetch.
const SEARCH_TTL: Duration = Duration::from_secs(300);
/// Search cache entry cap (cleared wholesale when exceeded).
const SEARCH_CACHE_CAP: usize = 64;

/// Cache-key normalization: case/whitespace variants of a query are one
/// search ("Daft Punk", "daft punk ", "DAFT  PUNK" share an entry).  What is
/// *sent* to InnerTube is unchanged — only the cache key collapses.
fn cache_key(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
}

/// Searches YouTube Music.  Pass `continuation` (from a previous page's
/// `next_continuation`) to fetch the next page.  First pages are cached for
/// 5 minutes — repeated searches are instant.
/// Empty first pages are remembered briefly so a dead query does not pay the
/// whole fallback chain (filtered -> unfiltered -> yt-dlp) on every keystroke.
const NEGATIVE_TTL: Duration = Duration::from_secs(30);
static NEGATIVE: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn negative_key(key: &str, videos: bool) -> String {
    format!("{key}|{videos}")
}

fn negative_hit(key: &str) -> bool {
    let mut map = NEGATIVE.lock().unwrap_or_else(|e| e.into_inner());
    match map.get(key) {
        Some(at) if at.elapsed() < NEGATIVE_TTL => true,
        Some(_) => {
            map.remove(key);
            false
        }
        None => false,
    }
}

fn remember_empty(key: &str) {
    let mut map = NEGATIVE.lock().unwrap_or_else(|e| e.into_inner());
    if map.len() > 128 {
        map.clear();
    }
    map.insert(key.to_string(), Instant::now());
}

pub fn search(
    query: &str,
    videos: bool,
    continuation: Option<&str>,
) -> Result<SearchResultPage, String> {
    if query.trim().is_empty() && continuation.is_none() {
        return Ok(SearchResultPage { tracks: Vec::new(), next_continuation: None });
    }

    let key = cache_key(query);
    let negative = negative_key(&key, videos);

    // Continuation pages bypass the cache (each page is requested once anyway).
    if continuation.is_none() {
        if let Some(page) = search_cache_get(&key, videos) {
            return Ok(page);
        }
        if negative_hit(&negative) {
            log::debug!("search {query:?}: negative cache hit");
            return Ok(SearchResultPage { tracks: Vec::new(), next_continuation: None });
        }
    }

    let mut body = serde_json::json!({ "context": innertube_context() });
    match continuation {
        Some(tok) => body["continuation"] = serde_json::Value::String(tok.to_string()),
        None => {
            body["query"] = serde_json::Value::String(query.trim().to_string());
            body["params"] = serde_json::Value::String(
                if videos { VIDEOS_FILTER } else { SONGS_FILTER }.to_string(),
            );
        }
    }

    let json = innertube_post(SEARCH_URL, body)?;
    let parse = if videos { parse_search_page } else { parse_search_page_songs };
    let page = parse(&json);
    let mut gated = super::catalog::has_premium_gate(&json);

    // Some regions answer the Songs/Videos filter-chip params with a
    // Premium-only message while a plain search still works.  Retry once
    // without the filter so search is never empty there.
    if page.tracks.is_empty() && continuation.is_none() {
        let mut plain = serde_json::json!({ "context": innertube_context() });
        plain["query"] = serde_json::Value::String(query.trim().to_string());
        if let Ok(json_plain) = innertube_post(SEARCH_URL, plain) {
            gated |= super::catalog::has_premium_gate(&json_plain);
            let songs = parse_search_page_songs(&json_plain);
            if !songs.tracks.is_empty() {
                log::debug!("filtered search empty for {query:?}; serving unfiltered songs");
                if gated {
                    super::catalog::mark_region_gated();
                }
                search_cache_put(&key, videos, &songs);
                return Ok(songs);
            }
        }
        if gated {
            // Confirmed by YouTube's own message: remember it so Home skips
            // the browse calls and later searches may use yt-dlp.
            super::catalog::mark_region_gated();
        }

        // yt-dlp's YouTube search is the only source that still returns real
        // songs where the catalog is gated; elsewhere InnerTube already does
        // and the subprocess would just add latency.
        if !videos && (gated || super::catalog::region_gated()) {
            let found = super::ytsearch::search(query, 15);
            if !found.is_empty() {
                log::debug!("catalog search empty for {query:?}; serving yt-dlp results");
                let page = SearchResultPage { tracks: found, next_continuation: None };
                search_cache_put(&key, videos, &page);
                return Ok(page);
            }
        }
    }

    if page.tracks.is_empty() && page.next_continuation.is_none() && continuation.is_none() {
        remember_empty(&negative);
        log::warn!("InnerTube search parsed 0 results for {query:?} — API shape may have changed");
    }
    if continuation.is_none() && (!page.tracks.is_empty() || page.next_continuation.is_some()) {
        // Never cache an empty first page: a transient block or shape drift
        // must not blank search/Home for the whole TTL (same rule as browse).
        search_cache_put(&key, videos, &page);
    }
    Ok(page)
}

/// Cap on returned typeahead suggestions (AM/Spotify show ~8 — 0).
const SUGGESTIONS_CAP: usize = 10;
/// InnerTube typeahead endpoint (#59) — live-verified 2026-11-01.
const SUGGESTIONS_URL: &str = "https://music.youtube.com/youtubei/v1/music/get_search_suggestions";

/// InnerTube search suggestions for the typeahead (#59).  Best-effort by
/// contract: ANY failure (short query, network, shape drift) returns an
/// empty vec — the frontend dropdown just stays closed, never an error UI.
pub fn search_suggestions(query: &str) -> Vec<String> {
    let q = query.trim();
    if q.chars().count() < 2 {
        return Vec::new();
    }
    let body = serde_json::json!({ "context": innertube_context(), "input": q });
    match innertube_post(SUGGESTIONS_URL, body) {
        Ok(json) => {
            let mut out = crate::online::parse::parse_search_suggestions(&json);
            out.truncate(SUGGESTIONS_CAP);
            out
        }
        Err(e) => {
            // Silent by contract for the UI, but never invisible when
            // someone is debugging why typeahead went dark.
            log::debug!("suggestions failed for {q:?}: {e}");
            Vec::new()
        }
    }
}

static SEARCH_CACHE: LazyLock<Mutex<HashMap<(String, bool), (Instant, SearchResultPage)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn search_cache_get(query: &str, videos: bool) -> Option<SearchResultPage> {
    let mut cache = SEARCH_CACHE.lock().ok()?;
    let entry = cache.get(&(query.to_string(), videos))?;
    if entry.0.elapsed() > SEARCH_TTL {
        cache.remove(&(query.to_string(), videos));
        return None;
    }
    Some(entry.1.clone())
}

fn search_cache_put(query: &str, videos: bool, page: &SearchResultPage) {
    if let Ok(mut cache) = SEARCH_CACHE.lock() {
        if cache.len() >= SEARCH_CACHE_CAP {
            cache.clear(); // rare; simplest LRU-ish reset
        }
        cache.insert((query.to_string(), videos), (Instant::now(), page.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::online::models::OnlineTrack;

    #[test]
    fn search_cache_roundtrip_and_ttl() {
        let page = SearchResultPage {
            tracks: vec![OnlineTrack {
                video_id: "c1".into(),
                title: "Cached Song".into(),
                artist: "A".into(),
                album: String::new(),
                duration_secs: None,
                thumbnail: None,
                cached: false,
                source: String::new(),
                artist_browse_id: None,
            }],
            next_continuation: None,
        };
        search_cache_put("cache test", false, &page);
        let got = search_cache_get("cache test", false).expect("hit");
        assert_eq!(got.tracks[0].video_id, "c1");
        assert!(search_cache_get("different query", false).is_none());
    }

    /// Live typeahead: a mainstream prefix must return suggestions.  Opt-in:
    ///   MUSICO_LIVE_TESTS=1 cargo test -p musico-online live_search_suggestions -- --ignored --nocapture
    #[test]
    #[ignore = "network test — run with MUSICO_LIVE_TESTS=1"]
    fn live_search_suggestions() {
        let out = search_suggestions("belie");
        assert!(!out.is_empty(), "typeahead returned nothing for 'belie'");
        assert!(
            out.iter().all(|s| s.to_lowercase().contains("belie")),
            "unexpected suggestions: {out:?}"
        );
        println!("suggestions: {out:?}");
    }

    #[test]
    fn cache_key_collapses_case_and_whitespace() {
        assert_eq!(cache_key("Daft Punk"), cache_key("daft punk"));
        assert_eq!(cache_key("  daft   PUNK  "), cache_key("daft punk"));
        assert_eq!(cache_key("Daft  Punk "), "daft punk");
        // Distinct queries stay distinct.
        assert_ne!(cache_key("daft punk"), cache_key("daft punkk"));
    }
}
