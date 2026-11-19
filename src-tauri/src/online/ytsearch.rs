//! # yt-dlp search fallback
//!
//! Some regions gate YouTube Music's InnerTube search (the Songs chip and the
//! browse catalog answer with a Premium-only message), so the catalog layer
//! can come back with entity cards but no playable tracks.  `yt-dlp` already
//! ships as the playback fallback, and `ytsearch` resolves against plain
//! YouTube — so the same binary can supply the missing catalog rows.
//!
//! Best-effort by contract: a missing binary, a timeout or malformed JSON all
//! yield an empty list, never an error.  The call is bounded: the child is
//! killed when the budget expires, so a hung extractor can't pin a search.

use std::io::Read;
use std::process::Stdio;
use std::sync::mpsc;
use std::time::Duration;

use serde_json::Value;

use super::models::OnlineTrack;
use super::stream;

/// Wall-clock bound for the whole search (spawn + extraction + JSON).  This is
/// a last-resort fallback, so it stays short: a slow extractor must not pin a
/// search when Saavn/Audius are already answering.
const SEARCH_BUDGET: Duration = Duration::from_secs(8);
/// Refuse absurd "tracks" (full streams/compilations) as search rows.
const MAX_TRACK_SECS: f64 = 2.0 * 3600.0;

/// Runs `yt-dlp "ytsearchN:<query>"` and maps the flat playlist to tracks.
pub fn search(query: &str, limit: usize) -> Vec<OnlineTrack> {
    let q = query.trim();
    if q.is_empty() || limit == 0 || !stream::yt_dlp_available() {
        return Vec::new();
    }
    let limit = limit.min(25);
    let target = format!("ytsearch{limit}:{q}");

    let mut child = match stream::yt_dlp_cmd()
        .args([
            "--flat-playlist",
            "--dump-single-json",
            "--no-warnings",
            "--no-call-home",
            "--socket-timeout",
            "8",
            &target,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            log::debug!("ytsearch: spawn failed: {e}");
            return Vec::new();
        }
    };

    let Some(stdout) = child.stdout.take() else { return Vec::new() };
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("ytsearch-read".into())
        .spawn(move || {
            let mut buf = String::new();
            let mut reader = std::io::BufReader::new(stdout);
            let _ = reader.read_to_string(&mut buf);
            let _ = tx.send(buf);
        })
        .ok();

    let json = match rx.recv_timeout(SEARCH_BUDGET) {
        Ok(json) => {
            let _ = child.wait();
            json
        }
        Err(_) => {
            // Budget exhausted: kill the extractor and report nothing.
            let _ = child.kill();
            let _ = child.wait();
            log::debug!("ytsearch: budget exhausted for {q:?}");
            return Vec::new();
        }
    };

    let mut tracks = parse_entries(&json, limit);
    // Drop bulk compilation channels for generic queries, but never for a
    // query that explicitly asks for them ("tamil songs").
    if !super::parse::query_targets_foreign_collection(q) {
        tracks.retain(|t| !super::parse::is_bulk_foreign_collection(&t.title));
    }
    tracks
}

/// Parses `--dump-single-json` output.  Split out so it can be unit-tested
/// with a fixture (no subprocess, no network).
fn parse_entries(json: &str, limit: usize) -> Vec<OnlineTrack> {
    let Ok(root) = serde_json::from_str::<Value>(json) else { return Vec::new() };
    let entries = root
        .get("entries")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    entries
        .into_iter()
        .filter_map(entry_to_track)
        .take(limit)
        .collect()
}

fn entry_to_track(entry: Value) -> Option<OnlineTrack> {
    let id = entry.get("id").and_then(Value::as_str)?.trim().to_string();
    if !valid_video_id(&id) {
        return None;
    }
    let title = entry.get("title").and_then(Value::as_str)?.trim().to_string();
    if title.is_empty() {
        return None;
    }
    let duration_secs = entry
        .get("duration")
        .and_then(Value::as_f64)
        .filter(|d| *d > 0.0 && *d <= MAX_TRACK_SECS)
        .map(|d| d.round() as u32);
    let artist = entry
        .get("uploader")
        .or_else(|| entry.get("channel"))
        .or_else(|| entry.get("artist"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let thumbnail = entry
        .get("thumbnails")
        .and_then(Value::as_array)
        .and_then(|list| list.last())
        .and_then(|t| t.get("url"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            entry
                .get("thumbnail")
                .and_then(Value::as_str)
                .map(str::to_string)
        });

    let mut track = OnlineTrack {
        video_id: id,
        title,
        artist,
        album: String::new(),
        duration_secs,
        thumbnail,
        cached: false,
        source: String::new(), // "" == YouTube (same as InnerTube rows)
        artist_browse_id: None,
    };
    track.cached = stream::cached_file(&track.video_id).is_some();
    Some(track)
}

/// YouTube ids are 11 URL-safe base64-ish characters.
fn valid_video_id(id: &str) -> bool {
    id.len() == 11 && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_playlist_entries() {
        let json = serde_json::json!({
            "entries": [
                { "id": "dQw4w9WgXcQ", "title": "Nightcall", "duration": 258.0,
                  "uploader": "Kavinsky", "thumbnails": [{ "url": "https://i.ytimg.com/vi/dQw4w9WgXcQ/hq.jpg" }] },
                { "id": "bad", "title": "too short id" },
                { "id": "abcdefghijk", "title": "  ", "duration": 10.0 }
            ]
        })
        .to_string();

        let tracks = parse_entries(&json, 10);
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].video_id, "dQw4w9WgXcQ");
        assert_eq!(tracks[0].title, "Nightcall");
        assert_eq!(tracks[0].artist, "Kavinsky");
        assert_eq!(tracks[0].duration_secs, Some(258));
        assert!(tracks[0].source.is_empty());
    }

    #[test]
    fn ignores_oversized_and_malformed_input() {
        assert!(parse_entries("not json", 5).is_empty());
        let json = serde_json::json!({
            "entries": [{ "id": "abcdefghijk", "title": "radio", "duration": 40000.0 }]
        })
        .to_string();
        let tracks = parse_entries(&json, 5);
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].duration_secs, None);
    }
}
