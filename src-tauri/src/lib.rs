mod event_bridge;
mod online;
mod state;

pub mod identity {
    pub const DATA_SUBDIR: &str = "musico-river";
    pub fn data_dir() -> Option<std::path::PathBuf> {
        dirs::data_dir().map(|d| d.join(DATA_SUBDIR))
    }
}

pub mod config {
    pub struct AppSettings {
        pub online_cache_mb: u64,
        pub download_quality: String,
        pub streaming_quality: String,
    }
    impl AppSettings {
        pub fn load() -> Self {
            Self {
                online_cache_mb: 64,
                download_quality: "standard".into(),
                streaming_quality: "standard".into(),
            }
        }
    }
}

pub mod scanner {
    pub fn cover_path(_song_id: &str) -> std::path::PathBuf {
        std::path::PathBuf::new()
    }
}

use std::sync::{Arc, Mutex};
use musico_playback::PlaybackEngine;
use state::{AppState, RiverPhase, RiverState, TrackMeta};
use tauri::{AppHandle, Emitter, Manager};

#[tauri::command]
async fn search_tracks(query: String) -> Result<Vec<TrackMeta>, String> {
    online::search_top_5(&query).await
}

#[tauri::command]
async fn start_river(app: AppHandle, track: TrackMeta) -> Result<(), String> {
    let state = app.state::<AppState>();
    {
        let mut r = state.river.lock().unwrap();
        r.reset_all();
        r.phase = RiverPhase::PlayingOriginalFirst;
        r.original_track = Some(track.clone());
        r.current_track = Some(track.clone());
        r.remember_played(&track.id);
    }

    let info = online::resolve_song_info(&track.id).await?;
    state.engine.play(info).map_err(|e| e.to_string())?;
    let _ = app.emit("river-track-changed", &track);

    let recent = state
        .river
        .lock()
        .unwrap()
        .recent_ids
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    if let Ok(similar_5) = online::fetch_similar_5(&track, &recent).await {
        let mut r = state.river.lock().unwrap();
        r.queue.extend(similar_5);
    }
    Ok(())
}

#[tauri::command]
async fn continue_river(app: AppHandle) -> Result<(), String> {
    let state = app.state::<AppState>();
    let (orig, recent) = {
        let mut r = state.river.lock().unwrap();
        r.phase = RiverPhase::PlayingSimilarBatch1;
        (
            r.original_track.clone(),
            r.recent_ids.iter().cloned().collect::<Vec<_>>(),
        )
    };

    if let Some(orig_track) = orig {
        let similar_5 = online::fetch_similar_5(&orig_track, &recent).await?;
        let first_track = {
            let mut r = state.river.lock().unwrap();
            r.queue.extend(similar_5);
            let first = r.queue.pop_front();
            if let Some(ref f) = first {
                r.current_track = Some(f.clone());
                r.remember_played(&f.id);
            }
            first
        };
        if let Some(first) = first_track {
            let _ = app.emit("river-track-changed", &first);
            let info = online::resolve_song_info(&first.id).await?;
            state.engine.play(info).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

#[tauri::command]
fn stop_river(app: AppHandle) -> Result<(), String> {
    let state = app.state::<AppState>();
    let _ = state.engine.stop();
    state.river.lock().unwrap().reset_all();
    Ok(())
}

#[tauri::command]
fn toggle_pause(app: AppHandle) -> Result<bool, String> {
    let state = app.state::<AppState>();
    let status = state.engine.state().status;
    if status == musico_playback::PlaybackStatus::Paused {
        state.engine.resume().map_err(|e| e.to_string())?;
        Ok(false)
    } else {
        state.engine.pause().map_err(|e| e.to_string())?;
        Ok(true)
    }
}

#[tauri::command]
fn set_volume(app: AppHandle, volume: f32) -> Result<(), String> {
    let state = app.state::<AppState>();
    state
        .engine
        .set_volume(volume.clamp(0.0, 1.0))
        .map_err(|e| e.to_string())
}

pub fn run() {
    env_logger::init();
    let engine = PlaybackEngine::new().expect("Failed to initialize audio engine");

    tauri::Builder::default()
        .manage(AppState {
            engine,
            river: Arc::new(Mutex::new(RiverState::new())),
        })
        .setup(|app| {
            event_bridge::spawn_event_bridge(app.handle().clone());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            search_tracks,
            start_river,
            continue_river,
            stop_river,
            toggle_pause,
            set_volume
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
