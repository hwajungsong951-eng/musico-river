use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager};
use musico_playback::PlaybackEvent;
use crate::state::{AppState, RiverPhase, TrackMeta};
use crate::online;

pub fn spawn_event_bridge(app: AppHandle) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_millis(50));
            let state = app.state::<AppState>();

            for event in state.engine.poll_events() {
                if let PlaybackEvent::SongEnded { .. } = event {
                    handle_track_ended(&app);
                }
            }
        }
    });
}

fn handle_track_ended(app: &AppHandle) {
    let state = app.state::<AppState>();
    let mut river = state.river.lock().unwrap();

    let next_track: Option<TrackMeta> = match river.phase {
        RiverPhase::PlayingOriginalFirst => {
            river.phase = RiverPhase::PlayingSimilarBatch1;
            river.queue.pop_front()
        }
        RiverPhase::PlayingSimilarBatch1 => {
            if let Some(t) = river.queue.pop_front() {
                Some(t)
            } else {
                river.phase = RiverPhase::PlayingOriginalReturn;
                river.original_track.clone()
            }
        }
        RiverPhase::PlayingOriginalReturn => {
            river.phase = RiverPhase::PlayingSimilarBatch2;
            river.queue.pop_front()
        }
        RiverPhase::PlayingSimilarBatch2 => {
            if let Some(t) = river.queue.pop_front() {
                Some(t)
            } else {
                river.phase = RiverPhase::WaitingDecision;
                river.current_track = None;
                let _ = app.emit("river-waiting", ());
                return;
            }
        }
        _ => return,
    };

    if let Some(track) = next_track {
        river.current_track = Some(track.clone());
        river.remember_played(&track.id);
        let prefetched = river.prefetched_next.take();
        let next_for_prefetch = river.queue.front().cloned();
        let original_for_refill = if river.phase == RiverPhase::PlayingOriginalReturn {
            river.original_track.clone()
        } else {
            None
        };
        let recent_ids: Vec<String> = river.recent_ids.iter().cloned().collect();
        drop(river);

        let _ = app.emit("river-track-changed", &track);

        let app_clone = app.clone();
        tauri::async_runtime::spawn(async move {
            let song_info = match prefetched {
                Some((id, info)) if id == track.id => Ok(info),
                _ => online::resolve_song_info(&track.id).await,
            };

            if let Ok(info) = song_info {
                let _ = app_clone.state::<AppState>().engine.play(info);
            }

            if let Some(orig) = original_for_refill {
                if let Ok(new_batch) = online::fetch_similar_5(&orig, &recent_ids).await {
                    let state_ref = app_clone.state::<AppState>();
                    let mut r = state_ref.river.lock().unwrap();
                    r.queue.extend(new_batch);
                }
            }

            if let Some(next_t) = next_for_prefetch {
                if let Ok(info) = online::resolve_song_info(&next_t.id).await {
                    let state_ref = app_clone.state::<AppState>();
                    let mut r = state_ref.river.lock().unwrap();
                    r.prefetched_next = Some((next_t.id, info));
                }
            }
        });
    }
}
