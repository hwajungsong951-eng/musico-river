use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use musico_playback::{PlaybackEngine, SongInfo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RiverPhase {
    Idle,
    PlayingOriginalFirst,
    PlayingSimilarBatch1,
    PlayingOriginalReturn,
    PlayingSimilarBatch2,
    WaitingDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackMeta {
    pub id: String,
    pub title: String,
    pub artist: String,
}

pub struct RiverState {
    pub phase: RiverPhase,
    pub original_track: Option<TrackMeta>,
    pub queue: VecDeque<TrackMeta>,
    pub current_track: Option<TrackMeta>,
    pub recent_ids: VecDeque<String>,
    pub prefetched_next: Option<(String, SongInfo)>,
}

impl RiverState {
    pub fn new() -> Self {
        Self {
            phase: RiverPhase::Idle,
            original_track: None,
            queue: VecDeque::with_capacity(5),
            current_track: None,
            recent_ids: VecDeque::with_capacity(30),
            prefetched_next: None,
        }
    }

    pub fn remember_played(&mut self, id: &str) {
        if self.recent_ids.len() >= 30 {
            self.recent_ids.pop_front();
        }
        if !self.recent_ids.iter().any(|x| x == id) {
            self.recent_ids.push_back(id.to_string());
        }
    }

    pub fn reset_all(&mut self) {
        self.phase = RiverPhase::Idle;
        self.original_track = None;
        self.queue.clear();
        self.current_track = None;
        self.recent_ids.clear();
        self.prefetched_next = None;
    }
}

pub struct AppState {
    pub engine: PlaybackEngine,
    pub river: Arc<Mutex<RiverState>>,
}
