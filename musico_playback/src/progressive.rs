//! # Progressive (play-while-downloading) sources
//!
//! An online track no longer has to land completely in the cache before the
//! decoder may open it.  The downloader writes into an in-flight file and
//! publishes its progress here; [`AudioDecoder`](crate::decoder::AudioDecoder)
//! notices the registration and reads through a [`GrowingFile`], which blocks
//! (bounded) whenever the decoder catches up with the network.
//!
//! Life cycle, all keyed by the in-flight path:
//!
//! 1. [`register`] — the downloader creates the file and announces it.
//! 2. [`Progress::advance`] after every chunk written.
//! 3. [`Progress::finish`] (renamed onto `final_path`) or [`Progress::fail`].
//!
//! After `finish`, [`redirect`] maps the vanished in-flight path to the final
//! cache file, so replay/previous/crossfade pre-open of the same `SongInfo`
//! keep working.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The reader gives up when the download makes no progress for this long.
const STALL_TIMEOUT: Duration = Duration::from_secs(20);
/// Poll granularity while waiting (also bounds missed-wakeup latency).
const WAIT_SLICE: Duration = Duration::from_millis(100);

const RUNNING: u8 = 0;
const DONE: u8 = 1;
const FAILED: u8 = 2;

/// Download progress shared between the writer and any number of readers.
#[derive(Debug)]
pub struct Progress {
    written: AtomicU64,
    /// Expected total size; 0 = unknown (no Content-Length).
    total: AtomicU64,
    state: AtomicU8,
    lock: Mutex<()>,
    cv: Condvar,
    /// When the writer last advanced (or registration time for a never-filled
    /// download).  Used to detect a stalled body read that no timeout covers.
    last_advance: Mutex<Instant>,
}

impl Progress {
    fn new(total: Option<u64>) -> Self {
        Self {
            written: AtomicU64::new(0),
            total: AtomicU64::new(total.unwrap_or(0)),
            state: AtomicU8::new(RUNNING),
            lock: Mutex::new(()),
            cv: Condvar::new(),
            last_advance: Mutex::new(Instant::now()),
        }
    }

    fn notify(&self) {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.cv.notify_all();
    }

    /// Bytes now durable in the in-flight file.
    pub fn advance(&self, written: u64) {
        self.written.store(written, Ordering::Release);
        *self.last_advance.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
        self.notify();
    }

    /// Time since the writer last made progress (or since registration).
    /// A blocked `read()` cannot time out on its own, so callers use this to
    /// detect and fail a stalled download.
    pub fn idle_for(&self) -> Duration {
        self.last_advance
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed()
    }

    /// Download complete; the file was renamed to `final_path` (the reader's
    /// open handle stays valid across the rename).
    pub fn finish(&self, inflight: &Path, final_path: &Path) {
        let w = self.written.load(Ordering::Acquire);
        self.total.store(w, Ordering::Release);
        self.state.store(DONE, Ordering::Release);
        let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
        reg.active.remove(inflight);
        reg.redirects.insert(inflight.to_path_buf(), final_path.to_path_buf());
        drop(reg);
        self.notify();
    }

    /// Download failed; readers get an I/O error instead of a truncated song.
    pub fn fail(&self, inflight: &Path) {
        self.state.store(FAILED, Ordering::Release);
        registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .remove(inflight);
        self.notify();
    }

    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == RUNNING
    }

    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Acquire)
    }

    fn total(&self) -> Option<u64> {
        match self.total.load(Ordering::Acquire) {
            0 => None,
            n => Some(n),
        }
    }
}

#[derive(Default)]
struct Registry {
    active: HashMap<PathBuf, Arc<Progress>>,
    redirects: HashMap<PathBuf, PathBuf>,
}

fn registry() -> &'static Mutex<Registry> {
    static REG: OnceLock<Mutex<Registry>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(Registry::default()))
}

/// Announces an in-flight download at `path`.
pub fn register(path: &Path, total: Option<u64>) -> Arc<Progress> {
    let p = Arc::new(Progress::new(total));
    let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    reg.redirects.remove(path);
    reg.active.insert(path.to_path_buf(), p.clone());
    p
}

/// The running download at `path`, if any.
pub fn lookup(path: &Path) -> Option<Arc<Progress>> {
    registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .active
        .get(path)
        .cloned()
}

/// Final cache path for a finished in-flight path (identity otherwise).
pub fn redirect(path: &Path) -> PathBuf {
    registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .redirects
        .get(path)
        .cloned()
        .unwrap_or_else(|| path.to_path_buf())
}

/// A file that is still being written.  Reads past the written end block
/// until more data arrives, the download completes (EOF) or fails (error).
pub struct GrowingFile {
    file: File,
    pos: u64,
    progress: Arc<Progress>,
}

impl GrowingFile {
    pub fn open(path: &Path, progress: Arc<Progress>) -> io::Result<Self> {
        Ok(Self { file: File::open(path)?, pos: 0, progress })
    }

    /// Waits until `want` bytes are available or the download settles.
    /// Returns the number of bytes readable from `self.pos`.
    fn wait_for(&self, want: u64) -> io::Result<u64> {
        let mut last_written = self.progress.written();
        let mut last_change = Instant::now();
        loop {
            let written = self.progress.written();
            if written >= want {
                return Ok(written.saturating_sub(self.pos));
            }
            match self.progress.state.load(Ordering::Acquire) {
                DONE => return Ok(written.saturating_sub(self.pos)),
                FAILED => {
                    return Err(io::Error::new(io::ErrorKind::Other, "stream download failed"))
                }
                _ => {}
            }
            if written != last_written {
                last_written = written;
                last_change = Instant::now();
            } else if last_change.elapsed() >= STALL_TIMEOUT {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "stream download stalled"));
            }
            let g = self.progress.lock.lock().unwrap_or_else(|e| e.into_inner());
            let _ = self.progress.cv.wait_timeout(g, WAIT_SLICE);
        }
    }
}

impl Read for GrowingFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let avail = self.wait_for(self.pos + 1)?;
        if avail == 0 {
            return Ok(0); // finished and fully consumed
        }
        let n = (buf.len() as u64).min(avail) as usize;
        self.file.seek(SeekFrom::Start(self.pos))?;
        let got = self.file.read(&mut buf[..n])?;
        self.pos += got as u64;
        Ok(got)
    }
}

impl Seek for GrowingFile {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let target = match to {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::Current(d) => self.pos as i128 + d as i128,
            SeekFrom::End(d) => {
                let total = match self.progress.total() {
                    Some(t) => t,
                    None => {
                        // Unknown length: only resolvable once complete.
                        self.wait_for(u64::MAX)?;
                        self.progress.written()
                    }
                };
                total as i128 + d as i128
            }
        };
        if target < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek before start"));
        }
        // Seeking itself never blocks; the next read waits for the bytes.
        self.pos = target as u64;
        Ok(self.pos)
    }
}

impl symphonia::core::io::MediaSource for GrowingFile {
    fn is_seekable(&self) -> bool {
        // Seeks into not-yet-downloaded ranges block in read(), which is
        // acceptable; without a known length symphonia can't seek from End.
        self.progress.total().is_some() || !self.progress.is_running()
    }

    fn byte_len(&self) -> Option<u64> {
        self.progress.total()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn idle_time_tracks_writer_progress() {
        let p = Progress::new(None);
        std::thread::sleep(Duration::from_millis(120));
        assert!(
            p.idle_for() >= Duration::from_millis(100),
            "idle must grow while the writer is blocked"
        );
        p.advance(64);
        assert!(
            p.idle_for() < Duration::from_millis(100),
            "advance resets the stall clock"
        );
    }

    #[test]
    fn reader_follows_writer_and_survives_rename() {
        let dir = std::env::temp_dir().join(format!("musico-prog-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let inflight = dir.join("a.inflight.m4a");
        let final_path = dir.join("a.m4a");
        let mut w = File::create(&inflight).unwrap();
        let prog = register(&inflight, Some(8));
        w.write_all(b"abcd").unwrap();
        prog.advance(4);

        let mut r = GrowingFile::open(&inflight, lookup(&inflight).unwrap()).unwrap();
        let writer = {
            let prog = prog.clone();
            let inflight = inflight.clone();
            let final_path = final_path.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(150));
                w.write_all(b"efgh").unwrap();
                w.flush().unwrap();
                drop(w);
                prog.advance(8);
                std::fs::rename(&inflight, &final_path).unwrap();
                prog.finish(&inflight, &final_path);
            })
        };
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        writer.join().unwrap();
        assert_eq!(out, b"abcdefgh");
        assert_eq!(redirect(&inflight), final_path);
        assert!(lookup(&inflight).is_none());
        drop(r);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_download_surfaces_an_error() {
        let dir = std::env::temp_dir().join(format!("musico-progf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let inflight = dir.join("b.inflight.mp3");
        File::create(&inflight).unwrap();
        let prog = register(&inflight, None);
        let mut r = GrowingFile::open(&inflight, prog.clone()).unwrap();
        prog.fail(&inflight);
        let mut buf = [0u8; 4];
        assert!(r.read(&mut buf).is_err());
        drop(r);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
