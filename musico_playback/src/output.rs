//! CPAL output stream and ring buffer bridge.
//!
//! Sets up the audio device, creates a lock-free ring buffer, and runs
//! the CPAL callback that pops samples and applies volume scaling.
//!
//! The callback also tracks how many samples it has actually *played*, so
//! the reported position matches what the listener hears instead of how far
//! the decoder has run ahead.

use crate::error::PlaybackError;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::{HeapConsumer, HeapProducer, HeapRb};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Manages the CPAL output stream and the producer half of the ring buffer.
///
/// The consumer half lives inside the CPAL audio callback.
pub struct AudioOutput {
    /// Kept alive — dropping the stream silences output.
    _stream: cpal::Stream,
    /// Name of the device this stream was opened on: lets the decoder thread
    /// notice a Windows default-device switch and reopen before the next song.
    device_name: String,
    /// Producer side: the decoder thread pushes samples here.
    producer: HeapProducer<f32>,
    /// Device sample rate in Hz.
    sample_rate: u32,
    /// Number of output channels.
    channels: usize,
    /// Shared volume as an `AtomicU32` (bit-cast f32). The CPAL callback
    /// reads this without locking.
    volume: Arc<AtomicU32>,
    /// Shared mute flag.
    muted: Arc<AtomicBool>,
    /// Shared flush flag — when set, the CPAL callback discards all buffered
    /// samples and clears the flag. Used for instant seek/stop.
    flush_requested: Arc<AtomicBool>,
    /// Shared pause gate — when set, the callback outputs silence and leaves
    /// the ring buffer untouched, so pause is instant and resume is seamless.
    paused: Arc<AtomicBool>,
    /// Total samples the callback has actually consumed (not counting flush
    /// discards). `played_secs()` derives the audible position from this.
    played_samples: Arc<AtomicU64>,
}

impl AudioOutput {
    /// Initialises the default audio output device and stream.
    ///
    /// Creates a ring buffer of ~1 second capacity and starts the stream.
    pub fn new() -> Result<Self, PlaybackError> {
        Self::open(false)
    }

    /// Opens a stream on the current default device.  `quiet` keeps the
    /// info-level "Audio device" line for speculative candidates (a probe that
    /// may be rejected on a format mismatch) so the log is not flooded.
    fn open(quiet: bool) -> Result<Self, PlaybackError> {
        let host = cpal::default_host();
        let device = host
            .default_output_device()
            .ok_or(PlaybackError::NoOutputDevice)?;

        let device_name = device.name().unwrap_or_default();
        if quiet {
            log::debug!("Audio device (candidate): {device_name:?}");
        } else {
            log::info!("Audio device: {device_name:?}");
        }

        let supported = device
            .supported_output_configs()
            .map_err(|e| PlaybackError::StreamBuild(format!("{e}")))?;

        // Collect configs, prefer f32.
        let configs: Vec<_> = supported.collect();
        let chosen = configs
            .iter()
            .find(|c| c.sample_format() == cpal::SampleFormat::F32)
            .or_else(|| configs.iter().find(|c| c.sample_format() == cpal::SampleFormat::I16))
            .or_else(|| configs.first())
            .ok_or_else(|| PlaybackError::StreamBuild("no supported config".into()))?;

        // Choose sample rate: prefer 48000, then 44100, then device default.
        let min_sr = chosen.min_sample_rate().0;
        let max_sr = chosen.max_sample_rate().0;
        let sr = if (min_sr..=max_sr).contains(&48000) {
            48000
        } else if (min_sr..=max_sr).contains(&44100) {
            44100
        } else {
            max_sr
        };

        let config = chosen.with_sample_rate(cpal::SampleRate(sr));
        let channels = config.channels() as usize;
        let sample_rate = sr;

        log::info!("Output config: {sample_rate} Hz, {channels} ch, {:?}", config.sample_format());

        // Ring buffer: 1 second of audio. Small enough that seek/stop flushes
        // quickly, large enough to ride out scheduler hiccups.
        let buf_size = (sample_rate as usize) * channels;
        let rb = HeapRb::<f32>::new(buf_size);
        let (producer, consumer) = rb.split();

        let volume = Arc::new(AtomicU32::new(f32::to_bits(1.0)));
        let muted = Arc::new(AtomicBool::new(false));
        let flush_requested = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let played_samples = Arc::new(AtomicU64::new(0));

        let vol_clone = Arc::clone(&volume);
        let muted_clone = Arc::clone(&muted);
        let flush_clone = Arc::clone(&flush_requested);
        let paused_clone = Arc::clone(&paused);
        let played_clone = Arc::clone(&played_samples);

        let stream = Self::build_stream(
            &device, &config.into(), consumer,
            vol_clone, muted_clone, flush_clone, paused_clone, played_clone,
        )?;
        stream.play().map_err(|e| PlaybackError::StreamBuild(format!("{e}")))?;

        Ok(Self {
            _stream: stream,
            device_name,
            producer,
            sample_rate,
            channels,
            volume,
            muted,
            flush_requested,
            paused,
            played_samples,
        })
    }

    /// Returns a mutable reference to the ring buffer producer.
    pub fn producer(&mut self) -> &mut HeapProducer<f32> {
        &mut self.producer
    }

    /// Device sample rate in Hz.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Name of the device this output is currently bound to.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Reopens the stream on the current system default device, preserving
    /// volume, mute, the pause gate and the audible-position counter.
    ///
    /// Returns the new device name on success.  If the new device's format
    /// differs from the one the decoder is producing, the swap is refused and
    /// the current stream keeps running (a mid-song sample-rate change needs a
    /// full re-init, not a stream swap).
    pub fn try_reopen_matching(&mut self, sample_rate: u32, channels: usize) -> Option<String> {
        let candidate = match AudioOutput::open(true) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("output reopen failed: {e}");
                return None;
            }
        };
        if candidate.sample_rate != sample_rate || candidate.channels != channels {
            // Expected outcome, not an error: the caller remembers the device
            // and stops probing it for the rest of the song.
            log::debug!(
                "default device format {} Hz/{} ch differs from the active {} Hz/{} ch",
                candidate.sample_rate, candidate.channels, sample_rate, channels
            );
            return None;
        }

        let name = candidate.device_name.clone();
        let fresh = candidate;
        fresh.volume.store(self.volume.load(Ordering::Relaxed), Ordering::Relaxed);
        fresh.muted.store(self.muted.load(Ordering::Relaxed), Ordering::Relaxed);
        fresh.paused.store(self.paused.load(Ordering::Relaxed), Ordering::Relaxed);
        fresh
            .played_samples
            .store(self.played_samples.load(Ordering::Relaxed), Ordering::Relaxed);
        *self = fresh;
        Some(name)
    }

    /// Name of the system default output device right now (None when the
    /// platform reports no default).
    pub fn default_device_name() -> Option<String> {
        let host = cpal::default_host();
        host.default_output_device()
            .and_then(|d| d.name().ok())
    }

    /// Number of output channels.
    pub fn channels(&self) -> usize {
        self.channels
    }

    /// Sets the volume (0.0–1.0) atomically for the CPAL callback.
    pub fn set_volume(&self, v: f32) {
        self.volume.store(f32::to_bits(v), Ordering::Relaxed);
    }

    /// Sets the muted flag atomically.
    pub fn set_muted(&self, m: bool) {
        self.muted.store(m, Ordering::Relaxed);
    }

    /// Requests a flush of the ring buffer. The CPAL callback will discard
    /// all buffered samples on its next invocation.
    pub fn request_flush(&self) {
        self.flush_requested.store(true, Ordering::Release);
    }

    /// Requests a flush and waits until the callback has discarded the
    /// buffered samples (flush discards are instant, so this takes at most
    /// one callback period). Returns `false` on timeout.
    pub fn flush_now(&self, timeout: Duration) -> bool {
        self.request_flush();
        self.wait_drain(timeout)
    }

    /// Gates the audio callback. While paused it outputs silence without
    /// consuming the ring buffer — pause is instant, and resume continues
    /// from exactly the buffered position.
    pub fn set_paused(&self, p: bool) {
        self.paused.store(p, Ordering::Release);
    }

    /// Seconds of audio the listener has actually heard since the last
    /// `reset_played()`.
    pub fn played_secs(&self) -> f32 {
        self.played_samples.load(Ordering::Relaxed) as f32
            / (self.sample_rate as f32 * self.channels as f32)
    }

    /// Resets the played-sample counter (song start / seek base).
    pub fn reset_played(&self) {
        self.played_samples.store(0, Ordering::Release);
    }

    /// Blocks until the callback has drained the ring buffer (song tail,
    /// seek flush). Returns `false` on timeout.
    pub fn wait_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while !self.producer.is_empty() {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        true
    }

    fn build_stream(
        device: &cpal::Device,
        config: &cpal::StreamConfig,
        mut consumer: HeapConsumer<f32>,
        volume: Arc<AtomicU32>,
        muted: Arc<AtomicBool>,
        flush_requested: Arc<AtomicBool>,
        paused: Arc<AtomicBool>,
        played_samples: Arc<AtomicU64>,
    ) -> Result<cpal::Stream, PlaybackError> {
        let stream = device
            .build_output_stream(
                config,
                move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                    // If a flush was requested (seek/stop), drain all stale
                    // samples. Flush discards do not count as played.
                    if flush_requested.compare_exchange(
                        true, false, Ordering::AcqRel, Ordering::Relaxed
                    ).is_ok() {
                        while consumer.pop().is_some() {}
                    }

                    // Pause gate: silence without consuming, so resume picks
                    // up exactly where the listener paused.
                    if paused.load(Ordering::Acquire) {
                        data.fill(0.0);
                        return;
                    }

                    let vol = if muted.load(Ordering::Relaxed) {
                        0.0
                    } else {
                        f32::from_bits(volume.load(Ordering::Relaxed))
                    };
                    let mut popped = 0usize;
                    for sample in data.iter_mut() {
                        *sample = match consumer.pop() {
                            Some(s) => {
                                popped += 1;
                                s * vol
                            }
                            None => 0.0, // underrun — fill silence
                        };
                    }
                    if popped > 0 {
                        played_samples.fetch_add(popped as u64, Ordering::Relaxed);
                    }
                },
                |err| log::error!("CPAL stream error: {err}"),
                None,
            )
            .map_err(|e| PlaybackError::StreamBuild(format!("{e}")))?;
        Ok(stream)
    }
}
