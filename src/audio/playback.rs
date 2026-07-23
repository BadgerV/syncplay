use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use cpal::traits::{DeviceTrait, StreamTrait};
use parking_lot::Mutex;

use crate::state::shared::{JitterBuffer, PlayoutGate, SharedRatio};
use crate::sync::clock::now_us;

/// Start audio playback through the given output device.
///
/// The output callback pulls interleaved i16 stereo samples from the jitter
/// buffer, resamples them by the current sync `ratio` (input frames consumed
/// per output frame — driven by the sync controller), applies `volume`, and
/// writes f32 samples to the device.
///
/// The `gate` holds the output silent — *without* draining the jitter buffer —
/// until its scheduled start instant, so playback begins at the same moment on
/// every endpoint.
pub fn start_playback(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    jitter: Arc<JitterBuffer>,
    ratio: Arc<SharedRatio>,
    volume: Arc<AtomicU32>,
    gate: Arc<PlayoutGate>,
    stop: Arc<AtomicBool>,
) -> Result<cpal::Stream, crate::error::SyncPlayError> {
    let channels = config.channels as usize;
    let resampler = Mutex::new(StereoResampler::new());

    let stream = device.build_output_stream(
        config,
        move |output: &mut [f32], _info: &cpal::OutputCallbackInfo| {
            if stop.load(Ordering::Relaxed) {
                output.fill(0.0);
                return;
            }

            // Hold silent until the shared playout deadline. Crucially we do NOT
            // touch the jitter buffer here, so it primes while we wait.
            if !gate.should_play(now_us()) {
                output.fill(0.0);
                return;
            }

            let vol = f32::from_bits(volume.load(Ordering::Relaxed));
            let r = ratio.get().clamp(0.98, 1.02);

            let mut res = resampler.lock();
            res.fill(&jitter, r, output, channels.max(1), vol);
        },
        |err| tracing::error!("Playback error: {}", err),
        None,
    )?;

    stream.play()?;
    Ok(stream)
}

// ─── Stereo Resampler ───────────────────────────────────

/// Stateful linear-interpolation resampler for a fixed-output audio callback.
///
/// `ratio` is the number of input frames consumed per output frame. Values
/// slightly above/below 1.0 speed up / slow down playback to keep the jitter
/// buffer at its target fill level. A linear resampler is more than adequate
/// for the ±0.2% corrections the sync controller applies, and — unlike a
/// fixed-input FFT/sinc resampler — it maps cleanly onto the "produce exactly
/// N output frames" contract of the CoreAudio callback.
pub struct StereoResampler {
    /// Un-consumed input frames carried across callbacks (f32 L/R pairs).
    residual: VecDeque<(f32, f32)>,
    /// Fractional position between `cur` and `next` in [0.0, 1.0).
    frac: f64,
    /// The two input frames straddling the current read position.
    cur: (f32, f32),
    next: (f32, f32),
    primed: bool,
}

impl StereoResampler {
    pub fn new() -> Self {
        Self {
            residual: VecDeque::new(),
            frac: 0.0,
            cur: (0.0, 0.0),
            next: (0.0, 0.0),
            primed: false,
        }
    }

    /// Fill `output` (interleaved, `channels`-wide) with `output.len() / channels`
    /// resampled frames pulled from `jitter`.
    pub fn fill(
        &mut self,
        jitter: &JitterBuffer,
        ratio: f64,
        output: &mut [f32],
        channels: usize,
        volume: f32,
    ) {
        let out_frames = output.len() / channels;

        // Top up the residual so we never pop the jitter buffer more than once
        // per callback. Missing samples come back as silence (underrun).
        let need = (out_frames as f64 * ratio).ceil() as usize + 4;
        if self.residual.len() < need {
            let deficit = need - self.residual.len();
            let mut tmp = vec![0i16; deficit * 2];
            jitter.pop_samples(&mut tmp); // zero-fills the tail on underrun
            for f in 0..deficit {
                let l = tmp[f * 2] as f32 / 32768.0;
                let r = tmp[f * 2 + 1] as f32 / 32768.0;
                self.residual.push_back((l, r));
            }
        }

        if !self.primed {
            self.cur = self.pop_input();
            self.next = self.pop_input();
            self.primed = true;
        }

        for i in 0..out_frames {
            let t = self.frac as f32;
            let l = (self.cur.0 + (self.next.0 - self.cur.0) * t) * volume;
            let r = (self.cur.1 + (self.next.1 - self.cur.1) * t) * volume;

            let idx = i * channels;
            output[idx] = l.clamp(-1.0, 1.0);
            if channels >= 2 {
                output[idx + 1] = r.clamp(-1.0, 1.0);
            }

            self.frac += ratio;
            while self.frac >= 1.0 {
                self.cur = self.next;
                self.next = self.pop_input();
                self.frac -= 1.0;
            }
        }
    }

    fn pop_input(&mut self) -> (f32, f32) {
        self.residual.pop_front().unwrap_or((0.0, 0.0))
    }
}

impl Default for StereoResampler {
    fn default() -> Self {
        Self::new()
    }
}
