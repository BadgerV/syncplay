//! System audio capture via ScreenCaptureKit — the Airfoil-style "grab what's
//! playing on this Mac" source, with no BlackHole / Multi-Output Device setup.
//!
//! Captures the whole system's audio mix (every app) as 48 kHz stereo, converts
//! it to the interleaved-i16 packets the rest of the pipeline expects, and feeds
//! them into the same channel a device/tone source uses.
//!
//! ## Permission
//! ScreenCaptureKit audio requires **Screen Recording** permission (System
//! Settings → Privacy & Security → Screen Recording) granted to this binary /
//! the terminal launching it. Without it, [`start_system_capture`] fails with a
//! clear error.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crossbeam_channel::Sender;
use screencapturekit::cm::AudioBuffer;
use screencapturekit::prelude::*;

use crate::error::{Result, SyncPlayError};

/// Output handler invoked on ScreenCaptureKit's dispatch queue with each audio
/// sample buffer. Converts to interleaved-stereo i16 and forwards to the sender
/// pipeline via `tx`.
struct AudioTap {
    tx: Sender<Vec<i16>>,
}

impl SCStreamOutputTrait for AudioTap {
    fn did_output_sample_buffer(&self, sample: CMSampleBuffer, of_type: SCStreamOutputType) {
        if of_type != SCStreamOutputType::Audio {
            return;
        }
        let Some(list) = sample.audio_buffer_list() else {
            return;
        };

        let out = match list.num_buffers() {
            0 => return,
            // Non-interleaved (planar) Float32: one buffer per channel.
            n if n >= 2 => {
                let left = list.get(0).map(buffer_f32).unwrap_or(&[]);
                let right = list.get(1).map(buffer_f32).unwrap_or(&[]);
                let frames = left.len().min(right.len());
                let mut v = Vec::with_capacity(frames * 2);
                for i in 0..frames {
                    v.push(to_i16(left[i]));
                    v.push(to_i16(right[i]));
                }
                v
            }
            // Single buffer: interleaved stereo, or mono to be duplicated.
            _ => {
                let Some(buf) = list.get(0) else {
                    return;
                };
                let samples = buffer_f32(buf);
                if buf.number_channels >= 2 {
                    samples.iter().map(|&s| to_i16(s)).collect()
                } else {
                    let mut v = Vec::with_capacity(samples.len() * 2);
                    for &s in samples {
                        let x = to_i16(s);
                        v.push(x); // L
                        v.push(x); // R
                    }
                    v
                }
            }
        };

        if !out.is_empty() {
            // Non-blocking: if the pipeline is momentarily full, drop rather than
            // stall ScreenCaptureKit's real-time audio thread.
            let _ = self.tx.try_send(out);
        }
    }
}

/// Reinterpret an audio buffer's bytes as `f32` samples. ScreenCaptureKit
/// delivers Float32 PCM; CoreAudio buffers are suitably aligned.
fn buffer_f32(buf: &AudioBuffer) -> &[f32] {
    let bytes = buf.data();
    let len = bytes.len() / std::mem::size_of::<f32>();
    if len == 0 {
        return &[];
    }
    // SAFETY: ScreenCaptureKit hands back Float32 PCM in CoreAudio-allocated
    // buffers, which are aligned to at least `align_of::<f32>()`, and `len` is
    // floored to whole f32s so we never read past `bytes`.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f32, len) }
}

/// Convert a normalized `[-1.0, 1.0]` float sample to i16.
fn to_i16(s: f32) -> i16 {
    (s.clamp(-1.0, 1.0) * 32767.0) as i16
}

/// Start capturing the system audio mix. Returns the live `SCStream`, which the
/// caller must keep alive for the duration of the session (dropping it stops
/// capture). Audio flows into `tx` as interleaved-stereo i16 packets.
///
/// `_stop` is accepted for signature parity with the other capture sources;
/// capture stops when the returned stream is dropped.
pub fn start_system_capture(tx: Sender<Vec<i16>>, _stop: Arc<AtomicBool>) -> Result<SCStream> {
    let content = SCShareableContent::get().map_err(|e| {
        SyncPlayError::Config(format!(
            "ScreenCaptureKit unavailable — grant Screen Recording permission \
             (System Settings → Privacy & Security → Screen Recording): {e}"
        ))
    })?;

    let display = content.displays().into_iter().next().ok_or_else(|| {
        SyncPlayError::Config("no display available for system audio capture".into())
    })?;

    let filter = SCContentFilter::create()
        .with_display(&display)
        .with_excluding_windows(&[])
        .build();

    let config = SCStreamConfiguration::new()
        .with_captures_audio(true)
        .with_sample_rate(48_000)
        .with_channel_count(2)
        // Don't capture our own delayed monitor output — would feed back.
        .with_excludes_current_process_audio(true);

    let mut stream = SCStream::new(&filter, &config);
    stream.add_output_handler(AudioTap { tx }, SCStreamOutputType::Audio);
    stream
        .start_capture()
        .map_err(|e| SyncPlayError::Config(format!("failed to start system audio capture: {e}")))?;

    tracing::info!("System audio capture started (ScreenCaptureKit, 48kHz stereo)");
    Ok(stream)
}
