use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use cpal::traits::{DeviceTrait, StreamTrait};

use crossbeam_channel::Sender;

use crate::error::Result;

/// Start capturing audio from the given device.
///
/// Audio is captured on CoreAudio's real-time callback thread. Each callback's
/// samples are converted to interleaved i16 stereo and forwarded as one chunk
/// via the crossbeam channel (mono input is duplicated to both channels).
///
/// Returns the cpal Stream handle. Audio flows while the stream is alive.
pub fn start_capture(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    packet_tx: Sender<Vec<i16>>,
    stop: Arc<AtomicBool>,
) -> Result<cpal::Stream> {
    let channels = config.channels as usize;
    let error_callback = |err| {
        tracing::error!("Capture error: {}", err);
    };

    let stream = device.build_input_stream(
        config,
        move |data: &[f32], _info: &cpal::InputCallbackInfo| {
            if stop.load(Ordering::Relaxed) {
                return;
            }

            // Convert f32 to i16 and accumulate into packet-sized chunks
            let frames = data.len() / channels;
            let mut buffer = Vec::with_capacity(frames * 2);

            // Handle mono → stereo expansion, or stereo pass-through
            if channels == 1 {
                for sample in data.iter().take(frames) {
                    let s = (*sample * 32767.0).clamp(-32768.0, 32767.0) as i16;
                    buffer.push(s);    // L
                    buffer.push(s);    // R (duplicate mono)
                }
            } else {
                // Stereo (or more — use first two channels)
                for frame in data.chunks(channels) {
                    let l = (frame[0] * 32767.0).clamp(-32768.0, 32767.0) as i16;
                    let r = if channels > 1 {
                        (frame[1] * 32767.0).clamp(-32768.0, 32767.0) as i16
                    } else {
                        l
                    };
                    buffer.push(l);
                    buffer.push(r);
                }
            }

            // Send to network thread; non-blocking — drop if full
            let _ = packet_tx.try_send(buffer);
        },
        error_callback,
        None,
    )?;

    stream.play()?;
    Ok(stream)
}
