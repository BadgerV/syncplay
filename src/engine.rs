//! Runtime orchestration: turns the UI's "Start"/"Connect" intents into live
//! audio + network pipelines.
//!
//! ## Why dedicated engine threads?
//!
//! On macOS (CoreAudio) a `cpal::Stream` is `!Send`: it cannot be moved between
//! threads or stored in the shared `AppState` (which must stay `Send`). So each
//! mode runs on its own std thread that *builds and owns* its cpal stream and
//! keeps it alive for the lifetime of the session by running the blocking
//! network loop on the same thread. Tearing down is a single `AtomicBool`.

use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cpal::traits::DeviceTrait;
use crossbeam_channel::{bounded, Sender};

use crate::audio::capture::start_capture;
use crate::audio::device;
use crate::audio::playback::start_playback;
use crate::net::discovery::DiscoveryService;
use crate::net::receiver::ReceiverSession;
use crate::net::sender::{SenderMonitor, SenderSession};
use crate::state::shared::{
    DiscoveredSender, JitterBuffer, PlayoutGate, ReceiverThreads, SenderThreads, SharedApp,
    SharedRatio, AUDIO_PORT, CHANNELS, SAMPLE_RATE,
};
use crate::sync::clock::ClockSync;
use crate::sync::controller::run_sync_controller;

/// Where the sender pulls audio from.
pub enum AudioSource {
    /// Capture from an input device (empty string = system default).
    Device(String),
    /// Synthesize a sine test tone at the given frequency (Hz). Lets us verify
    /// end-to-end streaming without a loopback device like BlackHole.
    Tone(f32),
    /// Capture the whole system audio mix via ScreenCaptureKit — the
    /// "stream what's playing" source. Source keeps playing LIVE (not muted).
    System,
    /// Capture via a muted Core Audio process tap — Airfoil-grade: the source's
    /// live output is silenced, and the delayed monitor replays it in sync with
    /// receivers, so every Mac plays together. No BlackHole needed (macOS 14.4+).
    Tap,
}

/// Start the sender pipeline. Returns thread handles the UI stores in state.
///
/// `playout_delay_ms` is the synchronized-playout budget: the source plays its
/// own audio this many ms after capture, matching every remote receiver.
pub fn start_sender(
    shared: SharedApp,
    source: AudioSource,
    playout_delay_ms: u64,
    monitor_output: Option<String>,
) -> SenderThreads {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = stop.clone();
    let handle = std::thread::Builder::new()
        .name("syncplay-sender".into())
        .spawn(move || {
            sender_engine(
                shared,
                source,
                playout_delay_ms,
                monitor_output,
                stop_thread,
            )
        })
        .expect("failed to spawn sender engine thread");

    SenderThreads {
        stop,
        net_handle: Some(handle),
    }
}

fn sender_engine(
    shared: SharedApp,
    source: AudioSource,
    playout_delay_ms: u64,
    monitor_output: Option<String>,
    stop: Arc<AtomicBool>,
) {
    let session = match SenderSession::new() {
        Ok(s) => s,
        Err(e) => return fail_sender(&shared, &format!("cannot bind sender ports: {e}")),
    };

    let (tx, rx) = bounded::<Vec<i16>>(64);

    // System capture taps the audio mix but doesn't silence it — the source is
    // already playing live on this Mac's speakers. A delayed local monitor would
    // add a second, offset copy (echo), so we skip it for this source. Other
    // sources (tone, BlackHole/mic) are NOT otherwise audible, so the monitor is
    // what makes them play locally, in sync with receivers.
    let is_system = matches!(source, AudioSource::System);

    // Own the audio source for the whole session. Exactly one of these is set;
    // the binding stays in scope so the stream/thread lives until we return.
    let mut _capture: Option<cpal::Stream> = None;
    let mut _tone: Option<std::thread::JoinHandle<()>> = None;
    let mut _system: Option<screencapturekit::prelude::SCStream> = None;
    let mut _tap: Option<crate::audio::tapcapture::TapCapture> = None;

    match source {
        AudioSource::Tone(freq) => {
            tracing::info!("Sender source: {freq:.0} Hz test tone");
            _tone = Some(spawn_tone(freq as f64, 0.15, tx, stop.clone()));
        }
        AudioSource::Tap => match crate::audio::tapcapture::start_tap_capture(tx, stop.clone()) {
            Ok(t) => {
                tracing::info!("Sender source: muted process tap (Airfoil-style, no BlackHole)");
                _tap = Some(t);
            }
            Err(e) => return fail_sender(&shared, &format!("cannot start process tap: {e}")),
        },
        AudioSource::System => {
            match crate::audio::systemcapture::start_system_capture(tx, stop.clone()) {
                Ok(s) => {
                    tracing::info!("Sender source: system audio (ScreenCaptureKit)");
                    _system = Some(s);
                }
                Err(e) => {
                    return fail_sender(&shared, &format!("cannot start system capture: {e}"))
                }
            }
        }
        AudioSource::Device(input_name) => {
            let device = if input_name.is_empty() {
                device::default_input_device()
            } else {
                device::find_input_device(&input_name).or_else(device::default_input_device)
            };
            let device = match device {
                Some(d) => d,
                None => return fail_sender(&shared, "no input device available"),
            };
            let config = match device::input_config(&device) {
                Ok(c) => c,
                Err(e) => return fail_sender(&shared, &format!("input config error: {e}")),
            };
            match start_capture(&device, &config, tx, stop.clone()) {
                Ok(s) => {
                    tracing::info!(
                        "Sender source: '{}'",
                        device.name().unwrap_or_else(|_| "unknown".into())
                    );
                    _capture = Some(s);
                }
                Err(e) => return fail_sender(&shared, &format!("cannot start capture: {e}")),
            }
        }
    }

    // Advertise ourselves so receivers can discover us.
    let discovery = DiscoveryService::new().ok();
    if let Some(d) = &discovery {
        let host = local_ip_address().unwrap_or_else(|| "127.0.0.1".to_string());
        if let Err(e) =
            d.register_sender("SyncPlay Sender", &host, AUDIO_PORT, SAMPLE_RATE, CHANNELS)
        {
            tracing::warn!("mDNS registration failed: {e}");
        }
    }

    // ── Local delayed monitor: play the source's own audio at capture+budget so
    //    it comes out in sync with every remote receiver. Kept in scope for the
    //    whole session; failure to open output degrades gracefully to net-only.
    let budget_us = playout_delay_ms * 1000;
    let mut _monitor_stream: Option<cpal::Stream> = None;
    let mut _monitor_sync: Option<std::thread::JoinHandle<()>> = None;
    let monitor = if is_system {
        tracing::info!("Source plays live (system capture) — skipping delayed monitor");
        None
    } else {
        build_sender_monitor(
            budget_us,
            monitor_output,
            stop.clone(),
            shared.clone(),
            &mut _monitor_stream,
            &mut _monitor_sync,
        )
    };

    // Blocks until `stop` is set; the source + monitor stay alive in scope.
    session.run(rx, shared.clone(), stop.clone(), monitor);

    if let Some(d) = &discovery {
        d.unregister();
    }
    shared.lock().sender.is_streaming = false;
    tracing::info!("Sender engine stopped");
}

/// Spawn a thread that emits an interleaved-stereo sine test tone into the
/// packet channel, paced in real time at 480-frame (10 ms) chunks.
fn spawn_tone(
    freq: f64,
    amplitude: f32,
    tx: Sender<Vec<i16>>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("syncplay-tone".into())
        .spawn(move || {
            const FRAMES: usize = 480;
            let step = std::f64::consts::TAU * freq / SAMPLE_RATE as f64;
            let chunk = Duration::from_micros(FRAMES as u64 * 1_000_000 / SAMPLE_RATE as u64);
            let mut phase = 0.0f64;
            let mut next = Instant::now();

            while !stop.load(Ordering::Relaxed) {
                let mut buf = Vec::with_capacity(FRAMES * 2);
                for _ in 0..FRAMES {
                    let s = (phase.sin() as f32 * amplitude * 32767.0) as i16;
                    buf.push(s); // L
                    buf.push(s); // R
                    phase += step;
                    if phase >= std::f64::consts::TAU {
                        phase -= std::f64::consts::TAU;
                    }
                }
                let _ = tx.try_send(buf);

                // Pace to real time so we don't flood the channel.
                next += chunk;
                let now = Instant::now();
                if next > now {
                    std::thread::sleep(next - now);
                } else {
                    next = now;
                }
            }
        })
        .expect("failed to spawn tone thread")
}

fn fail_sender(shared: &SharedApp, msg: &str) {
    tracing::error!("Sender failed to start: {msg}");
    shared.lock().sender.is_streaming = false;
}

/// Build the source's local delayed-playback monitor: an output stream + drift
/// controller that play the sender's own audio at `capture + budget`, so the
/// source is in sync with remote receivers. Returns `None` (net-only) if no
/// output device is available. On success, `stream_slot`/`sync_slot` are filled
/// and must be kept alive for the session.
fn build_sender_monitor(
    budget_us: u64,
    output_name: Option<String>,
    stop: Arc<AtomicBool>,
    shared: SharedApp,
    stream_slot: &mut Option<cpal::Stream>,
    sync_slot: &mut Option<std::thread::JoinHandle<()>>,
) -> Option<SenderMonitor> {
    // Pick an explicit output when named (e.g. the built-in speakers) so the
    // monitor never plays back into a capture loopback like BlackHole. Falls
    // back to the system default.
    let device = match output_name {
        Some(name) if !name.is_empty() => {
            device::find_output_device(&name).or_else(device::default_output_device)?
        }
        _ => device::default_output_device()?,
    };
    let config = match device::output_config(&device) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Source monitor disabled (output config error: {e})");
            return None;
        }
    };

    let max_samples = jitter_cap_samples(budget_us / 1000);
    let jitter = Arc::new(JitterBuffer::with_max(SAMPLE_RATE as usize, max_samples));
    let ratio = Arc::new(SharedRatio::new(1.0));
    let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));
    let gate = Arc::new(PlayoutGate::new());

    match start_playback(
        &device,
        &config,
        jitter.clone(),
        ratio.clone(),
        volume.clone(),
        gate.clone(),
        stop.clone(),
    ) {
        Ok(s) => *stream_slot = Some(s),
        Err(e) => {
            tracing::warn!("Source monitor disabled (playback error: {e})");
            return None;
        }
    }

    // The source's capture device (e.g. BlackHole) and its speakers run on
    // independent clocks, so the monitor still needs drift control. It does not
    // own the UI, so `mirror_to_ui = false`.
    let sync = {
        let (jitter, ratio, shared, volume, gate, stop) = (
            jitter.clone(),
            ratio.clone(),
            shared,
            volume.clone(),
            gate.clone(),
            stop,
        );
        std::thread::Builder::new()
            .name("syncplay-monitor-sync".into())
            .spawn(move || run_sync_controller(jitter, ratio, shared, volume, gate, stop, false))
            .expect("failed to spawn monitor sync thread")
    };
    *sync_slot = Some(sync);

    tracing::info!(
        "Source monitor: playing local audio delayed {}ms to match receivers",
        budget_us / 1000
    );
    Some(SenderMonitor {
        jitter,
        gate,
        budget_us,
    })
}

/// Start the receiver pipeline for a chosen sender. Returns thread handles.
pub fn start_receiver(
    shared: SharedApp,
    sender: DiscoveredSender,
    output_name: String,
    playout_delay_ms: u64,
) -> ReceiverThreads {
    let stop = Arc::new(AtomicBool::new(false));

    // Shared audio-path state, created here so both threads reference the same
    // buffers. `gate` holds playback silent until the synchronized start
    // instant; `clock` estimates the sender↔receiver clock offset.
    // Cap the buffer at budget + 120ms so a network burst can't accumulate
    // unbounded latency; excess is dropped to snap back to the playout point.
    let max_samples = jitter_cap_samples(playout_delay_ms);
    let jitter = Arc::new(JitterBuffer::with_max(SAMPLE_RATE as usize, max_samples));
    let ratio = Arc::new(SharedRatio::new(1.0));
    let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));
    let gate = Arc::new(PlayoutGate::new());
    let clock = Arc::new(ClockSync::new());
    let budget_us = playout_delay_ms * 1000;

    // Sync controller thread: adjusts `ratio` and mirrors the volume slider.
    let sync_handle = {
        let (jitter, ratio, shared, volume, gate, stop) = (
            jitter.clone(),
            ratio.clone(),
            shared.clone(),
            volume.clone(),
            gate.clone(),
            stop.clone(),
        );
        std::thread::Builder::new()
            .name("syncplay-sync".into())
            .spawn(move || run_sync_controller(jitter, ratio, shared, volume, gate, stop, true))
            .expect("failed to spawn sync controller thread")
    };

    // Engine thread: owns the playback stream and runs the recv loop.
    let net_handle = {
        let stop = stop.clone();
        std::thread::Builder::new()
            .name("syncplay-receiver".into())
            .spawn(move || {
                receiver_engine(
                    shared,
                    sender,
                    output_name,
                    jitter,
                    ratio,
                    volume,
                    gate,
                    clock,
                    budget_us,
                    stop,
                )
            })
            .expect("failed to spawn receiver engine thread")
    };

    ReceiverThreads {
        stop,
        net_handle: Some(net_handle),
        sync_handle: Some(sync_handle),
    }
}

#[allow(clippy::too_many_arguments)]
fn receiver_engine(
    shared: SharedApp,
    sender: DiscoveredSender,
    output_name: String,
    jitter: Arc<JitterBuffer>,
    ratio: Arc<SharedRatio>,
    volume: Arc<AtomicU32>,
    gate: Arc<PlayoutGate>,
    clock: Arc<ClockSync>,
    budget_us: u64,
    stop: Arc<AtomicBool>,
) {
    let device = if output_name.is_empty() {
        device::default_output_device()
    } else {
        device::find_output_device(&output_name).or_else(device::default_output_device)
    };
    let device = match device {
        Some(d) => d,
        None => return fail_receiver(&shared, "no output device available"),
    };

    let config = match device::output_config(&device) {
        Ok(c) => c,
        Err(e) => return fail_receiver(&shared, &format!("output config error: {e}")),
    };

    let session = match ReceiverSession::new(&sender) {
        Ok(s) => s,
        Err(e) => return fail_receiver(&shared, &format!("cannot connect to sender: {e}")),
    };

    // Build and keep the playback stream alive for the whole session.
    let _stream = match start_playback(
        &device,
        &config,
        jitter.clone(),
        ratio.clone(),
        volume.clone(),
        gate.clone(),
        stop.clone(),
    ) {
        Ok(s) => s,
        Err(e) => return fail_receiver(&shared, &format!("cannot start playback: {e}")),
    };

    tracing::info!(
        "Receiver engine running on '{}'",
        device.name().unwrap_or_else(|_| "unknown".into())
    );

    // Blocks until `stop` is set; `_stream` stays alive in scope meanwhile.
    session.run(
        &jitter,
        shared.clone(),
        stop.clone(),
        clock,
        gate,
        budget_us,
    );

    session.disconnect();
    shared.lock().receiver.is_connected = false;
    tracing::info!("Receiver engine stopped");
}

fn fail_receiver(shared: &SharedApp, msg: &str) {
    tracing::error!("Receiver failed to start: {msg}");
    shared.lock().receiver.is_connected = false;
}

/// Hard cap for a jitter buffer: `budget + 120ms` of stereo audio, expressed in
/// interleaved i16 samples. Bounds added latency so a network burst can't push
/// playback permanently behind the intended playout point.
fn jitter_cap_samples(budget_ms: u64) -> usize {
    const STEREO_SAMPLES_PER_MS: u64 = (SAMPLE_RATE as u64 / 1000) * CHANNELS as u64; // 96
    ((budget_ms + 120) * STEREO_SAMPLES_PER_MS) as usize
}

/// Discover this machine's local (non-loopback) IPv4 address.
///
/// Uses the standard "connect a UDP socket to a public address and read back
/// the local address" trick — no packets are actually sent.
pub fn local_ip_address() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|a| a.ip().to_string())
}
