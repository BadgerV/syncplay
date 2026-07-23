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
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::Arc;

use cpal::traits::DeviceTrait;
use crossbeam_channel::bounded;

use crate::audio::capture::start_capture;
use crate::audio::device;
use crate::audio::playback::start_playback;
use crate::net::discovery::DiscoveryService;
use crate::net::receiver::ReceiverSession;
use crate::net::sender::SenderSession;
use crate::state::shared::{
    DiscoveredSender, JitterBuffer, ReceiverThreads, SenderThreads, SharedApp, SharedRatio,
    AUDIO_PORT, CHANNELS, SAMPLE_RATE,
};
use crate::sync::controller::run_sync_controller;

/// Start the sender pipeline. Returns thread handles the UI stores in state.
pub fn start_sender(shared: SharedApp, input_name: String) -> SenderThreads {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = stop.clone();
    let handle = std::thread::Builder::new()
        .name("syncplay-sender".into())
        .spawn(move || sender_engine(shared, input_name, stop_thread))
        .expect("failed to spawn sender engine thread");

    SenderThreads {
        stop,
        net_handle: Some(handle),
    }
}

fn sender_engine(shared: SharedApp, input_name: String, stop: Arc<AtomicBool>) {
    // Resolve the input device (explicit selection, else system default).
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

    let session = match SenderSession::new() {
        Ok(s) => s,
        Err(e) => return fail_sender(&shared, &format!("cannot bind sender ports: {e}")),
    };

    let (tx, rx) = bounded::<Vec<i16>>(64);

    // Build and keep the capture stream alive for the whole session.
    let _stream = match start_capture(&device, &config, tx, stop.clone()) {
        Ok(s) => s,
        Err(e) => return fail_sender(&shared, &format!("cannot start capture: {e}")),
    };

    // Advertise ourselves so receivers can discover us.
    let discovery = DiscoveryService::new().ok();
    if let Some(d) = &discovery {
        let host = local_ip_address().unwrap_or_else(|| "127.0.0.1".to_string());
        if let Err(e) = d.register_sender("SyncPlay Sender", &host, AUDIO_PORT, SAMPLE_RATE, CHANNELS)
        {
            tracing::warn!("mDNS registration failed: {e}");
        }
    }

    tracing::info!(
        "Sender engine running on '{}'",
        device.name().unwrap_or_else(|_| "unknown".into())
    );

    // Blocks until `stop` is set; `_stream` stays alive in scope meanwhile.
    session.run(rx, shared.clone(), stop.clone());

    if let Some(d) = &discovery {
        d.unregister();
    }
    shared.lock().sender.is_streaming = false;
    tracing::info!("Sender engine stopped");
}

fn fail_sender(shared: &SharedApp, msg: &str) {
    tracing::error!("Sender failed to start: {msg}");
    shared.lock().sender.is_streaming = false;
}

/// Start the receiver pipeline for a chosen sender. Returns thread handles.
pub fn start_receiver(
    shared: SharedApp,
    sender: DiscoveredSender,
    output_name: String,
) -> ReceiverThreads {
    let stop = Arc::new(AtomicBool::new(false));

    // Shared audio-path state, created here so both threads reference the same
    // buffers.
    let jitter = Arc::new(JitterBuffer::new(SAMPLE_RATE as usize)); // ~1s capacity
    let ratio = Arc::new(SharedRatio::new(1.0));
    let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));

    // Sync controller thread: adjusts `ratio` and mirrors the volume slider.
    let sync_handle = {
        let (jitter, ratio, shared, volume, stop) = (
            jitter.clone(),
            ratio.clone(),
            shared.clone(),
            volume.clone(),
            stop.clone(),
        );
        std::thread::Builder::new()
            .name("syncplay-sync".into())
            .spawn(move || run_sync_controller(jitter, ratio, shared, volume, stop))
            .expect("failed to spawn sync controller thread")
    };

    // Engine thread: owns the playback stream and runs the recv loop.
    let net_handle = {
        let stop = stop.clone();
        std::thread::Builder::new()
            .name("syncplay-receiver".into())
            .spawn(move || {
                receiver_engine(shared, sender, output_name, jitter, ratio, volume, stop)
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
    session.run(&jitter, shared.clone(), stop.clone());

    session.disconnect();
    shared.lock().receiver.is_connected = false;
    tracing::info!("Receiver engine stopped");
}

fn fail_receiver(shared: &SharedApp, msg: &str) {
    tracing::error!("Receiver failed to start: {msg}");
    shared.lock().receiver.is_connected = false;
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
