mod audio;
mod engine;
mod error;
mod headless;
mod net;
mod state;
mod sync;
mod ui;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use parking_lot::Mutex;

use crate::audio::device;
use crate::error::Result;
use crate::net::discovery::run_discovery_browser;
use crate::state::shared::{AppMode, AppState, SharedApp};
use crate::ui::app::SyncPlayApp;

#[derive(Parser, Debug)]
#[command(name = "syncplay", about = "Stream audio across Macs on a LAN")]
struct Args {
    /// Start in sender mode (default: receiver)
    #[arg(long)]
    sender: bool,

    /// Input device name (sender mode, partial match)
    #[arg(long)]
    input_device: Option<String>,

    /// Output device name (receiver mode, partial match)
    #[arg(long)]
    output_device: Option<String>,

    /// Run without a GUI (for automated / two-machine testing)
    #[arg(long)]
    headless: bool,

    /// Sender: emit a sine test tone instead of capturing a device
    #[arg(long)]
    tone: bool,

    /// Sender: test-tone frequency in Hz
    #[arg(long, default_value_t = 440.0)]
    tone_freq: f32,

    /// Receiver: only connect to a sender whose name contains this substring
    #[arg(long)]
    connect: Option<String>,

    /// Receiver: connect directly to this sender IP, skipping mDNS discovery
    /// (use when the network blocks multicast/Bonjour but unicast works)
    #[arg(long)]
    sender_ip: Option<String>,

    /// Headless: stop automatically after this many seconds
    #[arg(long)]
    duration: Option<u64>,

    /// Synchronized-playout budget in ms: the fixed delay, from capture, at
    /// which every endpoint (source included) plays. Higher = more robust to
    /// network jitter but more latency. Must exceed network + device latency.
    #[arg(long, default_value_t = crate::state::shared::DEFAULT_PLAYOUT_DELAY_MS)]
    playout_delay_ms: u64,
}

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "syncplay=info".into()),
        )
        .init();

    let args = Args::parse();

    let mode = if args.sender {
        AppMode::Sender
    } else {
        AppMode::Receiver
    };

    tracing::info!("SyncPlay starting in {mode:?} mode");

    let app_state: SharedApp = Arc::new(Mutex::new(AppState::new(mode)));

    // Pre-populate device lists and apply CLI device selections.
    {
        let mut app = app_state.lock();
        app.sender.available_input_devices = device::enumerate_input_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
        app.sender.available_output_devices = device::enumerate_output_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
        app.receiver.available_output_devices = device::enumerate_output_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();

        if let Some(ref name) = args.input_device {
            app.sender.selected_input_device = name.clone();
        }
        if let Some(ref name) = args.output_device {
            app.receiver.selected_output_device = name.clone();
        }
    }

    // Always run the mDNS browser so receiver discovery works regardless of the
    // startup mode or later in-app mode switches. Sender registration is owned
    // by the engine and happens when streaming actually starts.
    let browser_stop = Arc::new(AtomicBool::new(false));
    if let Ok(daemon) = mdns_sd::ServiceDaemon::new() {
        let app = app_state.clone();
        let stop = browser_stop.clone();
        std::thread::spawn(move || {
            run_discovery_browser(daemon, app, stop);
        });
    } else {
        tracing::warn!("mDNS unavailable — sender discovery disabled");
    }

    // ── Headless mode: run the engine directly, no GUI ──
    if args.headless {
        let result = match mode {
            AppMode::Sender => {
                let source = if args.tone {
                    engine::AudioSource::Tone(args.tone_freq)
                } else {
                    engine::AudioSource::Device(args.input_device.clone().unwrap_or_default())
                };
                headless::run_sender(
                    app_state.clone(),
                    source,
                    args.playout_delay_ms,
                    args.output_device.clone(),
                    args.duration,
                )
            }
            AppMode::Receiver => headless::run_receiver(
                app_state.clone(),
                args.connect.clone(),
                args.sender_ip.clone(),
                args.playout_delay_ms,
                args.duration,
            ),
        };
        browser_stop.store(true, Ordering::Relaxed);
        return result;
    }

    // ── Launch GUI ──
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([500.0, 650.0])
            .with_min_inner_size([400.0, 400.0])
            .with_title("SyncPlay"),
        ..Default::default()
    };

    eframe::run_native(
        "SyncPlay",
        native_options,
        Box::new(|_cc| Ok(Box::new(SyncPlayApp::new(app_state.clone())))),
    )
    .map_err(|e| crate::error::SyncPlayError::Config(format!("eframe error: {e}")))?;

    // Cleanup: stop the browser and any live engine threads.
    browser_stop.store(true, Ordering::Relaxed);
    {
        let mut app = app_state.lock();
        if let Some(t) = &app.sender_threads {
            t.stop.store(true, Ordering::Relaxed);
        }
        if let Some(t) = &app.receiver_threads {
            t.stop.store(true, Ordering::Relaxed);
        }
        app.sender_threads = None;
        app.receiver_threads = None;
    }

    tracing::info!("SyncPlay shutting down");
    Ok(())
}
