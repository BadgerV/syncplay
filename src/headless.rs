//! Headless (no-GUI) runner for automated and two-machine testing.
//!
//! Mirrors the GUI's Start/Connect actions but drives them from the CLI and
//! logs periodic stats to stdout, so two machines (or two processes) can stream
//! and be verified without any manual button clicks.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::engine::{start_receiver, start_sender, AudioSource};
use crate::error::{Result, SyncPlayError};
use crate::state::shared::{DiscoveredSender, SharedApp, AUDIO_PORT, CHANNELS, SAMPLE_RATE};

/// How long a headless receiver waits to discover a sender before giving up.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Run the sender pipeline without a GUI, logging stats every second until
/// `duration_secs` elapses (or until killed, if `None`).
pub fn run_sender(
    shared: SharedApp,
    source: AudioSource,
    playout_delay_ms: u64,
    duration_secs: Option<u64>,
) -> Result<()> {
    shared.lock().sender.is_streaming = true;
    let threads = start_sender(shared.clone(), source, playout_delay_ms);
    tracing::info!("Headless sender running (Ctrl-C to stop).");

    let deadline = duration_secs.map(|s| Instant::now() + Duration::from_secs(s));
    loop {
        std::thread::sleep(Duration::from_secs(1));
        {
            let app = shared.lock();
            tracing::info!(
                "[sender] receivers={} packets_sent={} data={:.1}MB peak={:.2}",
                app.sender.receiver_count,
                app.sender.packets_sent,
                app.sender.bytes_sent as f64 / 1_000_000.0,
                app.sender.peak_level,
            );
        }
        if reached(deadline) {
            break;
        }
    }

    threads.stop.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));
    tracing::info!("Headless sender finished.");
    Ok(())
}

/// Run the receiver pipeline without a GUI: wait for a sender to be discovered,
/// connect, then log stats every second until `duration_secs` elapses.
pub fn run_receiver(
    shared: SharedApp,
    connect_filter: Option<String>,
    manual_ip: Option<String>,
    playout_delay_ms: u64,
    duration_secs: Option<u64>,
) -> Result<()> {
    // A manual IP skips mDNS entirely — useful when the network blocks
    // multicast/Bonjour but unicast between the two Macs still works.
    let sender = if let Some(ip) = manual_ip {
        tracing::info!("Manual connect to {ip}:{AUDIO_PORT} (skipping discovery)");
        DiscoveredSender {
            name: format!("manual@{ip}"),
            host: ip,
            port: AUDIO_PORT,
            sample_rate: SAMPLE_RATE,
            channels: CHANNELS,
        }
    } else {
        tracing::info!("Headless receiver: waiting up to 30s for a sender...");
        wait_for_sender(&shared, connect_filter.as_deref(), DISCOVERY_TIMEOUT)
            .ok_or_else(|| SyncPlayError::Config("no sender discovered within 30s".into()))?
    };

    tracing::info!(
        "Connecting to '{}' at {}:{}",
        sender.name,
        sender.host,
        sender.port
    );
    let output = shared.lock().receiver.selected_output_device.clone();
    let threads = start_receiver(shared.clone(), sender.clone(), output, playout_delay_ms);
    {
        let mut app = shared.lock();
        app.receiver.is_connected = true;
        app.receiver.connected_sender = Some(sender);
    }

    let deadline = duration_secs.map(|s| Instant::now() + Duration::from_secs(s));
    loop {
        std::thread::sleep(Duration::from_secs(1));
        {
            let app = shared.lock();
            let total = app.receiver.packets_received + app.receiver.packets_lost;
            let loss = if total == 0 {
                0.0
            } else {
                app.receiver.packets_lost as f64 / total as f64 * 100.0
            };
            tracing::info!(
                "[receiver] received={} lost={} ({:.1}%) buffer={:.0}ms underruns={} peak={:.2}",
                app.receiver.packets_received,
                app.receiver.packets_lost,
                loss,
                app.receiver.buffer_fill_ms,
                app.receiver.underruns,
                app.receiver.peak_level,
            );
        }
        if reached(deadline) {
            break;
        }
    }

    threads.stop.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));
    tracing::info!("Headless receiver finished.");
    Ok(())
}

/// Poll the discovery list until a matching sender appears or `timeout` passes.
fn wait_for_sender(
    shared: &SharedApp,
    name_filter: Option<&str>,
    timeout: Duration,
) -> Option<DiscoveredSender> {
    let deadline = Instant::now() + timeout;
    loop {
        {
            let app = shared.lock();
            let found = app.receiver.discovered_senders.iter().find(|s| {
                name_filter.is_none_or(|f| s.name.to_lowercase().contains(&f.to_lowercase()))
            });
            if let Some(s) = found {
                return Some(s.clone());
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn reached(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}
