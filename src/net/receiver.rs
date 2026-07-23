use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::net::protocol::{
    deserialize_packet, serialize_control, ControlMessage, MAX_PACKET_SIZE,
};
use crate::state::shared::{
    DiscoveredSender, JitterBuffer, PlayoutGate, SharedApp, AUDIO_PORT, CONTROL_PORT,
};
use crate::sync::clock::{now_us, ClockSync};

/// How often to send a clock-sync probe. Frequent at first (to converge fast),
/// then the best-RTT sample sticks for the rest of the session.
const TIMESYNC_INTERVAL: Duration = Duration::from_millis(250);

/// Manages the receiver-side UDP session.
///
/// A single socket is used for both subscribing (control) and receiving audio.
/// This is deliberate: the sender learns where to deliver audio from the source
/// address of the `Subscribe` message, so control and audio MUST share one
/// socket/port — otherwise the sender streams to the wrong port and no audio
/// ever arrives.
pub struct ReceiverSession {
    socket: UdpSocket,
    control_addr: SocketAddr,
}

impl ReceiverSession {
    /// Create a new receiver session and subscribe to the given sender.
    pub fn new(sender: &DiscoveredSender) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        // A read timeout lets the recv loop notice the stop flag even when no
        // packets are arriving (e.g. the sender went away).
        socket.set_read_timeout(Some(Duration::from_millis(500)))?;

        let control_addr: SocketAddr = format!("{}:{}", sender.host, CONTROL_PORT)
            .parse()
            .map_err(|e| {
                crate::error::SyncPlayError::Config(format!("Invalid control address: {e}"))
            })?;

        // Sanity-check the audio address parses (host is well-formed).
        let _audio_addr: SocketAddr =
            format!("{}:{}", sender.host, AUDIO_PORT)
                .parse()
                .map_err(|e| {
                    crate::error::SyncPlayError::Config(format!("Invalid sender address: {e}"))
                })?;

        // Subscribe from this socket so the sender delivers audio back to it.
        let data = serialize_control(&ControlMessage::Subscribe)
            .map_err(crate::error::SyncPlayError::Serialization)?;
        socket.send_to(&data, control_addr)?;

        tracing::info!("Subscribed to sender at {}:{}", sender.host, AUDIO_PORT);

        Ok(Self {
            socket,
            control_addr,
        })
    }

    /// Send an unsubscribe message to the sender.
    pub fn disconnect(&self) {
        if let Ok(data) = serialize_control(&ControlMessage::Unsubscribe) {
            let _ = self.socket.send_to(&data, self.control_addr);
        }
        tracing::info!("Unsubscribed from sender");
    }

    /// Run the main receive loop on the calling (engine) thread.
    ///
    /// Handles three things on one socket, distinguished by source port:
    /// - **Audio** (from the sender's audio port) → jitter buffer; the first
    ///   chunk (once the clock is synced) arms the playout `gate` at
    ///   `sender_ts − offset + budget` so playback starts at the shared instant.
    /// - **Control replies** (from the sender's control port) → `TimeSyncResponse`
    ///   folds a round-trip sample into `clock`.
    /// - It also periodically emits `TimeSyncRequest` probes.
    ///
    /// Blocks until `stop` is set.
    pub fn run(
        &self,
        jitter: &Arc<JitterBuffer>,
        shared: SharedApp,
        stop: Arc<AtomicBool>,
        clock: Arc<ClockSync>,
        gate: Arc<PlayoutGate>,
        budget_us: u64,
    ) {
        let mut recv_buf = vec![0u8; MAX_PACKET_SIZE];
        let mut peak: f32 = 0.0;
        let mut last_ping = Instant::now() - TIMESYNC_INTERVAL; // ping immediately

        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }

            // Emit a clock-sync probe on schedule.
            if last_ping.elapsed() >= TIMESYNC_INTERVAL {
                self.send_time_probe();
                last_ping = Instant::now();
            }

            match self.socket.recv_from(&mut recv_buf) {
                Ok((len, src)) => {
                    // Control replies arrive from the sender's control port.
                    if src.port() == CONTROL_PORT {
                        if let Ok(ControlMessage::TimeSyncResponse {
                            client_send_us,
                            server_us,
                        }) = bincode::deserialize::<ControlMessage>(&recv_buf[..len])
                        {
                            clock.update(client_send_us, server_us, now_us());
                            if !gate.is_armed() {
                                let rtt = clock.best_rtt_us();
                                let off = clock.offset_us();
                                tracing::info!(
                                    "Clock synced: offset={off}us rtt={rtt}us (waiting for audio to anchor playout)"
                                );
                            }
                        }
                        continue;
                    }

                    // Otherwise treat it as audio.
                    match deserialize_packet(&recv_buf[..len]) {
                        Ok(packet) => {
                            // Sentinel packet: sender is shutting down.
                            if packet.sequence_number == u64::MAX {
                                tracing::info!("Sender sent goodbye");
                                continue;
                            }

                            // Anchor playout on the first audio chunk after the
                            // clock is synced: schedule the shared start instant.
                            if !gate.is_armed() && clock.is_synced() {
                                let local = clock.remote_to_local_us(packet.timestamp_micros);
                                let deadline = (local + budget_us as i64).max(0) as u64;
                                gate.arm(deadline);
                                let lead = deadline as i64 - now_us() as i64;
                                tracing::info!(
                                    "Playout anchored: start in {}ms (budget={}ms, offset={}us)",
                                    lead / 1000,
                                    budget_us / 1000,
                                    clock.offset_us(),
                                );
                            }

                            jitter.record_sequence(packet.sequence_number);
                            peak = peak.max(peak_of(&packet.audio_data));
                            jitter.push_packet(&packet.audio_data);

                            // Update UI stats periodically.
                            if packet.sequence_number.is_multiple_of(50) {
                                let mut app = shared.lock();
                                app.receiver.packets_received = packet.sequence_number;
                                app.receiver.packets_lost =
                                    jitter.packets_lost.load(Ordering::Relaxed);
                                app.receiver.buffer_fill_ms = jitter.fill_ms();
                                app.receiver.underruns = jitter.underruns.load(Ordering::Relaxed);
                                app.receiver.peak_level = peak;
                                peak = 0.0;
                            }
                        }
                        Err(e) => {
                            tracing::trace!("Non-audio datagram ignored: {e}");
                        }
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Read timeout — loop back to check the stop flag / ping.
                }
                Err(e) => {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    tracing::error!("Receive error: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        tracing::info!("Receiver loop stopped");
    }

    /// Send one clock-sync probe to the sender's control port.
    fn send_time_probe(&self) {
        let msg = ControlMessage::TimeSyncRequest {
            client_send_us: now_us(),
        };
        if let Ok(data) = serialize_control(&msg) {
            let _ = self.socket.send_to(&data, self.control_addr);
        }
    }
}

/// Peak absolute amplitude of an interleaved i16 buffer, normalized to [0, 1].
fn peak_of(samples: &[i16]) -> f32 {
    let max = samples.iter().map(|s| s.unsigned_abs()).max().unwrap_or(0);
    max as f32 / 32768.0
}
