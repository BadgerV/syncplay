use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use parking_lot::Mutex;

use crate::error::Result;
use crate::net::protocol::{build_packet, serialize_packet, ControlMessage, MAX_PACKET_SIZE};
use crate::state::shared::{JitterBuffer, PlayoutGate, SharedApp, AUDIO_PORT, CONTROL_PORT};
use crate::sync::clock::now_us;

/// Local delayed-playback hookup for the source machine. The sender feeds a
/// copy of every captured chunk into `jitter` and arms `gate` so the source
/// plays the audio at `capture_ts + budget_us` — the same instant the remote
/// receiver plays it, which is what makes playback simultaneous.
pub struct SenderMonitor {
    pub jitter: Arc<JitterBuffer>,
    pub gate: Arc<PlayoutGate>,
    pub budget_us: u64,
}

/// Per-receiver bookkeeping. Currently only presence (map membership) is used;
/// the counters are reserved for future per-receiver stats in the UI.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct ReceiverStats {
    pub packets_sent: u64,
    pub bytes_sent: u64,
}

/// Peak absolute amplitude of an interleaved i16 buffer, normalized to [0, 1].
fn peak_of(samples: &[i16]) -> f32 {
    let max = samples.iter().map(|s| s.unsigned_abs()).max().unwrap_or(0);
    max as f32 / 32768.0
}

/// Manages the sender-side UDP session.
///
/// Maintains a list of subscribed receivers and sends audio packets
/// to each one via direct UDP.
pub struct SenderSession {
    audio_socket: UdpSocket,
    control_socket: UdpSocket,
    receivers: Arc<Mutex<HashMap<SocketAddr, ReceiverStats>>>,
}

impl SenderSession {
    /// Create a new sender session bound to the audio and control ports.
    pub fn new() -> Result<Self> {
        let audio_socket = UdpSocket::bind(format!("0.0.0.0:{AUDIO_PORT}")).map_err(|e| {
            crate::error::SyncPlayError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("Audio port {AUDIO_PORT} in use: {e}"),
            ))
        })?;

        let control_socket = UdpSocket::bind(format!("0.0.0.0:{CONTROL_PORT}")).map_err(|e| {
            crate::error::SyncPlayError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("Control port {CONTROL_PORT} in use: {e}"),
            ))
        })?;

        // Set non-blocking for the control socket so we can poll
        control_socket.set_nonblocking(true).ok();

        tracing::info!("Sender session created: audio={AUDIO_PORT}, control={CONTROL_PORT}");

        Ok(Self {
            audio_socket,
            control_socket,
            receivers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Run the main send loop on the calling (engine) thread.
    ///
    /// Reads audio data from the packet channel, wraps in AudioPacket,
    /// and sends to all connected receivers. Blocks until `stop` is set or
    /// the capture channel disconnects.
    pub fn run(
        &self,
        packet_rx: Receiver<Vec<i16>>,
        shared: SharedApp,
        stop: Arc<AtomicBool>,
        monitor: Option<SenderMonitor>,
    ) {
        let mut sequence_number: u64 = 0;
        let mut bytes_total: u64 = 0;
        let mut peak: f32 = 0.0;

        // Buffer for serialized packets
        let mut send_buf = vec![0u8; MAX_PACKET_SIZE];

        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }

            // Check for control messages (subscribe/unsubscribe)
            Self::process_control_messages(&self.control_socket, &self.receivers);

            // Read audio data from the capture pipeline
            match packet_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(audio_data) => {
                    peak = peak.max(peak_of(&audio_data));
                    // Capture timestamp on the shared process clock — the same
                    // timescale used to answer TimeSyncRequest, so receivers can
                    // map it into their own clock.
                    let capture_ts = now_us();

                    // Feed the source's own delayed monitor: play this chunk
                    // locally at capture_ts + budget, matching the remote.
                    if let Some(m) = &monitor {
                        m.jitter.push_packet(&audio_data);
                        m.gate.arm(capture_ts + m.budget_us);
                    }

                    let packet = build_packet(sequence_number, capture_ts, audio_data);

                    match serialize_packet(&packet) {
                        Ok(bytes) => {
                            let len = bytes.len().min(send_buf.len());
                            send_buf[..len].copy_from_slice(&bytes);

                            // Send to each receiver
                            let recvs = self.receivers.lock();
                            for addr in recvs.keys() {
                                if let Err(e) = self.audio_socket.send_to(&send_buf[..len], *addr) {
                                    tracing::warn!("Failed to send to {addr}: {e}");
                                }
                            }
                            drop(recvs);
                            bytes_total += len as u64;

                            // Update UI stats periodically
                            if sequence_number.is_multiple_of(50) {
                                let count = self.receivers.lock().len();
                                let mut app = shared.lock();
                                app.sender.packets_sent = sequence_number;
                                app.sender.bytes_sent = bytes_total;
                                app.sender.receiver_count = count;
                                app.sender.peak_level = peak;
                                peak = 0.0;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Serialization error: {e}");
                        }
                    }

                    sequence_number = sequence_number.wrapping_add(1);
                }
                Err(RecvTimeoutError::Timeout) => {
                    // No data yet, just loop
                }
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!("Capture channel disconnected, stopping sender");
                    break;
                }
            }
        }

        // Send goodbye to all receivers
        let goodbye = serialize_packet(&build_packet(u64::MAX, 0, vec![])).unwrap_or_default();
        let recvs = self.receivers.lock();
        for addr in recvs.keys() {
            let _ = self.audio_socket.send_to(&goodbye, *addr);
        }

        tracing::info!("Sender loop stopped. Sent {sequence_number} packets total");
    }

    /// Process incoming control messages (Subscribe/Unsubscribe).
    fn process_control_messages(
        control_socket: &UdpSocket,
        receivers: &Arc<Mutex<HashMap<SocketAddr, ReceiverStats>>>,
    ) {
        let mut buf = [0u8; 512];
        loop {
            match control_socket.recv_from(&mut buf) {
                Ok((len, addr)) => {
                    let msg: std::result::Result<ControlMessage, _> =
                        bincode::deserialize(&buf[..len]);
                    match msg {
                        Ok(ControlMessage::Subscribe) => {
                            let mut recvs = receivers.lock();
                            if let std::collections::hash_map::Entry::Vacant(e) = recvs.entry(addr)
                            {
                                tracing::info!("Receiver subscribed: {addr}");
                                e.insert(ReceiverStats::default());

                                // Send welcome
                                let welcome = ControlMessage::Welcome {
                                    sample_rate: 48000,
                                    channels: 2,
                                };
                                if let Ok(data) = bincode::serialize(&welcome) {
                                    let _ = control_socket.send_to(&data, addr);
                                }
                            }
                        }
                        Ok(ControlMessage::Unsubscribe) => {
                            let mut recvs = receivers.lock();
                            recvs.remove(&addr);
                            tracing::info!("Receiver unsubscribed: {addr}");

                            // Send goodbye
                            let goodbye = ControlMessage::Goodbye;
                            if let Ok(data) = bincode::serialize(&goodbye) {
                                let _ = control_socket.send_to(&data, addr);
                            }
                        }
                        Ok(ControlMessage::TimeSyncRequest { client_send_us }) => {
                            // Reply immediately, stamping our clock at receipt.
                            let reply = ControlMessage::TimeSyncResponse {
                                client_send_us,
                                server_us: now_us(),
                            };
                            if let Ok(data) = bincode::serialize(&reply) {
                                let _ = control_socket.send_to(&data, addr);
                            }
                        }
                        _ => {}
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break; // No more messages
                }
                Err(e) => {
                    tracing::debug!("Control socket error: {e}");
                    break;
                }
            }
        }
    }
}
