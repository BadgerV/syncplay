use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::net::protocol::{deserialize_packet, serialize_control, ControlMessage, MAX_PACKET_SIZE};
use crate::state::shared::{DiscoveredSender, JitterBuffer, SharedApp, AUDIO_PORT, CONTROL_PORT};

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
        let _audio_addr: SocketAddr = format!("{}:{}", sender.host, AUDIO_PORT)
            .parse()
            .map_err(|e| {
                crate::error::SyncPlayError::Config(format!("Invalid sender address: {e}"))
            })?;

        // Subscribe from this socket so the sender delivers audio back to it.
        let data =
            serialize_control(&ControlMessage::Subscribe).map_err(crate::error::SyncPlayError::Serialization)?;
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
    /// Receives UDP packets, deserializes AudioPackets, and pushes them into
    /// the jitter buffer. Tracks packet loss via sequence numbers. Blocks until
    /// `stop` is set.
    pub fn run(&self, jitter: &Arc<JitterBuffer>, shared: SharedApp, stop: Arc<AtomicBool>) {
        let mut recv_buf = vec![0u8; MAX_PACKET_SIZE];
        let mut peak: f32 = 0.0;

        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }

            match self.socket.recv_from(&mut recv_buf) {
                Ok((len, _src)) => {
                    match deserialize_packet(&recv_buf[..len]) {
                        Ok(packet) => {
                            // Sentinel packet: sender is shutting down.
                            if packet.sequence_number == u64::MAX {
                                tracing::info!("Sender sent goodbye");
                                continue;
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
                            // Likely a control message (Welcome/Goodbye) that
                            // shares this socket, or a corrupt datagram.
                            tracing::trace!("Non-audio datagram ignored: {e}");
                        }
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Read timeout — loop back to check the stop flag.
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
}

/// Peak absolute amplitude of an interleaved i16 buffer, normalized to [0, 1].
fn peak_of(samples: &[i16]) -> f32 {
    let max = samples.iter().map(|s| s.unsigned_abs()).max().unwrap_or(0);
    max as f32 / 32768.0
}
