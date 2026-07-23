use serde::{Deserialize, Serialize};

/// Audio packet sent from sender to receivers via UDP.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AudioPacket {
    /// Monotonically increasing packet sequence number
    pub sequence_number: u64,
    /// Sender's monotonic timestamp in microseconds since stream start
    pub timestamp_micros: u64,
    /// Number of audio frames in this packet (typically 480 for 10ms)
    pub frame_count: u32,
    /// Number of channels (2 for stereo)
    pub channels: u16,
    /// Interleaved stereo PCM: [L0, R0, L1, R1, ...]
    pub audio_data: Vec<i16>,
}

/// Control messages exchanged between sender and receivers.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ControlMessage {
    /// Receiver → Sender: request to subscribe to audio stream
    Subscribe,
    /// Receiver → Sender: request to unsubscribe
    Unsubscribe,
    /// Sender → Receiver: acknowledge subscription with stream info
    Welcome {
        sample_rate: u32,
        channels: u16,
    },
    /// Sender → Receiver: stream is ending
    Goodbye,
}

/// Maximum UDP packet size (generous buffer for ~2KB audio + headers)
pub const MAX_PACKET_SIZE: usize = 4096;

/// Serialize an AudioPacket to bytes using bincode.
pub fn serialize_packet(packet: &AudioPacket) -> Result<Vec<u8>, Box<bincode::ErrorKind>> {
    bincode::serialize(packet)
}

/// Deserialize an AudioPacket from bytes.
pub fn deserialize_packet(data: &[u8]) -> Result<AudioPacket, Box<bincode::ErrorKind>> {
    bincode::deserialize(data)
}

/// Serialize a ControlMessage to bytes.
pub fn serialize_control(msg: &ControlMessage) -> Result<Vec<u8>, Box<bincode::ErrorKind>> {
    bincode::serialize(msg)
}

/// Build a new AudioPacket for sending.
pub fn build_packet(
    sequence_number: u64,
    timestamp_micros: u64,
    audio_data: Vec<i16>,
) -> AudioPacket {
    let frame_count = (audio_data.len() / 2) as u32; // stereo
    AudioPacket {
        sequence_number,
        timestamp_micros,
        frame_count,
        channels: 2,
        audio_data,
    }
}
