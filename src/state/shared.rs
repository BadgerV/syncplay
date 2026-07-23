use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

// ─── Constants ─────────────────────────────────────────

pub const SAMPLE_RATE: u32 = 48_000;
pub const CHANNELS: u16 = 2;
pub const AUDIO_PORT: u16 = 12345;
pub const CONTROL_PORT: u16 = 12346;
pub const MDNS_SERVICE_TYPE: &str = "_syncplay._udp.local.";

/// Default synchronized-playout budget: the fixed delay, measured from the
/// moment audio is captured on the sender, at which *every* endpoint (the
/// source included) plays it. Must exceed worst-case network + device latency
/// so the buffer is primed by the time the deadline arrives. ~200 ms is the
/// same ballpark AirPlay 2 / Snapcast use.
pub const DEFAULT_PLAYOUT_DELAY_MS: u64 = 200;

// ─── App Mode ──────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppMode {
    Sender,
    #[default]
    Receiver,
}

// ─── Sender State ──────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SenderState {
    pub available_input_devices: Vec<String>,
    pub available_output_devices: Vec<String>,
    pub selected_input_device: String,
    pub is_streaming: bool,
    pub packets_sent: u64,
    pub bytes_sent: u64,
    pub peak_level: f32,
    pub receiver_count: usize,
}

impl Default for SenderState {
    fn default() -> Self {
        Self {
            available_input_devices: Vec::new(),
            available_output_devices: Vec::new(),
            selected_input_device: String::new(),
            is_streaming: false,
            packets_sent: 0,
            bytes_sent: 0,
            peak_level: 0.0,
            receiver_count: 0,
        }
    }
}

// ─── Receiver State ────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DiscoveredSender {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub sample_rate: u32,
    /// Advertised channel count (informational; playback always uses stereo).
    #[allow(dead_code)]
    pub channels: u16,
}

#[derive(Debug, Clone)]
pub struct ReceiverState {
    pub discovered_senders: Vec<DiscoveredSender>,
    pub connected_sender: Option<DiscoveredSender>,
    pub is_connected: bool,
    pub packets_received: u64,
    pub packets_lost: u64,
    pub target_delay_ms: f32,
    pub volume: f32,
    pub current_speed_adjust: f32,
    pub buffer_fill_ms: f32,
    pub peak_level: f32,
    pub underruns: u64,
    pub available_output_devices: Vec<String>,
    pub selected_output_device: String,
}

impl Default for ReceiverState {
    fn default() -> Self {
        Self {
            discovered_senders: Vec::new(),
            connected_sender: None,
            is_connected: false,
            packets_received: 0,
            packets_lost: 0,
            target_delay_ms: 50.0,
            volume: 1.0,
            current_speed_adjust: 0.0,
            buffer_fill_ms: 0.0,
            peak_level: 0.0,
            underruns: 0,
            available_output_devices: Vec::new(),
            selected_output_device: String::new(),
        }
    }
}

// ─── Shared Ratio ──────────────────────────────────────

pub struct SharedRatio {
    raw: AtomicU64,
}

impl SharedRatio {
    pub fn new(initial: f64) -> Self {
        Self {
            raw: AtomicU64::new(initial.to_bits()),
        }
    }

    pub fn set(&self, v: f64) {
        self.raw.store(v.to_bits(), Ordering::Release);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.raw.load(Ordering::Acquire))
    }
}

// ─── Jitter Buffer (lock-free-ish, using parking_lot Mutex) ──

/// A thread-safe jitter buffer backed by a VecDeque.
/// The Mutex is held briefly — the producer (network) pushes audio packets,
/// the consumer (audio callback) pops samples every ~10ms.
pub struct JitterBuffer {
    inner: Mutex<VecDeque<i16>>,
    pub total_written: AtomicU64,
    pub total_read: AtomicU64,
    pub last_sequence: AtomicU64,
    pub packets_lost: AtomicU64,
    pub underruns: AtomicU64,
}

impl JitterBuffer {
    pub fn new(capacity_frames: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity_frames * 2)),
            total_written: AtomicU64::new(0),
            total_read: AtomicU64::new(0),
            last_sequence: AtomicU64::new(u64::MAX),
            packets_lost: AtomicU64::new(0),
            underruns: AtomicU64::new(0),
        }
    }

    pub fn push_packet(&self, data: &[i16]) {
        let mut buf = self.inner.lock();
        buf.extend(data.iter());
        self.total_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
    }

    pub fn pop_samples(&self, out: &mut [i16]) -> usize {
        let mut buf = self.inner.lock();
        let available = buf.len().min(out.len());
        if available < out.len() {
            self.underruns.fetch_add(1, Ordering::Relaxed);
            // Fill missing portion with silence
            out[available..].fill(0);
        }
        for slot in out.iter_mut().take(available) {
            *slot = buf.pop_front().unwrap_or(0);
        }
        self.total_read
            .fetch_add(available as u64, Ordering::Relaxed);
        available
    }

    pub fn fill_ms(&self) -> f32 {
        let buf = self.inner.lock();
        buf.len() as f32 / (2.0 * 48.0)
    }

    pub fn record_sequence(&self, seq: u64) {
        let prev = self.last_sequence.swap(seq, Ordering::AcqRel);
        // Only count forward gaps; ignore the initial value and any reordering.
        if prev != u64::MAX && seq > prev && seq - prev > 1 {
            self.packets_lost
                .fetch_add(seq - prev - 1, Ordering::Relaxed);
        }
    }
}

// ─── Playout Gate ──────────────────────────────────────

/// Holds an audio output silent until a scheduled wall-clock deadline, then
/// latches open forever. This is the mechanism that makes playback start at the
/// *same instant* on every endpoint: each side arms the gate with its own
/// local deadline (`capture_ts − clock_offset + budget`) and the output stays
/// muted — without draining the jitter buffer — until that moment arrives.
pub struct PlayoutGate {
    /// Local-clock microseconds at which to begin playing. 0 = not yet armed.
    start_deadline_us: AtomicU64,
    /// Latched once the deadline has passed.
    started: AtomicBool,
}

impl PlayoutGate {
    pub fn new() -> Self {
        Self {
            start_deadline_us: AtomicU64::new(0),
            started: AtomicBool::new(false),
        }
    }

    /// Arm the gate with a local-clock deadline. Only the first call takes
    /// effect (the first audio chunk anchors the whole stream).
    pub fn arm(&self, deadline_us: u64) {
        let _ = self.start_deadline_us.compare_exchange(
            0,
            deadline_us.max(1),
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    pub fn is_armed(&self) -> bool {
        self.start_deadline_us.load(Ordering::Relaxed) != 0
    }

    pub fn deadline_us(&self) -> u64 {
        self.start_deadline_us.load(Ordering::Relaxed)
    }

    /// Whether the output should be producing audio at `now_us`. Latches true.
    pub fn should_play(&self, now_us: u64) -> bool {
        if self.started.load(Ordering::Relaxed) {
            return true;
        }
        let deadline = self.start_deadline_us.load(Ordering::Relaxed);
        if deadline != 0 && now_us >= deadline {
            self.started.store(true, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn has_started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }
}

impl Default for PlayoutGate {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Thread Handles ────────────────────────────────────

pub struct SenderThreads {
    pub stop: Arc<AtomicBool>,
    // Kept so the engine thread stays owned for the session's lifetime.
    #[allow(dead_code)]
    pub net_handle: Option<std::thread::JoinHandle<()>>,
}

pub struct ReceiverThreads {
    pub stop: Arc<AtomicBool>,
    // Kept so the engine/sync threads stay owned for the session's lifetime.
    #[allow(dead_code)]
    pub net_handle: Option<std::thread::JoinHandle<()>>,
    #[allow(dead_code)]
    pub sync_handle: Option<std::thread::JoinHandle<()>>,
}

// ─── App Root State ────────────────────────────────────

pub struct AppState {
    pub mode: AppMode,
    pub sender: SenderState,
    pub receiver: ReceiverState,
    pub sender_threads: Option<SenderThreads>,
    pub receiver_threads: Option<ReceiverThreads>,
}

impl AppState {
    pub fn new(mode: AppMode) -> Self {
        Self {
            mode,
            sender: SenderState::default(),
            receiver: ReceiverState::default(),
            sender_threads: None,
            receiver_threads: None,
        }
    }
}

// ─── Shared App Handle ─────────────────────────────────

pub type SharedApp = Arc<Mutex<AppState>>;
