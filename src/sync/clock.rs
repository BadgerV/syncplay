//! Shared monotonic clock and cross-machine offset estimation.
//!
//! For synchronized ("play at the same instant") playback we need two things:
//!
//! 1. A single monotonic clock per process — [`now_us`] — used both to stamp
//!    outgoing audio packets *and* to answer time-sync queries, so the two are
//!    always on the same timescale.
//! 2. An estimate of the offset between the sender's clock and the receiver's
//!    clock, measured with an SNTP-style round trip: [`ClockSync`].

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

/// Maximum round-trip time for a clock sample we're willing to anchor playout
/// on. rtt/2 bounds the offset error, so 40 ms ⇒ ≤20 ms alignment error.
pub const GOOD_RTT_US: u64 = 40_000;

/// Process-wide epoch. Captured once, lazily, on first use.
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Microseconds since this process's epoch. Monotonic, cheap, allocation-free —
/// safe to call from an audio callback.
pub fn now_us() -> u64 {
    epoch().elapsed().as_micros() as u64
}

/// Estimates the offset between a remote (sender) clock and the local
/// (receiver) clock using SNTP-style round trips.
///
/// `offset = sender_clock − receiver_clock`. To convert a sender timestamp
/// `ts` into local time: `local = ts − offset`.
///
/// We keep the sample with the smallest round-trip time, which is the least
/// contaminated by queuing/scheduling jitter — the standard NTP/Snapcast trick.
pub struct ClockSync {
    offset_us: AtomicI64,
    best_rtt_us: AtomicU64,
    synced: AtomicBool,
}

impl ClockSync {
    pub fn new() -> Self {
        Self {
            offset_us: AtomicI64::new(0),
            best_rtt_us: AtomicU64::new(u64::MAX),
            synced: AtomicBool::new(false),
        }
    }

    /// Fold in one round-trip sample.
    ///
    /// * `client_send_us` — local time the request left.
    /// * `server_us`      — remote time when the request was received.
    /// * `client_recv_us` — local time the reply arrived.
    ///
    /// Assuming a symmetric path, the remote stamped `server_us` when the local
    /// clock read `client_send_us + rtt/2`, so
    /// `offset = server_us − (client_send_us + rtt/2)`.
    pub fn update(&self, client_send_us: u64, server_us: u64, client_recv_us: u64) {
        let rtt = client_recv_us.saturating_sub(client_send_us);
        // Keep only improvements on the best (lowest-RTT) sample. Allow a small
        // slack so a persistently changing offset can still be tracked once the
        // very-best sample ages out is not needed here — sessions are short.
        if rtt <= self.best_rtt_us.load(Ordering::Relaxed) {
            let midpoint = client_send_us as i64 + (rtt as i64) / 2;
            let offset = server_us as i64 - midpoint;
            self.offset_us.store(offset, Ordering::Relaxed);
            self.best_rtt_us.store(rtt, Ordering::Relaxed);
            self.synced.store(true, Ordering::Relaxed);
        }
    }

    /// `sender_clock − receiver_clock`, in microseconds.
    pub fn offset_us(&self) -> i64 {
        self.offset_us.load(Ordering::Relaxed)
    }

    /// Best round-trip time seen so far (µs); `u64::MAX` until first sample.
    pub fn best_rtt_us(&self) -> u64 {
        self.best_rtt_us.load(Ordering::Relaxed)
    }

    /// True once at least one sample has been folded in.
    pub fn is_synced(&self) -> bool {
        self.synced.load(Ordering::Relaxed)
    }

    /// True once we have a *trustworthy* offset: a sample whose round-trip is
    /// low enough that the offset error (≈ rtt/2) is acceptable for anchoring.
    /// Anchoring off a high-RTT sample puts the playout deadline in the wrong
    /// place, so we wait for this before committing the start instant.
    pub fn is_good(&self) -> bool {
        self.synced.load(Ordering::Relaxed) && self.best_rtt_us() <= GOOD_RTT_US
    }

    /// Convert a remote (sender-clock) timestamp into local (receiver) time.
    pub fn remote_to_local_us(&self, remote_us: u64) -> i64 {
        remote_us as i64 - self.offset_us()
    }
}

impl Default for ClockSync {
    fn default() -> Self {
        Self::new()
    }
}
