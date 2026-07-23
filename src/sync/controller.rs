use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::state::shared::{JitterBuffer, SharedApp, SharedRatio};

/// Run the synchronization controller on a dedicated thread.
///
/// Uses a PI (proportional-integral) controller to hold the jitter buffer at
/// its target fill level by nudging the playback resample ratio, and mirrors
/// the user's volume slider into the atomic the audio callback reads.
///
/// ### Control Logic
///
/// The resampler's `ratio` is *input frames consumed per output frame*:
/// - `ratio > 1.0` → consume input faster → drains the buffer (plays faster)
/// - `ratio < 1.0` → consume input slower → fills the buffer (plays slower)
///
/// With `error = fill_ms - target_fill`:
/// - buffer too full  (error > 0) → `ratio > 1.0` to drain it
/// - buffer too empty (error < 0) → `ratio < 1.0` to let it refill
pub fn run_sync_controller(
    jitter: Arc<JitterBuffer>,
    ratio: Arc<SharedRatio>,
    shared: SharedApp,
    volume: Arc<AtomicU32>,
    stop: Arc<AtomicBool>,
) {
    const KP: f64 = 0.0005;
    const KI: f64 = 0.00002;
    const INTERVAL_MS: u64 = 100;
    const RATIO_MIN: f64 = 0.998;
    const RATIO_MAX: f64 = 1.002;

    let mut integral: f64 = 0.0;
    let dt = INTERVAL_MS as f64 / 1000.0;

    tracing::info!("Sync controller started (Kp={KP}, Ki={KI}, interval={INTERVAL_MS}ms)");

    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(INTERVAL_MS));

        let fill_ms = jitter.fill_ms() as f64;

        // Read the user's target delay and volume from the UI in one lock.
        let (target_delay, vol) = {
            let app = shared.lock();
            (app.receiver.target_delay_ms as f64, app.receiver.volume)
        };
        volume.store(vol.to_bits(), Ordering::Relaxed);

        // Target buffer fill = half the configured delay (balances latency
        // against underrun protection), with a small floor.
        let target_fill = (target_delay * 0.5).max(10.0);
        let error = fill_ms - target_fill;

        // Anti-windup: only integrate when not saturated (or unwinding).
        let current_ratio = ratio.get();
        let not_saturated = current_ratio > RATIO_MIN && current_ratio < RATIO_MAX;
        if not_saturated || error.signum() != integral.signum() {
            integral = (integral + error * dt).clamp(-5.0, 5.0);
        }

        let correction = KP * error + KI * integral;
        let new_ratio = (1.0 + correction).clamp(RATIO_MIN, RATIO_MAX);
        ratio.set(new_ratio);

        // Reflect state back to the UI.
        {
            let mut app = shared.lock();
            app.receiver.current_speed_adjust = (new_ratio - 1.0) as f32;
            app.receiver.buffer_fill_ms = fill_ms as f32;
        }
    }

    tracing::info!("Sync controller stopped");
}
