//! Cross-platform process-RSS helpers for integration tests.
//!
//! The bench framework measures the calling process's RSS via `memory-stats`.
//! Integration tests that spawn the daemon subprocess need to attach by PID
//! instead — `sysinfo` provides the cross-platform read.
//!
//! All readings report the *resident* set (physical memory), not virtual:
//! - Linux: VmRSS from /proc/{pid}/status
//! - Windows: WorkingSetSize from GetProcessMemoryInfo
//! - macOS: resident_size from task_info(MACH_TASK_BASIC_INFO)
//!
//! These have slightly different semantics across OSes (shared pages, anon
//! pages, etc.) — the absolute number is informational; assertions should
//! compare deltas from a baseline taken on the same process on the same OS.

use std::thread;
use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, System};

/// Default tolerance applied to settle assertions.
///
/// Sized to absorb Arrow allocator slack and tokio thread-pool growth without
/// masking a real leak. Tune the constant centrally — callers that want a
/// different bound should pass it explicitly to [`wait_until_rss_settles`].
pub const DEFAULT_RSS_TOLERANCE_BYTES: u64 = 50 * 1024 * 1024;

/// Sample the resident set size, in bytes, of the process with the given PID.
///
/// Returns `None` if the process is not visible to the caller, the OS does
/// not expose the metric, or sampling fails for any reason. Callers should
/// treat `None` as "skip the assertion" rather than as an error.
pub fn process_rss_bytes(pid: u32) -> Option<u64> {
    let mut sys = System::new();
    sys.refresh_process_specifics(Pid::from_u32(pid), ProcessRefreshKind::new().with_memory());
    sys.process(Pid::from_u32(pid)).map(|p| p.memory())
}

/// Poll `process_rss_bytes` until the reading is at or below
/// `target_bytes + tolerance` or `timeout` elapses. Returns the latest reading
/// when the condition is met, or `None` if the timeout was hit first.
///
/// Use this after an operation that should have released memory (cancellation,
/// stream drop, etc.) — RSS does not always return to baseline instantly
/// because allocators retain freed pages for reuse.
pub fn wait_until_rss_settles(
    pid: u32,
    target_bytes: u64,
    tolerance: u64,
    timeout: Duration,
) -> Option<u64> {
    let start = Instant::now();
    let mut last_seen = None;
    while start.elapsed() < timeout {
        if let Some(rss) = process_rss_bytes(pid) {
            last_seen = Some(rss);
            if rss <= target_bytes.saturating_add(tolerance) {
                return Some(rss);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    last_seen
}

/// Sample baseline RSS for a freshly-spawned process. Polls until the reading
/// is non-zero (the subprocess has actually allocated past startup), with a
/// short timeout — useful immediately after `Command::spawn` returns.
pub fn sample_baseline_rss(pid: u32, timeout: Duration) -> Option<u64> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(rss) = process_rss_bytes(pid) {
            if rss > 0 {
                return Some(rss);
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    None
}
