//! A small index for each thread that touches a cache, assigned on first use.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

static NEXT_THREAD_INDEX: AtomicUsize = AtomicUsize::new(0);
/// Bit `index & 63` is set for every thread index assigned so far.
static ASSIGNED_THREAD_SHARDS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_INDEX: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// The low six bits of every thread index assigned so far, as a mask. A
/// thread's bit is set, with a sequentially consistent read-modify-write,
/// before its index is first returned. A load, which may lag a recent
/// assignment: for pacing only.
#[inline]
pub(crate) fn assigned_thread_shards() -> u64 {
    ASSIGNED_THREAD_SHARDS.load(Ordering::Relaxed)
}

/// As [`assigned_thread_shards`], read with a zero-delta read-modify-write,
/// which observes every assignment before it in the mask's modification
/// order, where a load may not. Epoch advances use it to choose which
/// reader counters to scan (see `tests/cache_concurrency_model.rs`).
#[inline]
pub(crate) fn claimed_thread_shards() -> u64 {
    ASSIGNED_THREAD_SHARDS.fetch_or(0, Ordering::SeqCst)
}

/// This thread's index, drawn from a process-wide counter on its first call.
#[inline]
pub(crate) fn thread_index() -> usize {
    THREAD_INDEX.with(|thread| {
        let current = thread.get();
        if current != usize::MAX {
            return current;
        }
        let assigned = NEXT_THREAD_INDEX.fetch_add(1, Ordering::Relaxed);
        ASSIGNED_THREAD_SHARDS.fetch_or(1 << (assigned & 63), Ordering::SeqCst);
        thread.set(assigned);
        assigned
    })
}
