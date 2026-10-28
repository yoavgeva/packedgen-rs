//! Epoch-reclaimed slab storage for `ShardedCache` entries.
//!
//! Readers and writers pin an epoch with the direct arena's reader protocol
//! and its shared state machine in `direct_epoch_protocol`. On Linux a
//! thread announces its pins on a line of its own instead, and keeps the
//! announcement parked between them, so a pin is a few plain stores (see
//! [`Announcement`]). Entries live in one slab region per cache rather than
//! in individual allocations, with overflow chunks for bursts past the
//! region (see `SlotStore`):
//!
//! - Writers take vacant slots from their thread's stack for the cache, and
//!   write entries into them before taking the shard lock to publish.
//! - A writer that unpublishes a slot queues it in its shard with the epoch
//!   generation read after the unpublication, while its pin from before the
//!   unpublication is still held.
//! - The writer's thread reclaims a queued slot once the generation has
//!   advanced three times past its tag: the advance that reuses the tag's
//!   epoch slot found that slot and the one after it quiescent, which is the
//!   direct arena's two-rotation aging. Each write reclaims as many slots as
//!   it retires, twice as many while a backlog is due, and other writers
//!   drain the queue of a thread that stopped retiring.
//! - Values are dropped outside the shard lock by the reclaiming thread, and
//!   their slots join that thread's stack, so it refills slots it just
//!   touched. Each write prefetches the slot its thread reclaims next.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering, compiler_fence, fence};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;

use crate::cache_arena::{assigned_thread_shards, claimed_thread_shards, direct_arena_thread};
pub(crate) use crate::direct_epoch_protocol::reclaim_at;
use crate::direct_epoch_protocol::{
    Announced, EPOCHS, EVICTED, EVICTING, announce_afresh, classify_announcement, enter_epoch,
    epoch_slot, eviction_outcome, leave_announcement, leave_epoch, plan_epoch_advance,
    reuse_announcement,
};

/// Reader counters are spread over this many cache lines per epoch.
const READER_SHARDS: usize = 64;
/// As for reference counts: a process aborts long before a live count can
/// wrap, even with leaked pins.
const MAX_READERS: usize = isize::MAX as usize;
/// The largest weight one entry can account. The weight word's top bit is the
/// second access mark.
pub(crate) const MAX_WEIGHT: u32 = u32::MAX >> 1;
/// The largest deadline tick; all ones in the 31-bit field means none.
pub(crate) const MAX_EXPIRY_TICK: u64 = 0x7fff_fffe;
pub(crate) const NEVER_EXPIRES: u64 = u64::MAX;
const WEIGHT_MASK: u64 = MAX_WEIGHT as u64;
const SECOND_ACCESS_BIT: u64 = 1 << 31;
const EXPIRY_SHIFT: u32 = 32;
const EXPIRY_MASK: u64 = 0x7fff_ffff << EXPIRY_SHIFT;
const ACCESS_BIT: u64 = 1 << 63;
/// Retirements across the cache between tries to advance the generation. A
/// try scans every reader counter, so trying per shard write would cost more
/// than the writes.
const ADVANCE_AFTER: usize = 512;
/// Retirements per reader shard in use before an advance is tried.
const ADVANCE_PER_SHARD: usize = 32;

/// Retirements pending across a cache before an advance is tried: 32 per
/// reader shard any thread has used, at most `ADVANCE_AFTER`.
#[inline]
fn advance_after() -> usize {
    (ADVANCE_PER_SHARD * assigned_thread_shards().count_ones() as usize)
        .clamp(ADVANCE_PER_SHARD, ADVANCE_AFTER)
}

#[repr(align(128))]
struct ReaderCount(AtomicUsize);

#[repr(align(128))]
struct PendingCount(AtomicUsize);

/// Threads that hold one of these slots announce their pins on the slot's
/// line in each cache, with plain stores (see [`Announcement`]). Others
/// count their pins in the shared reader counters.
const ANNOUNCERS: usize = 64;
/// `ANNOUNCER`'s low byte: the thread's slot, or one of these.
const ANNOUNCER_SLOT: usize = 0xff;
/// The thread pins through the shared reader counters.
const SHARED_PINNER: usize = 0xfe;
/// The thread has not pinned yet.
const UNCLAIMED: usize = 0xff;
/// `ANNOUNCER` counts the pins its thread holds through its slot from here.
const ANNOUNCER_PIN: usize = 1 << 8;

/// Bit `slot` is set while a thread holds announcer slot `slot`.
static ANNOUNCERS_HELD: AtomicU64 = AtomicU64::new(0);
/// Bit `index & 63` is set before a thread first pins through the shared
/// counters, as the cache arena's thread mask is.
static SHARED_PINNERS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// The thread's announcer slot and the pins it holds through it.
    static ANNOUNCER: Cell<usize> = const { Cell::new(UNCLAIMED) };
    /// Returns the thread's slot when the thread exits.
    static ANNOUNCER_RELEASE: AnnouncerRelease = const { AnnouncerRelease };
}

/// Releases its thread's announcer slot at thread exit, unless a pin through
/// it is still held: another thread-local's destructor may drop that pin
/// later, and the slot then stays held for good. Pins taken after it find
/// no slot to claim and use the shared counters.
struct AnnouncerRelease;

impl Drop for AnnouncerRelease {
    fn drop(&mut self) {
        let state = ANNOUNCER.get();
        ANNOUNCER.set(state | UNCLAIMED);
        let slot = state & ANNOUNCER_SLOT;
        if slot < ANNOUNCERS && state < ANNOUNCER_PIN {
            ANNOUNCERS_HELD.fetch_and(!(1 << slot), Ordering::Release);
        }
    }
}

/// Sets this thread's bit in `SHARED_PINNERS`, sequentially consistently,
/// before its first pin through the shared counters.
fn claim_shared_pinner() {
    SHARED_PINNERS.fetch_or(1 << (direct_arena_thread() & 63), Ordering::SeqCst);
}

/// The shared pinners, read with a zero-delta read-modify-write, which
/// observes every claim before it in the mask's modification order (see
/// `tests/cache_concurrency_model.rs`). Builds that cannot announce pin
/// every thread through the shared counters, by its arena thread index.
fn shared_pinners() -> u64 {
    if ANNOUNCEABLE {
        SHARED_PINNERS.fetch_or(0, Ordering::SeqCst)
    } else {
        claimed_thread_shards()
    }
}

/// Whether pins that reuse an announcement need only a compiler fence,
/// with advances issuing `membarrier` to evict a parked announcement.
const ASYMMETRIC: bool = cfg!(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(miri)
));

/// Whether this build can announce pins: on Linux, where `membarrier`
/// decides at run time, and in tests and Miri elsewhere. Other builds
/// compile the shared counters alone, as before announcements.
const ANNOUNCEABLE: bool = ASYMMETRIC || cfg!(any(test, miri));

/// Whether this process announces pins: where `membarrier` registers, and
/// in tests and Miri elsewhere, with sequentially consistent fences on both
/// sides in its place.
fn announcements_supported() -> bool {
    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        if ASYMMETRIC {
            membarrier::register()
        } else {
            cfg!(any(test, miri))
        }
    })
}

/// The fence a pin issues after marking a parked announcement active.
#[inline]
fn light_fence() {
    if ASYMMETRIC {
        compiler_fence(Ordering::SeqCst);
    } else {
        fence(Ordering::SeqCst);
    }
}

/// The fence an advance issues before deciding a parked announcement is
/// evicted: with `light_fence`, it orders as two sequentially consistent
/// fences would. Returns false if the barrier failed.
fn heavy_fence() -> bool {
    if ASYMMETRIC {
        membarrier::private_expedited()
    } else {
        fence(Ordering::SeqCst);
        true
    }
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(miri)
))]
#[allow(unsafe_code, reason = "issues the membarrier system call")]
mod membarrier {
    unsafe extern "C" {
        fn syscall(number: i64, ...) -> i64;
    }
    #[cfg(target_arch = "x86_64")]
    const SYS_MEMBARRIER: i64 = 324;
    #[cfg(target_arch = "aarch64")]
    const SYS_MEMBARRIER: i64 = 283;
    const CMD_QUERY: i64 = 0;
    const CMD_PRIVATE_EXPEDITED: i64 = 1 << 3;
    const CMD_REGISTER_PRIVATE_EXPEDITED: i64 = 1 << 4;

    fn membarrier(command: i64) -> i64 {
        // SAFETY: membarrier reads no memory of this process; its arguments
        // are plain integers.
        unsafe { syscall(SYS_MEMBARRIER, command, 0_i64, 0_i64) }
    }

    /// Registers this process for private expedited barriers; returns
    /// whether the kernel supports and accepted it.
    pub(super) fn register() -> bool {
        let supported = membarrier(CMD_QUERY);
        supported >= 0
            && supported & CMD_PRIVATE_EXPEDITED != 0
            && membarrier(CMD_REGISTER_PRIVATE_EXPEDITED) == 0
    }

    /// Makes every running thread of this process execute a full memory
    /// barrier before returning. Re-registers once if the kernel refuses,
    /// as a forked child's may.
    pub(super) fn private_expedited() -> bool {
        membarrier(CMD_PRIVATE_EXPEDITED) == 0
            || (register() && membarrier(CMD_PRIVATE_EXPEDITED) == 0)
    }
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(miri)
)))]
mod membarrier {
    pub(super) fn register() -> bool {
        false
    }

    pub(super) fn private_expedited() -> bool {
        false
    }
}

/// An announcer slot's line in one cache.
///
/// A thread announces the epoch it pins in `word`, and leaves the
/// announcement parked when its last pin drops, so its next pin only marks it
/// active again: a store, a [`light_fence`], and loads of `evicted` and the
/// generation. An advance that a parked announcement blocks evicts it:
/// it sets `evicted`, issues a [`heavy_fence`], and finds the announcement
/// still parked, so any pin that marks it active afterwards sees `evicted`
/// and announces afresh. A fresh announcement, made when the epoch has moved
/// or after an eviction, stores and issues a sequentially consistent fence,
/// as the shared counters' read-modify-writes order.
#[repr(C, align(128))]
struct Announcement {
    /// Written only by the slot's thread: `pins << 3 | ANNOUNCED | epoch`.
    word: AtomicUsize,
    /// `EVICTING` or `EVICTED` once an advance evicts the parked
    /// announcement; the slot's thread clears it after announcing afresh.
    evicted: AtomicUsize,
}

/// The epoch clock a cache's readers and writers pin.
pub(crate) struct Epochs {
    /// Generation token: its low two bits name the current epoch slot.
    generation: AtomicUsize,
    /// `readers[epoch * READER_SHARDS + shard]`.
    readers: Box<[ReaderCount]>,
    /// One line per announcer slot; empty where announcements are
    /// unsupported.
    announcements: Box<[Announcement]>,
    /// Serializes advances; a writer that finds it held skips advancing.
    advancing: Mutex<()>,
    /// Retirements reported since the generation last advanced.
    pending: PendingCount,
}

impl Epochs {
    /// Bytes of the reader counters and announcement lines.
    pub(crate) fn bytes_used(&self) -> usize {
        size_of_val(&*self.readers) + size_of_val(&*self.announcements)
    }

    pub(crate) fn new() -> Self {
        let announcers = if announcements_supported() {
            ANNOUNCERS
        } else {
            0
        };
        Self {
            generation: AtomicUsize::new(0),
            readers: (0..EPOCHS * READER_SHARDS)
                .map(|_| ReaderCount(AtomicUsize::new(0)))
                .collect(),
            announcements: (0..announcers)
                .map(|_| Announcement {
                    word: AtomicUsize::new(0),
                    evicted: AtomicUsize::new(0),
                })
                .collect(),
            advancing: Mutex::new(()),
            pending: PendingCount(AtomicUsize::new(0)),
        }
    }

    /// Reports `count` retirements; returns whether enough are pending
    /// across the cache to try advancing the generation.
    #[inline]
    pub(crate) fn note_retired(&self, count: usize) -> bool {
        self.pending.0.fetch_add(count, Ordering::Relaxed) + count >= advance_after()
    }

    /// Pins the current epoch: slots unpublished after this returns are not
    /// reclaimed until the pin is dropped or refreshed.
    #[cfg_attr(
        any(
            all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ),
            test,
            miri
        ),
        allow(
            clippy::inline_always,
            reason = "out of line, every read calls it and receives the pin through memory"
        ),
        inline(always)
    )]
    #[cfg_attr(
        not(any(
            all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ),
            test,
            miri
        )),
        inline
    )]
    pub(crate) fn pin(&self) -> EpochPin<'_> {
        #[cfg(test)]
        PINS_HELD.set(PINS_HELD.get() + 1);
        #[cfg(test)]
        PINS_TAKEN.set(PINS_TAKEN.get() + 1);
        if !ANNOUNCEABLE {
            return self.pin_shared();
        }
        let state = ANNOUNCER.get();
        let slot = state & ANNOUNCER_SLOT;
        if let Some(announcement) = self.announcements.get(slot) {
            ANNOUNCER.set(state + ANNOUNCER_PIN);
            return EpochPin {
                epochs: self,
                epoch: self.announce(announcement),
                reader: ANNOUNCED_READER | slot,
                _not_send: PhantomData,
            };
        }
        if slot == UNCLAIMED {
            return self.pin_unclaimed();
        }
        self.pin_shared()
    }

    /// Claims this thread's announcer slot, or else its shared bit, then
    /// pins through it.
    #[cold]
    #[inline(never)]
    fn pin_unclaimed(&self) -> EpochPin<'_> {
        claim_announcer();
        let state = ANNOUNCER.get();
        let slot = state & ANNOUNCER_SLOT;
        if let Some(announcement) = self.announcements.get(slot) {
            ANNOUNCER.set(state + ANNOUNCER_PIN);
            return EpochPin {
                epochs: self,
                epoch: self.announce(announcement),
                reader: ANNOUNCED_READER | slot,
                _not_send: PhantomData,
            };
        }
        self.pin_shared()
    }

    /// Pins through the shared counters.
    #[inline]
    fn pin_shared(&self) -> EpochPin<'_> {
        let shard = direct_arena_thread() & (READER_SHARDS - 1);
        EpochPin {
            epochs: self,
            epoch: self.enter(shard),
            reader: shard,
            _not_send: PhantomData,
        }
    }

    /// Pins through this thread's announcement, reusing its epoch if it is
    /// parked in the current one and not evicted.
    #[allow(clippy::inline_always, reason = "the body of every announced pin")]
    #[inline(always)]
    fn announce(&self, announcement: &Announcement) -> usize {
        reuse_announcement(
            announcement.word.load(Ordering::Relaxed),
            |word| announcement.word.store(word, Ordering::Relaxed),
            light_fence,
            || announcement.evicted.load(Ordering::Relaxed),
            || self.generation.load(Ordering::Relaxed),
        )
        .unwrap_or_else(
            #[cold]
            || self.announce_afresh(announcement),
        )
    }

    #[cold]
    #[inline(never)]
    fn announce_afresh(&self, announcement: &Announcement) -> usize {
        #[cfg(test)]
        ORDERED_PINS.set(ORDERED_PINS.get() + 1);
        announce_afresh(
            || self.generation.load(Ordering::Relaxed),
            // Release: an advance that reads the new epoch takes the thread
            // to have left its old one, so its reads under that one must
            // happen before; a relaxed store would not carry the release
            // of the unpin before it.
            |word| announcement.word.store(word, Ordering::Release),
            || fence(Ordering::SeqCst),
            || announcement.evicted.load(Ordering::Relaxed),
            || announcement.evicted.store(0, Ordering::Relaxed),
        )
    }

    #[inline]
    fn enter(&self, shard: usize) -> usize {
        #[cfg(test)]
        ORDERED_PINS.set(ORDERED_PINS.get() + 1);
        enter_epoch(
            || self.generation.load(Ordering::SeqCst),
            |epoch| {
                let previous = self.readers[epoch * READER_SHARDS + shard]
                    .0
                    .fetch_add(1, Ordering::SeqCst);
                if previous >= MAX_READERS {
                    std::process::abort();
                }
            },
            |epoch| {
                self.readers[epoch * READER_SHARDS + shard]
                    .0
                    .fetch_sub(1, Ordering::SeqCst)
            },
        )
    }

    #[inline]
    fn leave(&self, epoch: usize, reader: usize) {
        if ANNOUNCEABLE
            && let Some(announcement) = self.announcements.get(reader ^ ANNOUNCED_READER)
        {
            let word = leave_announcement(announcement.word.load(Ordering::Relaxed));
            announcement.word.store(word, Ordering::Release);
            ANNOUNCER.set(ANNOUNCER.get() - ANNOUNCER_PIN);
            return;
        }
        leave_epoch(epoch, |epoch| {
            self.readers[epoch * READER_SHARDS + reader]
                .0
                .fetch_sub(1, Ordering::SeqCst)
        });
    }

    /// The generation to tag slots unpublished before this call.
    #[inline]
    pub(crate) fn generation(&self) -> usize {
        self.generation.load(Ordering::SeqCst)
    }

    /// Advances the generation if no reader remains in the epoch slots the
    /// advance retires; returns whether it advanced. Skips when another
    /// thread is advancing.
    pub(crate) fn try_advance(&self) -> bool {
        let Some(_advancing) = self.advancing.try_lock() else {
            return false;
        };
        let generation = self.generation.load(Ordering::SeqCst);
        // Read after the generation, and with a read-modify-write, as the
        // counters are: a reader in an epoch this advance must find quiescent
        // claimed its shard before loading the generation that an earlier
        // advance replaced.
        let shards = shared_pinners();
        // Pairs with the fence of a fresh announcement: either this advance
        // reads it, or that pin reads this generation or a later one.
        if ANNOUNCEABLE {
            fence(Ordering::SeqCst);
        }
        let announcers = if self.announcements.is_empty() {
            0
        } else {
            ANNOUNCERS_HELD.load(Ordering::Relaxed)
        };
        let mut evicting = 0_u64;
        let Some(advance) = plan_epoch_advance(generation, |epoch| {
            let counters = &self.readers[epoch * READER_SHARDS..(epoch + 1) * READER_SHARDS];
            let mut remaining = shards;
            while remaining != 0 {
                let shard = remaining.trailing_zeros() as usize;
                remaining &= remaining - 1;
                // A zero-delta RMW observes the immediately preceding counter
                // modification, which a load racing a registration may not.
                if counters[shard].0.fetch_add(0, Ordering::SeqCst) != 0 {
                    return false;
                }
            }
            let mut remaining = announcers;
            while remaining != 0 {
                let slot = remaining.trailing_zeros() as usize;
                remaining &= remaining - 1;
                let announcement = &self.announcements[slot];
                match classify_announcement(
                    announcement.word.load(Ordering::Acquire),
                    || announcement.evicted.load(Ordering::Relaxed),
                    epoch,
                ) {
                    Announced::Clear => {}
                    Announced::Blocks => return false,
                    Announced::Evict => evicting |= 1 << slot,
                }
            }
            true
        }) else {
            // Readers still hold an old epoch: wait for half as many more
            // retirements before scanning again.
            self.pending.0.store(advance_after() / 2, Ordering::Relaxed);
            return false;
        };
        if evicting != 0 && !self.evict(evicting, generation) {
            self.pending.0.store(advance_after() / 2, Ordering::Relaxed);
            return false;
        }
        self.pending.0.store(0, Ordering::Relaxed);
        self.generation
            .compare_exchange(
                generation,
                advance.next_generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    /// Evicts the parked announcements in `slots`, which block the advance
    /// from `generation`; returns false if one turned active first.
    #[cold]
    fn evict(&self, slots: u64, generation: usize) -> bool {
        let mut remaining = slots;
        while remaining != 0 {
            let slot = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            self.announcements[slot]
                .evicted
                .store(EVICTING, Ordering::Relaxed);
        }
        if !heavy_fence() {
            return false;
        }
        let current = epoch_slot(generation);
        let mut remaining = slots;
        while remaining != 0 {
            let slot = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            let announcement = &self.announcements[slot];
            match eviction_outcome(announcement.word.load(Ordering::Acquire), current) {
                // A pin marked it active before the barrier.
                None => return false,
                Some(true) => announcement.evicted.store(EVICTED, Ordering::Relaxed),
                Some(false) => {}
            }
        }
        true
    }
}

/// `EpochPin::reader` for a pin through announcer slot `reader ^ ANNOUNCED_READER`.
const ANNOUNCED_READER: usize = 1 << 16;

/// Claims an announcer slot for this thread, or a shared bit if none is
/// free or announcements are unsupported.
#[cold]
fn claim_announcer() {
    let mut held = ANNOUNCERS_HELD.load(Ordering::Relaxed);
    if announcements_supported() && ANNOUNCER_RELEASE.try_with(|_| ()).is_ok() {
        while held != u64::MAX {
            let slot = held.trailing_ones() as usize;
            match ANNOUNCERS_HELD.compare_exchange_weak(
                held,
                held | 1 << slot,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // An advance that misses the claim in the mask orders
                    // after this fence, and pins read its generation.
                    fence(Ordering::SeqCst);
                    ANNOUNCER.set(ANNOUNCER.get() & !ANNOUNCER_SLOT | slot);
                    return;
                }
                Err(current) => held = current,
            }
        }
    }
    claim_shared_pinner();
    ANNOUNCER.set(ANNOUNCER.get() & !ANNOUNCER_SLOT | SHARED_PINNER);
}

/// A pinned epoch; see [`Epochs::pin`].
pub(crate) struct EpochPin<'epochs> {
    epochs: &'epochs Epochs,
    epoch: usize,
    /// The shared counters' shard, or `ANNOUNCED_READER` and an announcer
    /// slot.
    reader: usize,
    _not_send: PhantomData<*mut ()>,
}

impl EpochPin<'_> {
    /// Leaves and re-enters the current epoch, letting slots unpublished
    /// since the pin be reclaimed.
    pub(crate) fn refresh(&mut self) {
        self.epochs.leave(self.epoch, self.reader);
        if ANNOUNCEABLE
            && let Some(announcement) = self
                .epochs
                .announcements
                .get(self.reader ^ ANNOUNCED_READER)
        {
            ANNOUNCER.set(ANNOUNCER.get() + ANNOUNCER_PIN);
            self.epoch = self.epochs.announce(announcement);
        } else {
            self.epoch = self.epochs.enter(self.reader);
        }
    }

    /// Unpins, as dropping the pin does. Writes end with it: their drop of
    /// the pin stayed an out-of-line call.
    #[allow(clippy::inline_always, reason = "every write unpins")]
    #[inline(always)]
    pub(crate) fn unpin(self) {
        let pin = std::mem::ManuallyDrop::new(self);
        pin.epochs.leave(pin.epoch, pin.reader);
        #[cfg(test)]
        PINS_HELD.set(PINS_HELD.get() - 1);
    }
}

impl Drop for EpochPin<'_> {
    #[inline]
    fn drop(&mut self) {
        self.epochs.leave(self.epoch, self.reader);
        #[cfg(test)]
        PINS_HELD.set(PINS_HELD.get() - 1);
    }
}

#[cfg(test)]
thread_local! {
    /// The epoch pins this thread holds.
    static PINS_HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The epoch pins this thread has taken.
    static PINS_TAKEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Pins this thread took with a read-modify-write or a sequentially
    /// consistent fence.
    static ORDERED_PINS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many epoch pins this thread has taken, for tests of work sharing
/// one.
#[cfg(test)]
pub(crate) fn pins_taken() -> usize {
    PINS_TAKEN.get()
}

/// How many epoch pins this thread holds, for tests of work kept outside
/// them.
#[cfg(test)]
pub(crate) fn pins_held() -> usize {
    PINS_HELD.get()
}

/// Asks Linux to back the whole huge pages within `length` bytes from
/// `start` with transparent huge pages. Many distributions enable them only
/// where asked, and a large cache's table and slab region otherwise take a
/// TLB entry per 4 KB. Elsewhere, or for less than a huge page, it does
/// nothing; a kernel that refuses the advice leaves the pages as they were.
#[allow(
    unsafe_code,
    reason = "advises the kernel about memory this process owns"
)]
pub(crate) fn advise_huge_pages(start: *const u8, length: usize) {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(miri)
    ))]
    {
        /// The `x86_64` and `aarch64` 4 KB-page huge page size. Every base page
        /// size divides it, so the advised range is page-aligned on kernels
        /// with larger base pages too.
        const HUGE_PAGE: usize = 2 << 20;
        const MADV_HUGEPAGE: i32 = 14;
        unsafe extern "C" {
            fn madvise(address: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
        }
        let first = start.addr().next_multiple_of(HUGE_PAGE);
        let end = (start.addr() + length) / HUGE_PAGE * HUGE_PAGE;
        if end > first {
            // SAFETY: the range lies within an allocation the caller owns, and
            // the advice changes no contents. A failure is ignored: the pages
            // stay as they were.
            let _ = unsafe {
                madvise(
                    start.cast_mut().with_addr(first).cast(),
                    end - first,
                    MADV_HUGEPAGE,
                )
            };
        }
    }
    #[cfg(not(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(miri)
    )))]
    let _ = (start, length);
}

/// Starts loading the cache line at `address`. A prefetch reads nothing in
/// the language's sense, so any address is allowed, a stale one included.
#[inline]
#[allow(unsafe_code, reason = "issues a prefetch hint")]
pub(crate) fn prefetch<T>(address: *const T) {
    #[cfg(test)]
    PREFETCHED.with(|lines| lines.borrow_mut().push(address as usize));
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    // SAFETY: a prefetch hint never faults and changes no program state.
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(address.cast());
    }
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    // SAFETY: as above.
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{address}]",
            address = in(reg) address,
            options(nostack, preserves_flags, readonly)
        );
    }
    #[cfg(any(miri, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
    let _ = address;
}

/// Like [`prefetch`], for a line about to be written: fetching it for
/// ownership saves the upgrade a write after a read would wait for.
#[inline]
#[allow(unsafe_code, reason = "issues a prefetch hint")]
pub(crate) fn prefetch_write<T>(address: *const T) {
    #[cfg(test)]
    PREFETCHED.with(|lines| lines.borrow_mut().push(address as usize));
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    // SAFETY: a prefetch hint never faults and changes no program state.
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_ET0 }>(address.cast());
    }
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    // SAFETY: as above.
    unsafe {
        core::arch::asm!(
            "prfm pstl1keep, [{address}]",
            address = in(reg) address,
            options(nostack, preserves_flags, readonly)
        );
    }
    #[cfg(any(miri, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
    let _ = address;
}

#[cfg(test)]
thread_local! {
    /// Addresses this thread prefetched, for tests of what writers fetch.
    pub(crate) static PREFETCHED: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
    /// Times this thread searched its slot stacks for a store's.
    pub(crate) static STACK_SEARCHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Times this thread borrowed its slot stacks' `RefCell`.
    pub(crate) static STACK_BORROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// One entry's storage: atomic metadata, then the value.
///
/// Metadata holds the weight in bits 0 to 30, the second access mark in bit
/// 31, the deadline tick in bits 32 to 62 (all ones for none), and the first
/// access mark in bit 63. A vacant slot's memory may be uninitialized.
#[repr(C)]
pub(crate) struct Slot<T> {
    metadata: AtomicU64,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: a slot's value is written only while one writer exclusively owns
// the slot, before release publication, and read only through shared
// references; the metadata is atomic.
#[allow(unsafe_code, reason = "slots hand shared values between threads")]
unsafe impl<T: Send + Sync> Sync for Slot<T> {}

impl<T> Slot<T> {
    /// The initialized value.
    ///
    /// # Safety
    ///
    /// The slot must hold a value that stays initialized for the returned
    /// lifetime: it is published, or unpublished and not yet reclaimed, under
    /// an epoch pin held for that lifetime, or owned by the caller.
    #[allow(unsafe_code, reason = "reads a value the caller proves initialized")]
    pub(crate) const unsafe fn value(&self) -> &T {
        // SAFETY: the caller proves the value is initialized and not mutated
        // for the returned lifetime.
        unsafe { (*self.value.get()).assume_init_ref() }
    }

    pub(crate) fn weight(&self) -> u64 {
        self.metadata.load(Ordering::Relaxed) & WEIGHT_MASK
    }

    /// The deadline tick, or `NEVER_EXPIRES`.
    pub(crate) fn expires_at(&self) -> u64 {
        let expiry = (self.metadata.load(Ordering::Relaxed) & EXPIRY_MASK) >> EXPIRY_SHIFT;
        if expiry == EXPIRY_MASK >> EXPIRY_SHIFT {
            NEVER_EXPIRES
        } else {
            expiry
        }
    }

    /// Marks the entry read unless its deadline has passed, deciding both
    /// from one load of its metadata; `now` runs only for entries with a
    /// deadline. Returns whether the entry is live.
    #[inline]
    pub(crate) fn mark_accessed_if_live(&self, now: impl FnOnce() -> u64) -> bool {
        let metadata = self.metadata.load(Ordering::Relaxed);
        // A field of all ones is the never-expiring encoding.
        let expiry = metadata & EXPIRY_MASK;
        if expiry != EXPIRY_MASK && expiry >> EXPIRY_SHIFT <= now() {
            return false;
        }
        if metadata & ACCESS_BIT == 0 {
            self.metadata.fetch_or(ACCESS_BIT, Ordering::Relaxed);
        } else if metadata & SECOND_ACCESS_BIT == 0 {
            self.metadata.fetch_or(SECOND_ACCESS_BIT, Ordering::Relaxed);
        }
        true
    }

    /// Moves the deadline of an entry live at tick `now` later, to
    /// `deadline`. An entry without a deadline, with a later one, or
    /// expired by `now` keeps its own; access marks set meanwhile stay.
    pub(crate) fn extend_deadline(&self, deadline: u64, now: u64) {
        let deadline = deadline.min(MAX_EXPIRY_TICK);
        let mut current = self.metadata.load(Ordering::Relaxed);
        loop {
            let expiry = (current & EXPIRY_MASK) >> EXPIRY_SHIFT;
            // The never-expiring encoding is above every deadline.
            if expiry >= deadline || expiry <= now {
                return;
            }
            match self.metadata.compare_exchange_weak(
                current,
                current & !EXPIRY_MASK | deadline << EXPIRY_SHIFT,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Whether the entry holds an access mark, without clearing it.
    #[cfg(test)]
    pub(crate) fn accessed(&self) -> bool {
        self.metadata.load(Ordering::Relaxed) & (ACCESS_BIT | SECOND_ACCESS_BIT) != 0
    }

    /// Clears one access mark and reports whether there was one, so a key
    /// read twice survives two eviction sweeps.
    pub(crate) fn take_accessed(&self) -> bool {
        let mut current = self.metadata.load(Ordering::Relaxed);
        loop {
            let mark = if current & SECOND_ACCESS_BIT != 0 {
                SECOND_ACCESS_BIT
            } else if current & ACCESS_BIT != 0 {
                ACCESS_BIT
            } else {
                return false;
            };
            match self.metadata.compare_exchange_weak(
                current,
                current & !mark,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Writes an entry into a vacant slot the caller owns.
///
/// # Safety
///
/// The caller must own `slot` exclusively: taken from its store and not yet
/// published. Its memory may be uninitialized.
#[allow(unsafe_code, reason = "initializes a slot the caller owns")]
pub(crate) unsafe fn fill<T>(slot: NonNull<Slot<T>>, value: T, weight: u32, expires_at: u64) {
    debug_assert!(weight <= MAX_WEIGHT);
    let expiry = if expires_at == NEVER_EXPIRES {
        EXPIRY_MASK >> EXPIRY_SHIFT
    } else {
        expires_at.min(MAX_EXPIRY_TICK)
    };
    let slot = slot.as_ptr();
    // SAFETY: the caller owns the slot, so no other thread accesses it; raw
    // field writes initialize it without reading what was there.
    unsafe {
        ptr::addr_of_mut!((*slot).metadata)
            .write(AtomicU64::new(u64::from(weight) | (expiry << EXPIRY_SHIFT)));
        ptr::addr_of_mut!((*slot).value).write(UnsafeCell::new(MaybeUninit::new(value)));
    }
}

/// Drops a slot's value in place, leaving the slot vacant.
///
/// # Safety
///
/// The slot must hold an initialized value that nothing else can reach, now
/// or later.
#[allow(unsafe_code, reason = "drops a value the caller owns exclusively")]
pub(crate) unsafe fn drop_value<T>(slot: NonNull<Slot<T>>) {
    // SAFETY: the caller owns the initialized value exclusively.
    unsafe { (*slot.as_ref().value.get()).assume_init_drop() };
}

/// Retired queues per cache: threads map to queues by their arena thread
/// index, as to reader shards, so each thread's queue stays in its own cache
/// lines.
const RETIRED_QUEUES: usize = READER_SHARDS;

/// The retired queue this thread uses in every cache.
#[inline]
pub(crate) fn retired_queue_index() -> usize {
    direct_arena_thread() & (RETIRED_QUEUES - 1)
}
/// A thread drains another thread's aged slots once per this many of its
/// own retirements, so a queue whose thread stopped writing still drains.
const FOREIGN_DRAIN_EVERY: u32 = 16;

/// A cache's unpublished slots awaiting reclamation, in one queue per thread
/// index.
pub(crate) struct RetiredSlots<T> {
    queues: Box<[RetiredQueue<T>]>,
}

#[repr(align(128))]
struct RetiredQueue<T>(Mutex<QueueState<T>>);

struct QueueState<T> {
    /// Unpublished slots with the first generation at which each may be
    /// reclaimed, oldest first.
    slots: RetiredRing<T>,
    /// Slots retired here since this queue last reported to its epochs.
    unreported: usize,
    /// Retirements left before this queue's thread drains another queue.
    until_foreign_drain: u32,
    /// The next queue a foreign drain visits.
    next_foreign: usize,
}

/// A queue's retired slots, oldest first, each with its due generation. Its
/// capacity is a power of two, so positions wrap with a mask: `VecDeque`
/// wraps them with a compare, and every write pushes one slot and pops one.
struct RetiredRing<T> {
    buffer: Box<[(usize, NonNull<Slot<T>>)]>,
    /// The oldest slot's position, counting every slot ever popped.
    head: usize,
    len: usize,
}

impl<T> RetiredRing<T> {
    fn new() -> Self {
        Self {
            buffer: Box::new([]),
            head: 0,
            len: 0,
        }
    }

    #[cfg(test)]
    const fn len(&self) -> usize {
        self.len
    }

    const fn capacity(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn at(&self, position: usize) -> (usize, NonNull<Slot<T>>) {
        self.buffer[position & (self.buffer.len() - 1)]
    }

    #[inline]
    fn push_back(&mut self, retired: (usize, NonNull<Slot<T>>)) {
        if self.len == self.buffer.len() {
            self.grow();
        }
        let mask = self.buffer.len() - 1;
        self.buffer[self.head.wrapping_add(self.len) & mask] = retired;
        self.len += 1;
    }

    #[inline]
    fn front(&self) -> Option<(usize, NonNull<Slot<T>>)> {
        (self.len != 0).then(|| self.at(self.head))
    }

    #[inline]
    fn back(&self) -> Option<(usize, NonNull<Slot<T>>)> {
        (self.len != 0).then(|| self.at(self.head.wrapping_add(self.len - 1)))
    }

    #[inline]
    fn pop_front(&mut self) -> Option<(usize, NonNull<Slot<T>>)> {
        let oldest = self.front()?;
        self.head = self.head.wrapping_add(1);
        self.len -= 1;
        Some(oldest)
    }

    /// Doubles the capacity, at least to 16, keeping the order.
    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let capacity = (self.buffer.len() * 2).max(16);
        let mut buffer = vec![(0, NonNull::dangling()); capacity].into_boxed_slice();
        for (index, entry) in buffer.iter_mut().take(self.len).enumerate() {
            *entry = self.at(self.head.wrapping_add(index));
        }
        self.buffer = buffer;
        self.head = 0;
    }
}

// SAFETY: queues only name slots of their cache's store, which outlives them,
// and are only touched under their mutexes.
#[allow(unsafe_code, reason = "retired queues hold raw slot pointers")]
unsafe impl<T: Send> Send for QueueState<T> {}

impl<T> RetiredSlots<T> {
    /// Bytes of the queues and of the buffers holding their retired slots;
    /// takes each queue's lock in turn.
    pub(crate) fn bytes_used(&self) -> usize {
        size_of_val(&*self.queues)
            + self
                .queues
                .iter()
                .map(|queue| {
                    queue.0.lock().slots.capacity() * size_of::<(usize, NonNull<Slot<T>>)>()
                })
                .sum::<usize>()
    }

    pub(crate) fn new() -> Self {
        Self {
            queues: (0..RETIRED_QUEUES)
                .map(|index| {
                    RetiredQueue(Mutex::new(QueueState {
                        slots: RetiredRing::new(),
                        unreported: 0,
                        until_foreign_drain: FOREIGN_DRAIN_EVERY,
                        next_foreign: index + 1,
                    }))
                })
                .collect(),
        }
    }

    /// Queues `slots`, unpublished by this thread while it held `pin`, taken
    /// before unpublishing them, and moves as many aged slots as it queued,
    /// twice as many while a backlog is due, from this thread's queue into
    /// `reclaimed`. Periodically it also
    /// reclaims aged slots from another queue into `store`, if that queue's
    /// thread has stopped retiring. Reads the generation that tags the slots
    /// here, while the caller still holds `pin`. Returns whether enough
    /// retirements are pending across the cache to try advancing.
    #[allow(clippy::inline_always, reason = "every write retires")]
    #[inline(always)]
    pub(crate) fn retire(
        &self,
        pin: &EpochPin<'_>,
        slots: impl Iterator<Item = NonNull<Slot<T>>>,
        reclaimed: &mut Reclaimed<T>,
        store: &SlotStore<T>,
    ) -> bool {
        let epochs = pin.epochs;
        // A pin through the shared counters names the thread's index, as
        // its queue does; an announced pin names its slot instead.
        let own = if ANNOUNCEABLE {
            retired_queue_index()
        } else {
            pin.reader
        };
        debug_assert_eq!(own, retired_queue_index());
        let generation = epochs.generation();
        let due = reclaim_at(generation);
        let mut queue = self.queues[own].0.lock();
        let mut retired = 0;
        for slot in slots {
            queue.slots.push_back((due, slot));
            retired += 1;
        }
        if retired == 0 {
            return false;
        }
        // Reclaim as many slots as this write retired, so values are dropped
        // as steadily as writes allocate them, which allocators' per-thread
        // caches absorb. Slots due since an earlier advance are a backlog,
        // and each write drains as many more of it, within one batch.
        Self::take_aged(&mut queue, generation, reclaimed, retired);
        if queue.slots.front().is_some_and(|(due, _)| due < generation) {
            Self::take_aged(&mut queue, generation, reclaimed, retired);
        }
        // The next write reclaims the front slot, a few advances after this
        // thread last touched it: start loading the lines its drop reads.
        if let Some((_, next)) = queue.slots.front() {
            let first = next.as_ptr().cast::<u8>();
            prefetch(first);
            prefetch(first.wrapping_add(size_of::<Slot<T>>() - 1));
        }
        queue.unreported += retired;
        queue.until_foreign_drain = queue.until_foreign_drain.saturating_sub(1);
        let foreign_queue = (queue.until_foreign_drain == 0).then(|| {
            queue.until_foreign_drain = FOREIGN_DRAIN_EVERY;
            let next = queue.next_foreign;
            queue.next_foreign = next.wrapping_add(1);
            next
        });
        let advance = if queue.unreported >= REPORT_EVERY {
            epochs.note_retired(std::mem::take(&mut queue.unreported))
        } else {
            false
        };
        drop(queue);
        let _ = (foreign_queue, store);
        advance
    }

    /// `retire` for writes that removed more than one entry, out of line.
    #[inline(never)]
    pub(crate) fn retire_many(
        &self,
        pin: &EpochPin<'_>,
        slots: impl Iterator<Item = NonNull<Slot<T>>>,
        reclaimed: &mut Reclaimed<T>,
        store: &SlotStore<T>,
    ) -> bool {
        self.retire(pin, slots, reclaimed, store)
    }

    /// Reclaims a batch from the queue `next` names, if its thread has
    /// stopped retiring; a write in sixteen calls it. Out of line, the batch
    /// it sets up costs the others nothing.
    #[cold]
    #[inline(never)]
    fn drain_foreign(&self, own: usize, next: usize, generation: usize, store: &SlotStore<T>) {
        let index = next & (RETIRED_QUEUES - 1);
        if index != own
            && let Some(mut queue) = self.queues[index].0.try_lock()
            // Only a queue whose newest slot is aged: its thread has retired
            // nothing for three advances, so it is not draining the queue.
            && queue.slots.back().is_some_and(|(due, _)| due <= generation)
        {
            let mut foreign = Reclaimed::new();
            Self::take_aged(&mut queue, generation, &mut foreign, RECLAIM_BATCH);
            drop(queue);
            // Return to this queue until it is drained.
            self.queues[own].0.lock().next_foreign = next;
            foreign.finish(store);
        }
    }

    /// Reclaims every slot aged by `generation`, a batch at a time, dropping
    /// values outside the queue locks.
    pub(crate) fn reclaim_all(&self, generation: usize, store: &SlotStore<T>) {
        for queue in &self.queues {
            loop {
                let mut reclaimed = Reclaimed::new();
                Self::take_aged(
                    &mut queue.0.lock(),
                    generation,
                    &mut reclaimed,
                    RECLAIM_BATCH,
                );
                let full = reclaimed.is_full();
                reclaimed.finish(store);
                if !full {
                    break;
                }
            }
        }
    }

    /// Moves up to `limit` aged slots from one queue while `reclaimed` has
    /// room.
    #[inline]
    fn take_aged(
        queue: &mut QueueState<T>,
        generation: usize,
        reclaimed: &mut Reclaimed<T>,
        limit: usize,
    ) {
        let end = (reclaimed.len + limit).min(RECLAIM_BATCH);
        while reclaimed.len < end {
            match queue.slots.front() {
                Some((due, slot)) if due <= generation => {
                    queue.slots.pop_front();
                    reclaimed.slots[reclaimed.len] = slot;
                    reclaimed.len += 1;
                }
                _ => break,
            }
        }
    }
}

/// Retirements a queue gathers before reporting them to its epochs.
const REPORT_EVERY: usize = 16;

impl<T> Drop for RetiredSlots<T> {
    /// Drops every queued value. The owning cache is gone with every pin on
    /// it, and its store frees the slots' memory only after this.
    #[allow(unsafe_code, reason = "drops retired values no pin reaches")]
    fn drop(&mut self) {
        for queue in &mut self.queues {
            while let Some((_, slot)) = queue.0.get_mut().slots.pop_front() {
                // SAFETY: retired slots hold initialized values, and nothing
                // can reach them once their cache is gone.
                unsafe { drop_value(slot) };
            }
        }
    }
}

/// A cache's vacant slots.
///
/// Slots live in one region the store reserves up front, sized for the
/// cache's entries and the retired entries reclamation usually trails by.
/// Its pages are touched only as slots are first handed out. Each thread
/// keeps its own stack of vacant region slots for each cache it writes,
/// filled by the slots it reclaims, so a slot whose value a thread just
/// dropped is refilled from that thread's cache lines. Threads draw region
/// slots from the shared depot, in batches. A thread's stack overflows into
/// the depot, and returns to it when the thread exits while the cache lives.
///
/// Once the region is used up, as while a pinned reader holds back
/// reclamation, slots come one at a time from small overflow chunks, the
/// lowest-addressed chunk with room first. Overflow slots return to their
/// chunk, never to a thread's stack, and vacant region slots are taken
/// first, so a chunk whose entries are all removed and reclaimed is freed.
/// An entry that stays cached keeps its chunk.
///
/// The table publishes each slot as a nonzero four-byte handle. A region
/// slot's handle is its offset in words from one word before the region, so
/// readers decode it with one add. An overflow slot's handle has
/// `OVERFLOW_HANDLE` set and names its chunk's id and its index there, which
/// readers decode through the depot's directory.
pub(crate) struct SlotStore<T> {
    /// One word before the region.
    handle_base: NonNull<u8>,
    id: u64,
    depot: Arc<Depot>,
    _slots: PhantomData<Slot<T>>,
}

// SAFETY: `handle_base` only locates the depot's region, which the store
// keeps alive; otherwise the store is `Send` and `Sync` as its other fields
// make it: as `Slot<T>` is.
#[allow(unsafe_code, reason = "the store keeps a pointer to its region")]
unsafe impl<T: Send> Send for SlotStore<T> {}
// SAFETY: as above.
#[allow(unsafe_code, reason = "the store keeps a pointer to its region")]
unsafe impl<T: Send + Sync> Sync for SlotStore<T> {}

/// Set in overflow slots' handles.
pub(crate) const OVERFLOW_HANDLE: u32 = 1 << 31;

/// A store's handle decoder; see [`SlotStore::decoder`].
pub(crate) struct SlotDecoder<'store, T> {
    /// One word below the region's first slot, holding the directory's
    /// address.
    handle_base: NonNull<u8>,
    _store: PhantomData<&'store SlotStore<T>>,
}

impl<T> Clone for SlotDecoder<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for SlotDecoder<'_, T> {}

impl<T> SlotDecoder<'_, T> {
    /// The slot `handle` names, as [`SlotStore::slot`].
    #[allow(unsafe_code, reason = "decodes a region handle")]
    #[inline]
    pub(crate) fn slot(self, handle: u32) -> NonNull<Slot<T>> {
        if handle & OVERFLOW_HANDLE == 0 {
            let offset = usize::try_from(handle).expect("a handle fits usize") << 3;
            // SAFETY: a region handle is its slot's offset in words from
            // `handle_base`, so the address lies within the region.
            unsafe { NonNull::new_unchecked(self.handle_base.as_ptr().wrapping_add(offset)) }.cast()
        } else {
            self.overflow_slot(handle)
        }
    }

    /// Decodes an overflow handle. Inlined into readers' lookups, and
    /// reading the directory through `handle_base`, so the lookup keeps no
    /// more state live: an out-of-line call there, or a store pointer kept
    /// for this path, though neither ran while the region had room, made
    /// resident hits at 1,000,000 entries 23% slower on Apple Silicon. Its
    /// failure paths only panic, and never return to the lookup.
    #[allow(
        unsafe_code,
        reason = "reads the directory's address from the region's header"
    )]
    #[allow(
        clippy::inline_always,
        reason = "out of line, its call slowed every lookup that could reach it"
    )]
    #[inline(always)]
    fn overflow_slot(self, handle: u32) -> NonNull<Slot<T>> {
        debug_assert_ne!(handle & OVERFLOW_HANDLE, 0);
        // SAFETY: the store wrote the directory's address there before
        // handing out any slot, and its depot outlives `'store`.
        let directory = unsafe { &*self.handle_base.cast::<*const Directory>().read() };
        let index = usize::try_from(handle & !OVERFLOW_HANDLE).expect("a handle fits usize");
        let chunk = directory.get(index / OVERFLOW_CHUNK_SLOTS);
        let slot = chunk.wrapping_add(index % OVERFLOW_CHUNK_SLOTS * size_of::<Slot<T>>());
        NonNull::new(slot)
            .expect("an overflow handle names a live chunk")
            .cast()
    }
}

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

/// Vacant slots a thread draws at a time, and moves to the depot at a time.
const THREAD_BATCH: usize = 32;
/// A thread's stack moves a batch to the depot past this many slots.
const THREAD_LIMIT: usize = 4 * THREAD_BATCH;
/// Slots per overflow chunk: few, so that the entries a cache keeps for long
/// keep little memory allocated past the region.
const OVERFLOW_CHUNK_SLOTS: usize = 64;
/// Overflow chunks a store can hold: their slots' handles fill the 31 bits
/// below `OVERFLOW_HANDLE`.
const MAX_OVERFLOW_CHUNKS: usize = (1 << 31) / OVERFLOW_CHUNK_SLOTS;
/// Chunk addresses per directory block.
const DIRECTORY_BLOCK: usize = 4_096;
/// The largest region: its last slot's handle stays below `OVERFLOW_HANDLE`.
const MAX_REGION_BYTES: usize = (1 << 34) - 8;

/// A store's region, vacant slots, and overflow chunks, with slot types
/// erased so threads' stacks can name the depot without naming the cache's
/// entry type.
struct Depot {
    /// The region's allocation: a header, then the slots.
    allocation: NonNull<u8>,
    allocation_layout: Layout,
    /// The region's first slot.
    region: NonNull<u8>,
    region_slots: usize,
    /// The layout of one slot.
    slot: Layout,
    directory: Directory,
    state: Mutex<DepotState>,
}

/// Overflow chunks' addresses by id, which readers decoding an overflow
/// handle read without the depot's lock. Blocks are allocated as ids reach
/// them, and kept until the depot drops.
struct Directory {
    blocks: OnceLock<Box<[AtomicPtr<DirectoryBlock>]>>,
}

struct DirectoryBlock([AtomicPtr<u8>; DIRECTORY_BLOCK]);

impl Directory {
    /// Records chunk `id`'s address. Requires the depot's lock, and precedes
    /// handing out any of the chunk's slots.
    fn set(&self, id: usize, chunk: *mut u8) {
        let blocks = self.blocks.get_or_init(|| {
            (0..MAX_OVERFLOW_CHUNKS / DIRECTORY_BLOCK)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect()
        });
        let entry = &blocks[id / DIRECTORY_BLOCK];
        let mut block = entry.load(Ordering::Acquire);
        if block.is_null() {
            block = Box::into_raw(Box::new(DirectoryBlock(std::array::from_fn(|_| {
                AtomicPtr::new(ptr::null_mut())
            }))));
            entry.store(block, Ordering::Release);
        }
        // SAFETY: blocks live until the directory drops.
        #[allow(unsafe_code, reason = "reads a block the directory owns")]
        let block = unsafe { &*block };
        block.0[id % DIRECTORY_BLOCK].store(chunk, Ordering::Release);
    }

    /// Chunk `id`'s address, for a handle published after it was recorded.
    #[inline]
    fn get(&self, id: usize) -> *mut u8 {
        let block = self
            .blocks
            .get()
            .expect("an overflow handle follows its chunk")[id / DIRECTORY_BLOCK]
            .load(Ordering::Acquire);
        // SAFETY: as in `set`; the block was published before the chunk's
        // handles.
        #[allow(unsafe_code, reason = "reads a block the directory owns")]
        let block = unsafe { &*block };
        block.0[id % DIRECTORY_BLOCK].load(Ordering::Acquire)
    }
}

impl Drop for Directory {
    #[allow(unsafe_code, reason = "frees the blocks this directory allocated")]
    fn drop(&mut self) {
        for entry in self
            .blocks
            .get_mut()
            .into_iter()
            .flat_map(|blocks| blocks.iter_mut())
        {
            let block = *entry.get_mut();
            if !block.is_null() {
                // SAFETY: allocated by `set` with `Box::new`, and freed once.
                drop(unsafe { Box::from_raw(block) });
            }
        }
    }
}

struct DepotState {
    /// Vacant region slots.
    vacant: Vec<NonNull<u8>>,
    /// Region slots from this index on have never been handed out.
    fresh: usize,
    /// Overflow chunks by address.
    overflow: BTreeMap<usize, OverflowChunk>,
    /// Addresses of the overflow chunks with a slot to hand out.
    open: BTreeSet<usize>,
    /// Ids of freed overflow chunks.
    free_ids: Vec<usize>,
    /// Overflow chunk ids from this one on have never been used.
    next_id: usize,
}

struct OverflowChunk {
    start: NonNull<u8>,
    id: usize,
    /// Slots from this index on have never been handed out.
    fresh: usize,
    vacant: Vec<NonNull<u8>>,
    /// Slots handed out and not yet returned.
    outstanding: usize,
}

// SAFETY: the depot only names slots of its own region and chunks, behind
// its mutex. Slots carry values between threads; `SlotStore<T>` is `Send`
// and `Sync` only as `Slot<T>` is.
#[allow(unsafe_code, reason = "depots own raw slot pointers")]
unsafe impl Send for Depot {}
// SAFETY: as above; the region's address and size never change, and the
// rest is behind the mutex.
#[allow(unsafe_code, reason = "depots own raw slot pointers")]
unsafe impl Sync for Depot {}

impl Depot {
    #[inline]
    fn in_region(&self, slot: NonNull<u8>) -> bool {
        slot.addr().get().wrapping_sub(self.region.addr().get())
            < self.region_slots * self.slot.size()
    }

    #[allow(unsafe_code, reason = "indexes the region's slots")]
    #[inline]
    fn region_slot(&self, index: usize) -> NonNull<u8> {
        debug_assert!(index < self.region_slots);
        // SAFETY: the index is within the region.
        unsafe { self.region.add(index * self.slot.size()) }
    }

    fn chunk_layout(&self, slots: usize) -> Layout {
        Layout::from_size_align(slots * self.slot.size(), self.slot.align())
            .expect("an overflow chunk's size fits isize")
    }

    /// A vacant slot of the lowest-addressed overflow chunk with one, or of
    /// a new chunk, with its handle.
    #[allow(unsafe_code, reason = "allocates and indexes overflow chunks")]
    #[cold]
    fn take_overflow(&self, state: &mut DepotState) -> (NonNull<u8>, u32) {
        let address = if let Some(&address) = state.open.first() {
            address
        } else {
            let layout = self.chunk_layout(OVERFLOW_CHUNK_SLOTS);
            // SAFETY: a slot has a nonzero size: it holds an `AtomicU64`.
            let start = NonNull::new(unsafe { alloc(layout) })
                .unwrap_or_else(|| handle_alloc_error(layout));
            let id = state.free_ids.pop().unwrap_or_else(|| {
                state.next_id += 1;
                state.next_id - 1
            });
            assert!(
                id < MAX_OVERFLOW_CHUNKS,
                "a cache holds fewer than 2^31 overflow slots"
            );
            self.directory.set(id, start.as_ptr());
            let address = start.addr().get();
            state.overflow.insert(
                address,
                OverflowChunk {
                    start,
                    id,
                    fresh: 0,
                    vacant: Vec::new(),
                    outstanding: 0,
                },
            );
            state.open.insert(address);
            address
        };
        let chunk = state.overflow.get_mut(&address).expect("open chunks exist");
        let slot = chunk.vacant.pop().unwrap_or_else(|| {
            chunk.fresh += 1;
            // SAFETY: an open chunk without vacant slots has fresh ones, so
            // the index is within the chunk.
            unsafe { chunk.start.add((chunk.fresh - 1) * self.slot.size()) }
        });
        chunk.outstanding += 1;
        let index = (slot.addr().get() - address) / self.slot.size();
        let handle = u32::try_from(chunk.id * OVERFLOW_CHUNK_SLOTS + index)
            .expect("overflow handles fit 31 bits")
            | OVERFLOW_HANDLE;
        if chunk.vacant.is_empty() && chunk.fresh == OVERFLOW_CHUNK_SLOTS {
            state.open.remove(&address);
        }
        (slot, handle)
    }

    /// Returns an overflow slot to its chunk, freeing the chunk once all of
    /// its slots are back.
    #[allow(
        unsafe_code,
        reason = "frees an overflow chunk none of whose slots is out"
    )]
    #[cold]
    fn give_overflow(&self, state: &mut DepotState, slot: NonNull<u8>) {
        let (&address, chunk) = state
            .overflow
            .range_mut(..=slot.addr().get())
            .next_back()
            .expect("an overflow slot belongs to a chunk");
        debug_assert!(
            slot.addr().get() - address < OVERFLOW_CHUNK_SLOTS * self.slot.size(),
            "an overflow slot belongs to a chunk"
        );
        chunk.outstanding -= 1;
        if chunk.outstanding == 0 {
            let chunk = state
                .overflow
                .remove(&address)
                .expect("the chunk was found");
            state.open.remove(&address);
            self.directory.set(chunk.id, ptr::null_mut());
            state.free_ids.push(chunk.id);
            let layout = self.chunk_layout(OVERFLOW_CHUNK_SLOTS);
            // SAFETY: allocated with this layout, and none of its slots is
            // out: each holds no value and is named nowhere else, so no
            // reader holds one of their handles.
            unsafe { dealloc(chunk.start.as_ptr(), layout) };
        } else {
            chunk.vacant.push(slot);
            state.open.insert(address);
        }
    }
}

impl Drop for Depot {
    /// Frees the region and every overflow chunk. Values are dropped or
    /// vacant by now: the cache dropped its published and retired values
    /// before releasing the depot.
    #[allow(unsafe_code, reason = "frees memory this depot allocated")]
    fn drop(&mut self) {
        // SAFETY: the region was allocated with this layout.
        unsafe { dealloc(self.allocation.as_ptr(), self.allocation_layout) };
        let layout = self.chunk_layout(OVERFLOW_CHUNK_SLOTS);
        for chunk in std::mem::take(&mut self.state.get_mut().overflow).into_values() {
            // SAFETY: allocated with this layout.
            unsafe { dealloc(chunk.start.as_ptr(), layout) };
        }
    }
}

/// One thread's vacant region slots for one store. Only the store whose id
/// matches casts the pointers back to slots.
struct ThreadSlots {
    store: u64,
    depot: Weak<Depot>,
    slots: Vec<NonNull<u8>>,
}

#[derive(Default)]
struct ThreadStores(Vec<ThreadSlots>);

impl Drop for ThreadStores {
    fn drop(&mut self) {
        for stack in self.0.drain(..) {
            if let Some(depot) = stack.depot.upgrade() {
                depot.state.lock().vacant.extend(stack.slots);
            }
        }
    }
}

thread_local! {
    static THREAD_STORES: RefCell<ThreadStores> = RefCell::new(ThreadStores::default());
}

/// This thread's slot stacks, or `None` while thread-local storage is
/// unavailable, as during thread exit. Every caller shares this one small
/// `try_with`, so the compiler inlines it into each; a closure per caller
/// stayed out of line once the insert body had two copies.
#[inline]
fn thread_stores() -> Option<NonNull<RefCell<ThreadStores>>> {
    #[cfg(test)]
    STACK_BORROWS.with(|count| count.set(count.get() + 1));
    THREAD_STORES.try_with(|stores| NonNull::from(stores)).ok()
}

/// The vacant region slots of the first store this thread takes a slot
/// from. Most threads write to one cache, whose writes reach this stack in
/// a constant-initialized thread-local, without `THREAD_STORES`' `RefCell`
/// and search; other stores' stacks stay there. Once its store drops, the
/// next store to take a slot claims it.
///
/// Only this thread reaches it, and each access ends before running
/// anything that could reach it again: no allocation, no user code.
struct FirstStack {
    /// The store's id, or 0 while unclaimed.
    store: u64,
    depot: Option<Weak<Depot>>,
    len: usize,
    slots: [NonNull<u8>; THREAD_LIMIT],
}

impl Drop for FirstStack {
    fn drop(&mut self) {
        if let Some(depot) = self.depot.take().and_then(|depot| depot.upgrade()) {
            depot
                .state
                .lock()
                .vacant
                .extend_from_slice(&self.slots[..self.len]);
        }
    }
}

thread_local! {
    static FIRST_STACK: UnsafeCell<FirstStack> = const {
        UnsafeCell::new(FirstStack {
            store: 0,
            depot: None,
            len: 0,
            slots: [NonNull::dangling(); THREAD_LIMIT],
        })
    };
}

/// This thread's first stack, or `None` while thread-local storage is
/// unavailable, as during thread exit.
#[inline]
fn first_stack() -> Option<NonNull<FirstStack>> {
    FIRST_STACK
        .try_with(|stack| NonNull::new(stack.get()))
        .ok()
        .flatten()
}

impl<T> SlotStore<T> {
    /// A store whose region holds `region_slots` slots, at most 16 GiB.
    #[allow(unsafe_code, reason = "allocates the region")]
    pub(crate) fn new(region_slots: usize) -> Self {
        let slot = Layout::new::<Slot<T>>();
        let region_slots = region_slots.clamp(1, MAX_REGION_BYTES / slot.size());
        // The header's last word, just below the first slot, holds the
        // directory's address.
        let header = slot.align().max(size_of::<usize>());
        let layout = Layout::from_size_align(header + region_slots * slot.size(), slot.align())
            .expect("the region's size fits isize");
        // SAFETY: the layout has a nonzero size.
        let allocation =
            NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout));
        // Before any slot is touched: the kernel picks page sizes as it
        // first backs each page.
        advise_huge_pages(allocation.as_ptr(), layout.size());
        // SAFETY: the header lies within the allocation.
        let region = unsafe { allocation.add(header) };
        // SAFETY: the header holds at least one word.
        let handle_base = unsafe { region.sub(size_of::<usize>()) };
        let depot = Arc::new(Depot {
            allocation,
            allocation_layout: layout,
            region,
            region_slots,
            slot,
            directory: Directory {
                blocks: OnceLock::new(),
            },
            state: Mutex::new(DepotState {
                vacant: Vec::new(),
                fresh: 0,
                overflow: BTreeMap::new(),
                open: BTreeSet::new(),
                free_ids: Vec::new(),
                next_id: 0,
            }),
        });
        // SAFETY: `handle_base` is aligned and lies in the header, which
        // nothing else uses. The directory lives in the depot, which every
        // decoder's store keeps alive.
        unsafe {
            handle_base
                .cast::<*const Directory>()
                .write(&raw const depot.directory);
        };
        Self {
            handle_base,
            id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            depot,
            _slots: PhantomData,
        }
    }

    /// The slot `handle` names. Its slot must be one this store handed out
    /// and has not yet been given back.
    #[inline]
    pub(crate) fn slot(&self, handle: u32) -> NonNull<Slot<T>> {
        self.decoder().slot(handle)
    }

    /// Decodes handles with the region's base already loaded. Readers take
    /// one before pinning: loads after a pin's or a handle's acquire wait for
    /// it, and a base loaded there lengthened every hit's chain of loads.
    #[inline]
    pub(crate) fn decoder(&self) -> SlotDecoder<'_, T> {
        SlotDecoder {
            handle_base: self.handle_base,
            _store: PhantomData,
        }
    }

    /// A region slot's handle.
    #[inline]
    fn region_handle(&self, slot: NonNull<u8>) -> u32 {
        u32::try_from((slot.addr().get() - self.handle_base.addr().get()) >> 3)
            .expect("region handles fit 31 bits")
    }

    /// Runs `with` on this thread's stack for this store, creating it if
    /// needed. Returns `None` when thread-local storage is unavailable, as
    /// during thread exit.
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    #[allow(unsafe_code, reason = "reads this thread's own thread-local")]
    fn with_stack<R>(&self, with: impl FnOnce(&mut Vec<NonNull<u8>>) -> R) -> Option<R> {
        #[cfg(test)]
        STACK_SEARCHES.with(|count| count.set(count.get() + 1));
        let stores = thread_stores()?;
        // SAFETY: a thread-local lives at a fixed address until its thread
        // exits, past this call on this thread.
        let mut stores = unsafe { stores.as_ref() }.try_borrow_mut().ok()?;
        let index = if let Some(index) = stores.0.iter().position(|stack| stack.store == self.id) {
            index
        } else {
            // Forget stacks of stores that no longer exist: their regions are
            // gone, and their pointers are never read.
            stores.0.retain(|stack| stack.depot.strong_count() != 0);
            stores.0.push(ThreadSlots {
                store: self.id,
                depot: Arc::downgrade(&self.depot),
                slots: Vec::new(),
            });
            stores.0.len() - 1
        };
        Some(with(&mut stores.0[index].slots))
    }

    /// A vacant slot the caller now owns, with its handle, from this
    /// thread's first stack when it is this store's.
    #[allow(unsafe_code, reason = "reads this thread's own thread-local")]
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    pub(crate) fn take(&self) -> (NonNull<Slot<T>>, u32) {
        if let Some(mut stack) = first_stack() {
            // SAFETY: this thread's own first stack, which nothing else on
            // the thread references while this runs.
            let stack = unsafe { stack.as_mut() };
            if stack.store == self.id && stack.len != 0 {
                stack.len -= 1;
                let slot = stack.slots[stack.len];
                return (slot.cast(), self.region_handle(slot));
            }
        }
        self.take_cold()
    }

    /// `take` from the first stack once claimed or refilled, else from a
    /// stack found by search, refilled if empty.
    #[cold]
    #[inline(never)]
    fn take_cold(&self) -> (NonNull<Slot<T>>, u32) {
        if let Some(slot) = self.take_from_first() {
            return (slot.cast(), self.region_handle(slot));
        }
        if let Some(Some(slot)) = self.with_stack(|stack| {
            if stack.is_empty() {
                self.refill(stack);
            }
            stack.pop()
        }) {
            return (slot.cast(), self.region_handle(slot));
        }
        self.take_unbatched()
    }

    /// Takes a slot from this thread's first stack, claiming it if it is
    /// unclaimed or its store has dropped, and refilling it if empty.
    /// Returns `None` if another live store holds it, the region is used
    /// up, or thread-local storage is gone.
    #[allow(unsafe_code, reason = "reads this thread's own thread-local")]
    fn take_from_first(&self) -> Option<NonNull<u8>> {
        let mut stack = first_stack()?;
        let mut previous = None;
        let slot = {
            // SAFETY: as in `take`; the depot's lock runs no user code.
            let stack = unsafe { stack.as_mut() };
            if stack.store != self.id {
                if stack
                    .depot
                    .as_ref()
                    .is_some_and(|depot| depot.strong_count() != 0)
                {
                    return None;
                }
                // A dropped store's slots are gone with its region.
                stack.store = self.id;
                stack.len = 0;
                previous = stack.depot.replace(Arc::downgrade(&self.depot));
            }
            if stack.len == 0 {
                stack.len = self.refill_into(&mut stack.slots[..THREAD_BATCH]);
            }
            stack.len.checked_sub(1).map(|top| {
                stack.len = top;
                stack.slots[top]
            })
        };
        // Outside the stack's access: the last reference to a dropped
        // depot frees it.
        drop(previous);
        slot
    }

    /// Fills `slots` from the start with vacant region slots, from the
    /// depot or else from the region's untouched rest, as `refill` does;
    /// returns how many. None once the region is used up.
    fn refill_into(&self, slots: &mut [NonNull<u8>]) -> usize {
        let depot = &*self.depot;
        let mut state = depot.state.lock();
        if !state.vacant.is_empty() {
            let keep = state.vacant.len().saturating_sub(slots.len());
            let mut count = 0;
            for (target, slot) in slots.iter_mut().zip(state.vacant.drain(keep..)) {
                *target = slot;
                count += 1;
            }
            return count;
        }
        let first = state.fresh;
        let end = (first + slots.len()).min(depot.region_slots);
        state.fresh = end;
        drop(state);
        // Last first, so pops walk the region in address order.
        for (target, index) in slots.iter_mut().zip((first..end).rev()) {
            *target = depot.region_slot(index);
        }
        end - first
    }

    /// A slot straight from the depot: the region is used up, or
    /// thread-local storage is gone.
    #[cold]
    fn take_unbatched(&self) -> (NonNull<Slot<T>>, u32) {
        let depot = &*self.depot;
        let mut state = depot.state.lock();
        let slot = if let Some(slot) = state.vacant.pop() {
            slot
        } else if state.fresh < depot.region_slots {
            state.fresh += 1;
            depot.region_slot(state.fresh - 1)
        } else {
            let (slot, handle) = depot.take_overflow(&mut state);
            return (slot.cast(), handle);
        };
        (slot.cast(), self.region_handle(slot))
    }

    /// Moves a batch of vacant region slots into `stack`, from the depot or
    /// else from the region's untouched rest. Moves none once the region is
    /// used up.
    fn refill(&self, stack: &mut Vec<NonNull<u8>>) {
        let depot = &*self.depot;
        let mut state = depot.state.lock();
        if !state.vacant.is_empty() {
            let keep = state.vacant.len().saturating_sub(THREAD_BATCH);
            stack.extend(state.vacant.drain(keep..));
            return;
        }
        let first = state.fresh;
        let end = (first + THREAD_BATCH).min(depot.region_slots);
        state.fresh = end;
        drop(state);
        // Last first, so pops walk the region in address order. `fill`
        // initializes each slot before anything reads it.
        stack.extend((first..end).rev().map(|index| depot.region_slot(index)));
    }

    /// Bytes of the slots handed out so far: region slots up to the furthest
    /// any thread took, whether they now hold entries or wait for reuse,
    /// and every overflow chunk. A large region's untouched rest takes no
    /// memory until first written.
    pub(crate) fn bytes_used(&self) -> usize {
        let depot = &*self.depot;
        let state = depot.state.lock();
        (state.fresh + state.overflow.len() * OVERFLOW_CHUNK_SLOTS) * depot.slot.size()
    }

    /// Gives back one vacant slot: a region slot to this thread's stack,
    /// which overflows into the depot, and an overflow slot to its chunk.
    #[allow(unsafe_code, reason = "reads this thread's own thread-local")]
    #[allow(clippy::inline_always, reason = "every write's reclamation")]
    #[inline(always)]
    pub(crate) fn give_one(&self, slot: NonNull<Slot<T>>) {
        if self.depot.in_region(slot.cast())
            && let Some(mut stack) = first_stack()
        {
            // SAFETY: as in `take`.
            let stack = unsafe { stack.as_mut() };
            if stack.store == self.id && stack.len < THREAD_LIMIT {
                stack.slots[stack.len] = slot.cast();
                stack.len += 1;
                return;
            }
        }
        self.give_cold(slot);
    }

    /// `give_one` for an overflow slot, a full first stack, or a store
    /// whose stack is listed.
    #[allow(unsafe_code, reason = "reads this thread's own thread-local")]
    #[cold]
    #[inline(never)]
    fn give_cold(&self, slot: NonNull<Slot<T>>) {
        let depot = &*self.depot;
        if !depot.in_region(slot.cast()) {
            depot.give_overflow(&mut depot.state.lock(), slot.cast());
            return;
        }
        if !self.push_first_spilling(slot) {
            self.give_listed(&[slot]);
        }
    }

    /// Pushes `slot` on this thread's first stack if it is this store's,
    /// first moving the stack's newest batch to the depot when it is full.
    /// Returns false if the stack is another store's or thread-local
    /// storage is gone.
    #[allow(unsafe_code, reason = "reads this thread's own thread-local")]
    fn push_first_spilling(&self, slot: NonNull<Slot<T>>) -> bool {
        let Some(mut stack) = first_stack() else {
            return false;
        };
        let mut spilled = [NonNull::dangling(); THREAD_BATCH];
        let spill = {
            // SAFETY: as in `take`.
            let stack = unsafe { stack.as_mut() };
            if stack.store != self.id {
                return false;
            }
            let spill = stack.len == THREAD_LIMIT;
            if spill {
                stack.len -= THREAD_BATCH;
                spilled.copy_from_slice(&stack.slots[stack.len..][..THREAD_BATCH]);
            }
            stack.slots[stack.len] = slot.cast();
            stack.len += 1;
            spill
        };
        // Outside the stack's access: extending the depot may allocate.
        if spill {
            self.depot.state.lock().vacant.extend_from_slice(&spilled);
        }
        true
    }

    /// Gives back vacant slots: region slots to this thread's stack, which
    /// overflows into the depot, and overflow slots to their chunks.
    pub(crate) fn give(&self, slots: &[NonNull<Slot<T>>]) {
        for &slot in slots {
            self.give_one(slot);
        }
    }

    /// `give` for a store whose stack this thread lists in
    /// `THREAD_STORES`.
    fn give_listed(&self, slots: &[NonNull<Slot<T>>]) {
        let depot = &*self.depot;
        let mut overflow = false;
        let stacked = self.with_stack(|stack| {
            for slot in slots {
                if depot.in_region(slot.cast()) {
                    stack.push(slot.cast());
                } else {
                    overflow = true;
                }
            }
            if stack.len() > THREAD_LIMIT {
                let keep = stack.len() - THREAD_BATCH;
                depot.state.lock().vacant.extend(stack.drain(keep..));
            }
        });
        if stacked.is_none() || overflow {
            let mut state = depot.state.lock();
            for slot in slots {
                let slot = slot.cast();
                if !depot.in_region(slot) {
                    depot.give_overflow(&mut state, slot);
                } else if stacked.is_none() {
                    state.vacant.push(slot);
                }
            }
        }
    }
}

/// Up to `RECLAIM_BATCH` aged slots collected under a queue lock, whose
/// values are dropped and slots given to the thread's stack after it is
/// released.
pub(crate) struct Reclaimed<T> {
    slots: [NonNull<Slot<T>>; RECLAIM_BATCH],
    len: usize,
}

/// Aged slots one batch holds. A write reclaims about as many as it retires,
/// one or two, so batches stay small enough to hand on by value cheaply.
pub(crate) const RECLAIM_BATCH: usize = 4;

impl<T> Reclaimed<T> {
    pub(crate) const fn new() -> Self {
        Self {
            slots: [NonNull::dangling(); RECLAIM_BATCH],
            len: 0,
        }
    }

    const fn is_full(&self) -> bool {
        self.len == RECLAIM_BATCH
    }

    /// Drops each value and gives the vacant slots to this thread's stack.
    /// If a value panics while dropping, the batch's slots leak, with the
    /// values after it.
    #[allow(unsafe_code, reason = "drops values of slots no pin reaches")]
    #[allow(clippy::inline_always, reason = "every write's reclamation")]
    #[inline(always)]
    pub(crate) fn finish(self, store: &SlotStore<T>) {
        if self.len == 0 {
            return;
        }
        // A write usually reclaims one slot, without the loops.
        if self.len == 1 {
            // SAFETY: an aged retired slot holds an initialized value that no
            // pin reaches and that only this batch owns.
            unsafe { drop_value(self.slots[0]) };
            store.give_one(self.slots[0]);
            return;
        }
        self.finish_many(store);
    }

    #[allow(unsafe_code, reason = "drops values of slots no pin reaches")]
    #[inline(never)]
    fn finish_many(self, store: &SlotStore<T>) {
        let slots = &self.slots[..self.len];
        for slot in slots {
            // SAFETY: an aged retired slot holds an initialized value that no
            // pin reaches and that only this batch owns.
            unsafe { drop_value(*slot) };
        }
        store.give(slots);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;
    use std::thread;

    use super::*;
    use crate::direct_epoch_protocol::{AGING_ADVANCES, next_epoch_token};

    impl<T> RetiredSlots<T> {
        /// Slots waiting in one queue.
        pub(crate) fn queued(&self, queue: usize) -> usize {
            self.queues[queue].0.lock().slots.len()
        }

        /// The address of the slot one queue reclaims next.
        fn front(&self, queue: usize) -> Option<usize> {
            let queue = self.queues[queue].0.lock();
            queue.slots.front().map(|(_, slot)| slot.as_ptr() as usize)
        }
    }

    impl<T> SlotStore<T> {
        /// Vacant slots in the shared depot.
        pub(crate) fn depot_len(&self) -> usize {
            self.depot.state.lock().vacant.len()
        }

        pub(crate) fn in_region(&self, slot: NonNull<Slot<T>>) -> bool {
            self.depot.in_region(slot.cast())
        }

        /// Slots the region holds.
        pub(crate) fn region_slots(&self) -> usize {
            self.depot.region_slots
        }

        /// The address and length of the region's slots.
        #[cfg(target_os = "linux")]
        pub(crate) fn region_span(&self) -> (usize, usize) {
            (
                self.depot.region.as_ptr() as usize,
                self.depot.region_slots * self.depot.slot.size(),
            )
        }

        /// Overflow chunks the store holds.
        pub(crate) fn overflow_chunks(&self) -> usize {
            self.depot.state.lock().overflow.len()
        }
    }

    #[test]
    fn slots_come_from_one_region_until_it_fills() {
        let store = SlotStore::<u64>::new(100);
        let taken: Vec<_> = (0..100).map(|_| store.take()).collect();
        for &(slot, handle) in &taken {
            assert_eq!(handle & OVERFLOW_HANDLE, 0);
            assert_eq!(store.slot(handle), slot, "a region handle names its slot");
        }
        let slots: Vec<_> = taken.iter().map(|&(slot, _)| slot).collect();
        assert!(
            slots.iter().all(|&slot| store.in_region(slot)),
            "a slot came from outside the region"
        );
        let mut addresses: Vec<_> = slots.iter().map(|slot| slot.as_ptr() as usize).collect();
        addresses.sort_unstable();
        addresses.dedup();
        assert_eq!(addresses.len(), 100, "a region slot was handed out twice");
        let (extra, handle) = store.take();
        assert!(!store.in_region(extra), "the region holds only 100 slots");
        assert_ne!(handle & OVERFLOW_HANDLE, 0);
        assert_eq!(
            store.slot(handle),
            extra,
            "an overflow handle names its slot"
        );
        store.give(&slots);
        store.give(&[extra]);
    }

    #[test]
    fn a_threads_first_stack_passes_to_a_live_store_once_its_store_drops() {
        thread::spawn(|| {
            let first = SlotStore::<u64>::new(64);
            let (slot, _) = first.take();
            first.give(&[slot]);
            drop(first);
            let second = SlotStore::<u64>::new(64);
            let (slot, _) = second.take();
            let before = STACK_BORROWS.with(std::cell::Cell::get);
            for _ in 0..100 {
                second.give(&[slot]);
                assert_eq!(second.take().0, slot, "the stack is last in, first out");
            }
            assert_eq!(
                STACK_BORROWS.with(std::cell::Cell::get),
                before,
                "the second store's slots went through the list of stacks"
            );
            second.give(&[slot]);
        })
        .join()
        .expect("the thread finishes");
    }

    #[test]
    fn a_threads_vacant_slots_return_to_the_depot_when_it_exits() {
        let store = Arc::new(SlotStore::<u64>::new(256));
        let writer = Arc::clone(&store);
        // Joined, not scoped: a scope can end before the thread's
        // thread-locals are destroyed.
        thread::spawn(move || {
            let slots: Vec<_> = (0..40).map(|_| writer.take().0).collect();
            writer.give(&slots);
        })
        .join()
        .expect("the thread finishes");
        // Two batches of 32 left the depot; all 64 came back.
        assert_eq!(store.depot_len(), 64);
    }

    #[test]
    fn overflow_chunks_are_freed_once_their_slots_return() {
        let store = SlotStore::<u64>::new(16);
        let region: Vec<_> = (0..16).map(|_| store.take().0).collect();
        let overflow: Vec<_> = (0..1_000)
            .map(|_| {
                let (slot, handle) = store.take();
                assert_eq!(
                    store.slot(handle),
                    slot,
                    "an overflow handle names its slot"
                );
                slot
            })
            .collect();
        assert!(overflow.iter().all(|&slot| !store.in_region(slot)));
        assert!(store.overflow_chunks() > 0);
        store.give(&overflow[..500]);
        assert!(
            store.overflow_chunks() > 0,
            "half the overflow is still out"
        );
        store.give(&overflow[500..]);
        assert_eq!(
            store.overflow_chunks(),
            0,
            "a vacant overflow chunk was kept"
        );
        store.give(&region);
        assert!(
            store.in_region(store.take().0),
            "vacant region slots come first"
        );
    }

    #[test]
    fn slots_are_reclaimable_three_advances_after_their_tag() {
        // Tokens skip every fourth value: slot 2 advances by two.
        assert_eq!(reclaim_at(0), 4);
        assert_eq!(reclaim_at(1), 5);
        assert_eq!(reclaim_at(2), 6);
        assert_eq!(reclaim_at(4), 8);
        assert_eq!(reclaim_at(usize::MAX - 1), usize::MAX);
        let advanced =
            |tag: usize| (0..AGING_ADVANCES).try_fold(tag, |token, _| next_epoch_token(token));
        let near_end = (usize::MAX - 64..=usize::MAX).filter(|token| token & 3 != 3);
        for tag in (0..1_024).filter(|token| token & 3 != 3).chain(near_end) {
            assert_eq!(
                reclaim_at(tag),
                advanced(tag).unwrap_or(usize::MAX),
                "tag {tag}"
            );
        }
    }

    /// Retires `count` filled slots from this thread, as one write would,
    /// and returns how many aged slots the write reclaimed from its own queue.
    #[allow(unsafe_code, reason = "fills slots taken from the test's store")]
    fn retire_filled(
        epochs: &Epochs,
        retired: &RetiredSlots<u64>,
        store: &SlotStore<u64>,
        count: usize,
    ) -> usize {
        let pin = epochs.pin();
        let slots: Vec<_> = (0..count)
            .map(|value| {
                let (slot, _) = store.take();
                // SAFETY: `take` handed the vacant slot to this test alone.
                unsafe { fill(slot, value as u64, 1, NEVER_EXPIRES) };
                slot
            })
            .collect();
        let mut reclaimed = Reclaimed::new();
        retired.retire(&pin, slots.into_iter(), &mut reclaimed, store);
        let count = reclaimed.len;
        reclaimed.finish(store);
        drop(pin);
        count
    }

    #[test]
    fn writes_reclaim_as_many_slots_as_they_retire() {
        let epochs = Epochs::new();
        // Declared first so it drops last: retired values live in its region.
        let store = SlotStore::new(4_096);
        let retired = RetiredSlots::new();
        assert_eq!(retire_filled(&epochs, &retired, &store, 64), 0);
        for _ in 0..3 {
            assert!(epochs.try_advance());
        }
        // All 64 are due. Dropping them in bursts would overflow allocators'
        // per-thread caches, so each write frees as many as it retires.
        for _ in 0..32 {
            assert_eq!(retire_filled(&epochs, &retired, &store, 1), 1);
        }
        assert!(epochs.try_advance());
        // The 32 left became due before the latest advance: a backlog, which
        // each write drains by as many more as it retires, while they fit
        // one reclamation batch.
        for _ in 0..8 {
            assert_eq!(retire_filled(&epochs, &retired, &store, 1), 2);
        }
        for _ in 0..4 {
            assert_eq!(retire_filled(&epochs, &retired, &store, 2), 4);
        }
        assert_eq!(
            retire_filled(&epochs, &retired, &store, 1),
            0,
            "only the backlog was due"
        );
    }

    #[test]
    fn a_write_prefetches_the_slot_the_next_write_reclaims() {
        // Dropping a reclaimed value reads its slot, last touched a few
        // advances earlier. On x86, with a 1 MB L2, that read and the
        // value's own then missed in series.
        let epochs = Epochs::new();
        // Declared first so it drops last: retired values live in its region.
        let store = SlotStore::new(4_096);
        let retired = RetiredSlots::new();
        assert_eq!(retire_filled(&epochs, &retired, &store, 4), 0);
        for _ in 0..3 {
            assert!(epochs.try_advance());
        }
        PREFETCHED.with(|lines| lines.borrow_mut().clear());
        assert_eq!(retire_filled(&epochs, &retired, &store, 1), 1);
        let next = retired
            .front(retired_queue_index())
            .expect("three aged slots remain");
        assert!(
            PREFETCHED.with(|lines| lines.borrow().contains(&next)),
            "the next slot to reclaim was not prefetched"
        );
    }

    #[test]
    fn foreign_drains_leave_a_retiring_threads_queue_alone() {
        let epochs = Epochs::new();
        // Declared first so it drops last: retired values live in its region.
        let store = SlotStore::new(4_096);
        let retired = RetiredSlots::new();
        let (to_other, other_requests) = mpsc::channel::<usize>();
        let (to_main, other_retired) = mpsc::channel();
        thread::scope(|scope| {
            scope.spawn(|| {
                for count in other_requests {
                    retire_filled(&epochs, &retired, &store, count);
                    to_main
                        .send(retired_queue_index())
                        .expect("the test thread listens");
                }
            });
            let retire_on_other = |count| {
                to_other.send(count).expect("the other thread listens");
                other_retired.recv().expect("the other thread retires")
            };
            let other = retire_on_other(64);
            if other == retired_queue_index() {
                // Thread indices 64 apart share a queue; nothing to compare.
                return;
            }
            for _ in 0..3 {
                assert!(epochs.try_advance());
            }
            // The other thread retires again, so its queue is its own to
            // drain, at its own pace.
            retire_on_other(1);
            let visit_every_queue = RETIRED_QUEUES * FOREIGN_DRAIN_EVERY as usize;
            for _ in 0..visit_every_queue {
                retire_filled(&epochs, &retired, &store, 1);
            }
            // Its own write reclaimed one of the 64 and queued one.
            assert_eq!(
                retired.queued(other),
                64,
                "a writer took aged slots from a thread still retiring"
            );
            // Three advances later, the other thread has retired nothing
            // since: writers drain its queue.
            for _ in 0..3 {
                assert!(epochs.try_advance());
            }
            for _ in 0..8 {
                for _ in 0..visit_every_queue {
                    retire_filled(&epochs, &retired, &store, 1);
                }
                if retired.queued(other) == 0 {
                    break;
                }
            }
            assert_eq!(retired.queued(other), 0, "the idle queue was not drained");
            drop(to_other);
        });
    }

    #[test]
    fn retired_rings_keep_slots_oldest_first_as_they_grow_and_wrap() {
        let slot = |index: usize| {
            NonNull::new(std::ptr::without_provenance_mut::<Slot<u64>>(
                (index + 1) * 64,
            ))
            .expect("nonzero")
        };
        let mut ring = RetiredRing::<u64>::new();
        let mut model = VecDeque::new();
        let mut next = 0;
        // Pushes outrun pops, then pops outrun pushes, so the ring both
        // grows with its oldest slot mid-buffer and wraps without growing.
        for (pushes, pops) in [
            (5, 2),
            (40, 30),
            (3, 0),
            (100, 7),
            (0, 60),
            (17, 20),
            (64, 64),
        ] {
            for _ in 0..pushes {
                ring.push_back((next, slot(next)));
                model.push_back((next, slot(next)));
                next += 1;
            }
            assert_eq!(ring.back(), model.back().copied());
            for _ in 0..pops {
                assert_eq!(ring.front(), model.front().copied());
                assert_eq!(ring.pop_front(), model.pop_front());
            }
            assert_eq!(ring.len(), model.len());
            assert!(ring.capacity().is_power_of_two() || ring.capacity() == 0);
        }
        while let Some(oldest) = model.pop_front() {
            assert_eq!(ring.pop_front(), Some(oldest));
        }
        assert_eq!(
            (ring.pop_front(), ring.front(), ring.back()),
            (None, None, None)
        );
    }

    #[test]
    fn a_writes_reclamation_batch_fits_a_cache_line() {
        // Every write sets up one and hands it on by value.
        assert!(std::mem::size_of::<Reclaimed<u64>>() <= 64);
    }

    #[test]
    fn consecutive_pins_on_a_thread_reuse_one_ordered_announcement() {
        // On x86 a pin's read-modify-write is a full barrier, so each read
        // waited for the previous one's misses. Where announcements are
        // supported (Linux, and tests and Miri elsewhere), a thread's
        // later pins reuse its parked announcement until the epoch moves.
        let epochs = Epochs::new();
        drop(epochs.pin());
        let before = ORDERED_PINS.get();
        for _ in 0..1_000 {
            drop(epochs.pin());
        }
        assert_eq!(ORDERED_PINS.get() - before, 0);
        assert!(epochs.try_advance());
        drop(epochs.pin());
        assert_eq!(
            ORDERED_PINS.get() - before,
            1,
            "a moved epoch is announced afresh, once"
        );
    }

    #[test]
    fn threads_that_exit_return_their_announcer_slots() {
        // Without the release, the first 64 threads would keep every slot
        // and later threads would pin through the shared counters.
        let epochs = Epochs::new();
        for _ in 0..2 * ANNOUNCERS {
            thread::scope(|scope| {
                scope.spawn(|| drop(epochs.pin()));
            });
        }
        let slot = thread::scope(|scope| {
            scope
                .spawn(|| {
                    drop(epochs.pin());
                    ANNOUNCER.get() & ANNOUNCER_SLOT
                })
                .join()
                .expect("the pinning thread finishes")
        });
        assert!(slot < ANNOUNCERS, "a thread found no announcer slot free");
    }

    #[test]
    fn a_thread_that_exits_holding_a_pin_keeps_its_announcer_slot() {
        // A thread-local that a thread fills after its first pin drops after
        // the slot's release would: the pin it holds still writes the
        // slot's lines, so no other thread may claim the slot.
        // A static, as the pin outlives the thread's other locals; Miri
        // reports a leaked box.
        static CLOCK: std::sync::LazyLock<Epochs> = std::sync::LazyLock::new(Epochs::new);
        let epochs: &'static Epochs = &CLOCK;
        thread_local! {
            static HELD: RefCell<Option<EpochPin<'static>>> = const { RefCell::new(None) };
        }
        let slot = thread::spawn(move || {
            // Registered before the slot's release, so dropped after it.
            HELD.with(|_| ());
            HELD.with(|held| *held.borrow_mut() = Some(epochs.pin()));
            ANNOUNCER.get() & ANNOUNCER_SLOT
        })
        .join()
        .expect("the pinning thread exits");
        assert!(slot < ANNOUNCERS);
        assert_ne!(
            ANNOUNCERS_HELD.load(Ordering::Relaxed) & 1 << slot,
            0,
            "the slot was released under a pin that its thread still held"
        );
        // The pin left before the thread exited: advances pass it.
        for _ in 0..EPOCHS {
            assert!(epochs.try_advance());
        }
    }

    #[test]
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    fn linux_pins_through_announcements_with_membarrier_evictions() {
        assert!(announcements_supported());
        let epochs = Epochs::new();
        drop(epochs.pin());
        assert!(epochs.try_advance());
        // The parked announcement blocks the next advance until evicted.
        assert!(epochs.try_advance());
        assert!(heavy_fence());
    }

    #[test]
    fn a_pin_blocks_the_second_advance_past_its_epoch() {
        let epochs = Epochs::new();
        let pin = epochs.pin();
        assert!(
            epochs.try_advance(),
            "readers in the current slot allow one advance"
        );
        assert!(
            !epochs.try_advance(),
            "the pin's slot must be quiescent for the next"
        );
        drop(pin);
        assert!(epochs.try_advance());
        assert!(epochs.try_advance());
    }
}
