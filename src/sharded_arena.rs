//! Epoch-reclaimed slab storage for `ShardedCache` entries.
//!
//! Readers and writers pin an epoch with the direct arena's reader protocol
//! and its shared state machine in `direct_epoch_protocol`. Entries live in
//! one slab region per cache rather than in individual allocations, with
//! overflow chunks for bursts past the region (see `SlotStore`):
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
//!   it retires, one more while a backlog is due, and other writers drain the
//!   queue of a thread that stopped retiring.
//! - Values are dropped outside the shard lock by the reclaiming thread, and
//!   their slots join that thread's stack, so it refills slots it just
//!   touched. Each write prefetches the slot its thread reclaims next.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::cell::{RefCell, UnsafeCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;

use crate::cache_arena::{assigned_thread_shards, claimed_thread_shards, direct_arena_thread};
pub(crate) use crate::direct_epoch_protocol::reclaim_at;
use crate::direct_epoch_protocol::{EPOCHS, enter_epoch, leave_epoch, plan_epoch_advance};

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

/// The epoch clock a cache's readers and writers pin.
pub(crate) struct Epochs {
    /// Generation token: its low two bits name the current epoch slot.
    generation: AtomicUsize,
    /// `readers[epoch * READER_SHARDS + shard]`.
    readers: Box<[ReaderCount]>,
    /// Serializes advances; a writer that finds it held skips advancing.
    advancing: Mutex<()>,
    /// Retirements reported since the generation last advanced.
    pending: PendingCount,
}

impl Epochs {
    pub(crate) fn new() -> Self {
        Self {
            generation: AtomicUsize::new(0),
            readers: (0..EPOCHS * READER_SHARDS)
                .map(|_| ReaderCount(AtomicUsize::new(0)))
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
    #[inline]
    pub(crate) fn pin(&self) -> EpochPin<'_> {
        let shard = direct_arena_thread() & (READER_SHARDS - 1);
        #[cfg(test)]
        PINS_HELD.set(PINS_HELD.get() + 1);
        #[cfg(test)]
        PINS_TAKEN.set(PINS_TAKEN.get() + 1);
        EpochPin {
            epochs: self,
            epoch: self.enter(shard),
            shard,
            _not_send: PhantomData,
        }
    }

    #[inline]
    fn enter(&self, shard: usize) -> usize {
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
    fn leave(&self, epoch: usize, shard: usize) {
        leave_epoch(epoch, |epoch| {
            self.readers[epoch * READER_SHARDS + shard]
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
        let shards = claimed_thread_shards();
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
            true
        }) else {
            // Readers still hold an old epoch: wait for half as many more
            // retirements before scanning again.
            self.pending.0.store(advance_after() / 2, Ordering::Relaxed);
            return false;
        };
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
}

/// A pinned epoch; see [`Epochs::pin`].
pub(crate) struct EpochPin<'epochs> {
    epochs: &'epochs Epochs,
    epoch: usize,
    shard: usize,
    _not_send: PhantomData<*mut ()>,
}

impl EpochPin<'_> {
    /// Leaves and re-enters the current epoch, letting slots unpublished
    /// since the pin be reclaimed.
    pub(crate) fn refresh(&mut self) {
        self.epochs.leave(self.epoch, self.shard);
        self.epoch = self.epochs.enter(self.shard);
    }
}

impl Drop for EpochPin<'_> {
    #[inline]
    fn drop(&mut self) {
        self.epochs.leave(self.epoch, self.shard);
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
    slots: VecDeque<(usize, NonNull<Slot<T>>)>,
    /// Slots retired here since this queue last reported to its epochs.
    unreported: usize,
    /// Retirements left before this queue's thread drains another queue.
    until_foreign_drain: u32,
    /// The next queue a foreign drain visits.
    next_foreign: usize,
}

// SAFETY: queues only name slots of their cache's store, which outlives them,
// and are only touched under their mutexes.
#[allow(unsafe_code, reason = "retired queues hold raw slot pointers")]
unsafe impl<T: Send> Send for QueueState<T> {}

impl<T> RetiredSlots<T> {
    pub(crate) fn new() -> Self {
        Self {
            queues: (0..RETIRED_QUEUES)
                .map(|index| {
                    RetiredQueue(Mutex::new(QueueState {
                        slots: VecDeque::new(),
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
    /// one more while a backlog is due, from this thread's queue into
    /// `reclaimed`. Periodically it also
    /// moves aged slots into `foreign` from another queue, if that queue's
    /// thread has stopped retiring. Reads the generation that tags the slots
    /// here, while the caller still holds `pin`. Returns whether enough
    /// retirements are pending across the cache to try advancing.
    pub(crate) fn retire(
        &self,
        pin: &EpochPin<'_>,
        slots: impl Iterator<Item = NonNull<Slot<T>>>,
        reclaimed: &mut Reclaimed<T>,
        foreign: &mut Reclaimed<T>,
    ) -> bool {
        let epochs = pin.epochs;
        // The pin's reader shard is the thread's index, as is its queue's.
        let own = pin.shard;
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
        // and each write drains one more of it.
        Self::take_aged(&mut queue, generation, reclaimed, retired);
        if queue
            .slots
            .front()
            .is_some_and(|&(due, _)| due < generation)
        {
            Self::take_aged(&mut queue, generation, reclaimed, 1);
        }
        // The next write reclaims the front slot, a few advances after this
        // thread last touched it: start loading the lines its drop reads.
        if let Some(&(_, next)) = queue.slots.front() {
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
        if let Some(next) = foreign_queue
            && let index = next & (RETIRED_QUEUES - 1)
            && index != own
            && let Some(mut queue) = self.queues[index].0.try_lock()
            // Only a queue whose newest slot is aged: its thread has retired
            // nothing for three advances, so it is not draining the queue.
            && queue.slots.back().is_some_and(|&(due, _)| due <= generation)
        {
            Self::take_aged(&mut queue, generation, foreign, RECLAIM_BATCH);
            drop(queue);
            // Return to this queue until it is drained.
            self.queues[own].0.lock().next_foreign = next;
        }
        advance
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
                Some(&(due, slot)) if due <= generation => {
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
    THREAD_STORES.try_with(|stores| NonNull::from(stores)).ok()
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

    /// A vacant slot the caller now owns, with its handle.
    #[allow(
        clippy::inline_always,
        reason = "with `insert_many` as a second caller of the insert body, it stayed out of line"
    )]
    #[inline(always)]
    pub(crate) fn take(&self) -> (NonNull<Slot<T>>, u32) {
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

    /// Gives back vacant slots: region slots to this thread's stack, which
    /// overflows into the depot, and overflow slots to their chunks.
    #[inline]
    pub(crate) fn give(&self, slots: &[NonNull<Slot<T>>]) {
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
    #[inline]
    pub(crate) fn finish(self, store: &SlotStore<T>) {
        if self.len == 0 {
            return;
        }
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
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    impl<T> RetiredSlots<T> {
        /// Slots waiting in one queue.
        pub(crate) fn queued(&self, queue: usize) -> usize {
            self.queues[queue].0.lock().slots.len()
        }

        /// The address of the slot one queue reclaims next.
        fn front(&self, queue: usize) -> Option<usize> {
            let queue = self.queues[queue].0.lock();
            queue.slots.front().map(|&(_, slot)| slot.as_ptr() as usize)
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
    }

    /// Retires `count` filled slots from this thread, as one write would,
    /// and returns how many aged slots the write reclaimed.
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
        let mut foreign = Reclaimed::new();
        retired.retire(&pin, slots.into_iter(), &mut reclaimed, &mut foreign);
        let count = reclaimed.len + foreign.len;
        reclaimed.finish(store);
        foreign.finish(store);
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
        // each write drains by one more.
        for _ in 0..16 {
            assert_eq!(retire_filled(&epochs, &retired, &store, 1), 2);
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
    fn a_writes_reclamation_batch_fits_a_cache_line() {
        // Every write sets up two, its own and a foreign drain's, and hands
        // them on by value.
        assert!(std::mem::size_of::<Reclaimed<u64>>() <= 64);
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
