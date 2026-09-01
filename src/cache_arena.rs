//! Epoch-protected generational storage for cache values.
//!
//! This is one of the isolated `PackedGen` modules permitted to contain unsafe
//! code. Every published pointer has stable `Box` or arena-block storage. The
//! direct cache uses a sharded quiescent-state reader protocol; the generational
//! arena uses `seize`. Both retire values only after they become unreachable,
//! and owned guards keep returned values alive after removal or cache drop.

use std::cell::{Cell, UnsafeCell};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, RwLock};
use seize::{Collector, Guard, LocalGuard, reclaim};

use crate::NonMaxU64;
#[cfg(test)]
use crate::direct_epoch_protocol::next_epoch_token as next_direct_epoch_token;
use crate::direct_epoch_protocol::{
    EPOCHS as DIRECT_EPOCHS, enter_epoch, epoch_slot as direct_epoch_slot, leave_epoch,
    plan_epoch_advance,
};

const SLOT_BITS: u32 = 24;
const SHARD_BITS: u32 = 8;
const GENERATION_BITS: u32 = 31;
const SLOT_MASK: u64 = (1_u64 << SLOT_BITS) - 1;
const SHARD_MASK: u64 = (1_u64 << SHARD_BITS) - 1;
const MAX_GENERATION: u32 = (1_u32 << GENERATION_BITS) - 1;
const ACCESSED_BIT: u32 = 1_u32 << GENERATION_BITS;
const BLOCK_BITS: u32 = 10;
const BLOCK_SIZE: usize = 1 << BLOCK_BITS;
const BLOCK_MASK: usize = BLOCK_SIZE - 1;
const PAGE_BITS: u32 = 8;
const PAGE_SIZE: usize = 1 << PAGE_BITS;
const PAGE_MASK: usize = PAGE_SIZE - 1;
const DIRECTORY_SIZE: usize = 1 << (SLOT_BITS - BLOCK_BITS - PAGE_BITS);
const PIN_BLOCK_CACHE_SIZE: usize = 32;
const DIRECT_ACCESS_BIT: u64 = 1_u64 << 63;
const DIRECT_BLOCK_BITS: u32 = 8;
const DIRECT_BLOCK_SIZE: usize = 1 << DIRECT_BLOCK_BITS;
const DIRECT_RESERVATION_SIZE: usize = 8;
const DIRECT_BOXED_TAG: usize = 1;
const DIRECT_RECYCLED_BOXED_TAG: usize = 2;
const DIRECT_POINTER_TAGS: usize = DIRECT_BOXED_TAG | DIRECT_RECYCLED_BOXED_TAG;
#[cfg(feature = "prepared-keys")]
const DIRECT_RECYCLED_BOX_CAPACITY_PER_SHARD: usize = DIRECT_RECLAIM_BATCH * 2;
const DIRECT_EXPIRY_SHIFT: u32 = 32;
const DIRECT_EXPIRY_MASK: u64 = 0x7fff_ffff_u64 << DIRECT_EXPIRY_SHIFT;
const DIRECT_NEVER_EXPIRES: u32 = 0x7fff_ffff;
const DIRECT_MAX_PARTITIONS: usize = 256;
const DIRECT_RECLAIM_BATCH: usize = 512;
pub(crate) const DIRECT_RECLAIM_PUBLISH_BATCH: usize = 64;
const DIRECT_READER_SHARDS: usize = 64;
// Match the standard-library reference-count strategy: reserve half the
// counter space so a process terminates long before a concurrent increment can
// wrap a live reader count to zero. On supported 64-bit targets this permits
// more than nine quintillion simultaneous/forgotten reader guards.
const DIRECT_MAX_READERS: usize = isize::MAX as usize;
const _: () = assert!(DIRECT_MAX_READERS <= usize::MAX / 2);
const DIRECT_RETIRED_SHARDS: usize = 64;
const DIRECT_LOOKUP_SHARDS: usize = 64;
const DIRECT_LOOKUP_SEGMENT_BITS: u32 = 16;
pub(crate) const DIRECT_MAX_EXPIRY_TICK: u64 = 0x7fff_fffe;
pub(crate) const MAX_EXPIRY_TICK: u64 = u32::MAX as u64 - 1;

static NEXT_DIRECT_ARENA_THREAD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static DIRECT_ARENA_THREAD: Cell<usize> = const { Cell::new(usize::MAX) };
    // Non-zero only while this thread is running user destructors with one
    // direct arena's collector gate held. The exact owner identity matters:
    // destruction may re-enter a different cache, whose gate must still be
    // acquired normally.
    static DIRECT_RECLAIM_OWNER: Cell<usize> = const { Cell::new(0) };
}

struct DirectReclaimOwnerScope {
    previous: usize,
}

impl DirectReclaimOwnerScope {
    fn enter(owner: usize) -> Self {
        debug_assert_ne!(owner, 0);
        let previous = DIRECT_RECLAIM_OWNER.with(|current| current.replace(owner));
        Self { previous }
    }
}

impl Drop for DirectReclaimOwnerScope {
    fn drop(&mut self) {
        DIRECT_RECLAIM_OWNER.with(|current| current.set(self.previous));
    }
}

#[inline]
fn acquire_direct_reader(counter: &AtomicUsize) {
    let previous = counter.fetch_add(1, Ordering::SeqCst);
    if previous >= DIRECT_MAX_READERS {
        direct_reader_counter_exhausted();
    }
}

#[cold]
#[inline(never)]
fn direct_reader_counter_exhausted() -> ! {
    // Panicking is insufficient: a caller could catch and repeat the panic
    // until the leaked increments wrapped. Aborting is the same fail-closed
    // policy used by reference-counted ownership at impossible count limits.
    std::process::abort()
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ArenaHandle(NonMaxU64);

impl ArenaHandle {
    fn new(shard: usize, slot: u32, generation: u32) -> Self {
        debug_assert!(shard <= usize::try_from(SHARD_MASK).expect("8-bit mask fits usize"));
        debug_assert!(u64::from(slot) <= SLOT_MASK);
        debug_assert!((1..=MAX_GENERATION).contains(&generation));
        let raw = (u64::from(generation) << (SLOT_BITS + SHARD_BITS))
            | (u64::try_from(shard).expect("arena shard is at most 255") << SLOT_BITS)
            | u64::from(slot);
        Self(NonMaxU64::new(raw).expect("arena handles keep the reserved top bit clear"))
    }

    pub(crate) const fn from_index_value(value: NonMaxU64) -> Self {
        Self(value)
    }

    pub(crate) const fn index_value(self) -> NonMaxU64 {
        self.0
    }

    const fn shard(self) -> usize {
        ((self.0.get() >> SLOT_BITS) & SHARD_MASK) as usize
    }

    const fn slot(self) -> u32 {
        (self.0.get() & SLOT_MASK) as u32
    }

    const fn generation(self) -> u32 {
        (self.0.get() >> (SLOT_BITS + SHARD_BITS)) as u32
    }
}

pub(crate) struct ArenaEntry<V> {
    value: V,
    weight: u32,
    expires_at: AtomicU32,
}

impl<V> ArenaEntry<V> {
    #[allow(clippy::unnecessary_box_returns)]
    pub(crate) fn new(value: V, weight: u32, expires_at: u64) -> Box<Self> {
        Box::new(Self {
            value,
            weight,
            expires_at: AtomicU32::new(encode_expiry(expires_at)),
        })
    }

    pub(crate) const fn value(&self) -> &V {
        &self.value
    }

    pub(crate) fn expires_at(&self) -> u64 {
        let expiry = self.expires_at.load(Ordering::Acquire);
        if expiry == u32::MAX {
            u64::MAX
        } else {
            u64::from(expiry)
        }
    }

    pub(crate) fn set_expires_at(&self, expires_at: u64) {
        self.expires_at
            .store(encode_expiry(expires_at), Ordering::Release);
    }

    pub(crate) const fn weight(&self) -> u64 {
        self.weight as u64
    }
}

/// A pointer protected by a thread-local epoch guard.
pub(crate) struct ArenaValue<T> {
    guard: ManuallyDrop<LocalGuard<'static>>,
    _collector: Arc<Collector>,
    pointer: NonNull<T>,
    _value: PhantomData<T>,
}

pub(crate) struct ArenaPin<'arena, V> {
    arena: &'arena ValueArena<V>,
    guard: LocalGuard<'arena>,
    blocks: [std::cell::Cell<CachedBlock<V>>; PIN_BLOCK_CACHE_SIZE],
}

struct CachedBlock<V> {
    tag: u32,
    pointer: *const ArenaBlock<V>,
}

impl<V> Copy for CachedBlock<V> {}

impl<V> Clone for CachedBlock<V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> CachedBlock<V> {
    const EMPTY: Self = Self {
        tag: u32::MAX,
        pointer: ptr::null(),
    };
}

impl<V> ArenaPin<'_, V> {
    #[allow(
        unsafe_code,
        reason = "dereferences pointers protected by the seize guard"
    )]
    pub(crate) fn get(&self, handle: ArenaHandle) -> Option<&ArenaEntry<V>> {
        let partition = self.arena.partitions.get(handle.shard())?;
        let block_index = (handle.slot() as usize) >> BLOCK_BITS;
        let tag = u32::try_from((handle.shard() << (SLOT_BITS - BLOCK_BITS)) | block_index)
            .expect("arena block tag fits u32");
        let cache_index =
            (block_index ^ handle.shard().wrapping_mul(0x9e37)) & (PIN_BLOCK_CACHE_SIZE - 1);
        let cached = self.blocks[cache_index].get();
        let block = if cached.tag == tag {
            // SAFETY: arena blocks are append-only and cannot be dropped while
            // this ArenaPin borrows the arena.
            unsafe { cached.pointer.as_ref() }?
        } else {
            let block = partition.block(handle.slot())?;
            self.blocks[cache_index].set(CachedBlock {
                tag,
                pointer: block,
            });
            block
        };
        let offset = handle.slot() as usize & BLOCK_MASK;
        let slot = &block.entries[offset];
        let generation = &block.generations[offset];
        let before = generation.load(Ordering::Acquire) & MAX_GENERATION;
        if before != handle.generation() {
            return None;
        }
        let pointer = self.guard.protect(slot, Ordering::Acquire);
        let after = generation.load(Ordering::Acquire) & MAX_GENERATION;
        if pointer.is_null() || before != after {
            return None;
        }
        // SAFETY: pointer was protected by self.guard and the returned
        // reference cannot outlive the ArenaPin that owns that guard.
        Some(unsafe { &*pointer })
    }

    pub(crate) fn refresh(&mut self) {
        self.guard.refresh();
    }

    pub(crate) fn thread_id(&self) -> usize {
        self.guard.thread_id()
    }
}

impl<T> ArenaValue<T> {
    /// Takes ownership of one collector guard protecting `pointer`.
    ///
    /// # Safety
    ///
    /// `guard` must have been entered from `collector`, and `pointer` must be a
    /// live non-null allocation protected by that guard for the complete
    /// lifetime transferred into the returned value.
    #[allow(
        unsafe_code,
        reason = "owns a protected pointer and its transmuted collector guard"
    )]
    unsafe fn protected(
        collector: &Arc<Collector>,
        guard: LocalGuard<'_>,
        pointer: *mut T,
    ) -> Self {
        let pointer = NonNull::new(pointer).expect("protected arena pointer is non-null");
        // SAFETY: the collector is heap-owned by the Arc stored in this value.
        // Its address is stable, and Drop destroys the guard before that Arc.
        let guard = unsafe { std::mem::transmute::<LocalGuard<'_>, LocalGuard<'static>>(guard) };
        Self {
            guard: ManuallyDrop::new(guard),
            _collector: Arc::clone(collector),
            pointer,
            _value: PhantomData,
        }
    }

    /// Retires an unreachable allocation while retaining its current guard.
    ///
    /// # Safety
    ///
    /// `guard` must have been entered from `collector`. `pointer` must be a
    /// live allocation created for `reclaim::boxed`, must be unreachable to
    /// future arena loads, and must not be retired anywhere else.
    #[allow(
        unsafe_code,
        reason = "retires one unreachable pointer through its exact collector guard"
    )]
    unsafe fn retired(collector: &Arc<Collector>, guard: LocalGuard<'_>, pointer: *mut T) -> Self {
        // SAFETY: the caller has already made pointer unreachable to future
        // arena loads. It originated from Box::into_raw, and this guard keeps
        // the current protected access alive until the returned value drops.
        unsafe { guard.defer_retire(pointer, reclaim::boxed) };
        // SAFETY: this function's contract carries the same collector, guard,
        // liveness, and allocation proof required by `protected`.
        unsafe { Self::protected(collector, guard, pointer) }
    }
}

#[allow(
    unsafe_code,
    reason = "dereferences a pointer retained by the owned collector guard"
)]
impl<T> Deref for ArenaValue<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: construction requires a collector-protected pointer, and the
        // owned guard remains active for the complete lifetime of self.
        unsafe { self.pointer.as_ref() }
    }
}

#[allow(
    unsafe_code,
    reason = "manually releases the owned collector guard after the pointer"
)]
impl<T> Drop for ArenaValue<T> {
    fn drop(&mut self) {
        // SAFETY: ManuallyDrop prevents automatic destruction; the guard must
        // leave its reservation before the final collector Arc is released.
        unsafe { ManuallyDrop::drop(&mut self.guard) };
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DirectHandle(NonMaxU64);

/// Unique ownership of an entry that has been made unreachable from the
/// direct-cache index and must be reclaimed exactly once.
///
/// Unlike [`DirectHandle`], this capability is deliberately neither `Copy`
/// nor `Clone`: queuing it for reclamation consumes the sole retirement
/// responsibility.
#[repr(transparent)]
#[derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RemovedDirectHandle(DirectHandle);

/// A direct handle proven to belong to the arena protected for `'pin`.
///
/// The proof is established only by [`DirectArenaPin::protect`]. Keeping the
/// raw handle and its pin lifetime in one transparent value prevents ordinary
/// safe crate code from dereferencing an unbranded or foreign handle.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct ProtectedDirectHandle<'pin, V> {
    handle: DirectHandle,
    marker: PhantomData<&'pin DirectArenaEntry<V>>,
}

const _: () =
    assert!(std::mem::size_of::<RemovedDirectHandle>() == std::mem::size_of::<DirectHandle>());
const _: () =
    assert!(std::mem::align_of::<RemovedDirectHandle>() == std::mem::align_of::<DirectHandle>());

impl<'pin, V> ProtectedDirectHandle<'pin, V> {
    #[inline]
    #[allow(
        unsafe_code,
        reason = "dereferences the live same-arena handle proven at construction"
    )]
    pub(crate) fn entry(&self) -> &'pin DirectArenaEntry<V> {
        // SAFETY: `DirectArenaPin::protect` requires this handle to name a live
        // allocation in the arena protected for the complete `'pin` lifetime.
        unsafe { &*self.handle.pointer::<V>() }
    }
}

impl DirectHandle {
    fn from_pointer<V>(pointer: *mut DirectArenaEntry<V>) -> Self {
        let address = pointer.expose_provenance();
        debug_assert_eq!(address & DIRECT_POINTER_TAGS, 0);
        let address = u64::try_from(address).expect("pointer address fits u64");
        Self(NonMaxU64::new(address).expect("allocated pointer is not the reserved marker"))
    }

    fn from_boxed_pointer<V>(pointer: *mut DirectArenaEntry<V>) -> Self {
        let address = pointer.expose_provenance();
        debug_assert_eq!(address & DIRECT_POINTER_TAGS, 0);
        let tagged = address | DIRECT_BOXED_TAG;
        let tagged = u64::try_from(tagged).expect("tagged pointer address fits u64");
        Self(NonMaxU64::new(tagged).expect("allocated pointer is not the reserved marker"))
    }

    #[cfg(feature = "prepared-keys")]
    fn from_recycled_boxed_pointer<V>(pointer: *mut DirectArenaEntry<V>) -> Self {
        let address = pointer.expose_provenance();
        debug_assert_eq!(address & DIRECT_POINTER_TAGS, 0);
        let tagged = address | DIRECT_BOXED_TAG | DIRECT_RECYCLED_BOXED_TAG;
        let tagged = u64::try_from(tagged).expect("tagged pointer address fits u64");
        Self(NonMaxU64::new(tagged).expect("allocated pointer is not the reserved marker"))
    }

    /// Reconstructs a published direct-arena handle loaded from the cache's
    /// authoritative index.
    ///
    /// # Safety
    ///
    /// `value` must be an unchanged value previously produced by
    /// [`Self::index_value`] for a live allocation in this arena.
    #[allow(
        unsafe_code,
        reason = "declares the raw index-to-arena handle boundary"
    )]
    pub(crate) const unsafe fn from_index_value(value: NonMaxU64) -> Self {
        Self(value)
    }

    pub(crate) const fn index_value(self) -> NonMaxU64 {
        self.0
    }

    fn pointer<V>(self) -> *mut DirectArenaEntry<V> {
        let address = usize::try_from(self.0.get()).expect("pointer address fits usize")
            & !DIRECT_POINTER_TAGS;
        std::ptr::with_exposed_provenance_mut(address)
    }

    const fn is_boxed(self) -> bool {
        self.0.get() & DIRECT_BOXED_TAG as u64 != 0
    }

    #[cfg(feature = "prepared-keys")]
    const fn is_recycled_boxed(self) -> bool {
        self.0.get() & DIRECT_RECYCLED_BOXED_TAG as u64 != 0
    }
}

impl RemovedDirectHandle {
    /// Converts an index handle after an exact removal or replacement has
    /// transferred its sole reclamation responsibility to the caller.
    ///
    /// # Safety
    ///
    /// `handle` must have been returned by the exact index operation that made
    /// this allocation unreachable. No other `RemovedDirectHandle` may have
    /// been constructed for the same publication.
    #[inline]
    #[allow(
        unsafe_code,
        reason = "declares the unique-retirement ownership transition"
    )]
    pub(crate) const unsafe fn from_exact_removal(handle: DirectHandle) -> Self {
        Self(handle)
    }

    #[inline]
    const fn published(&self) -> DirectHandle {
        self.0
    }
}

#[repr(C)]
pub(crate) struct DirectArenaEntry<V> {
    metadata: AtomicU64,
    value: V,
}

impl<V> DirectArenaEntry<V> {
    fn new(value: V, weight: u32, expires_at: u64) -> Self {
        Self {
            metadata: AtomicU64::new(
                u64::from(weight) | (u64::from(encode_direct_expiry(expires_at)) << 32),
            ),
            value,
        }
    }

    pub(crate) const fn value(&self) -> &V {
        &self.value
    }

    pub(crate) fn weight(&self) -> u64 {
        self.metadata.load(Ordering::Relaxed) & u64::from(u32::MAX)
    }

    pub(crate) fn expires_at(&self) -> u64 {
        // Entry publication is ordered by the index. Expiry is independent
        // atomic state and does not publish any data guarded by this load.
        let expiry = ((self.metadata.load(Ordering::Relaxed) & DIRECT_EXPIRY_MASK)
            >> DIRECT_EXPIRY_SHIFT) as u32;
        if expiry == DIRECT_NEVER_EXPIRES {
            u64::MAX
        } else {
            u64::from(expiry)
        }
    }

    pub(crate) fn set_expires_at(&self, expires_at: u64) {
        let expiry = u64::from(encode_direct_expiry(expires_at)) << DIRECT_EXPIRY_SHIFT;
        let mut current = self.metadata.load(Ordering::Relaxed);
        loop {
            let next = (current & !DIRECT_EXPIRY_MASK) | expiry;
            match self.metadata.compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn mark_accessed(&self) {
        if self.metadata.load(Ordering::Relaxed) & DIRECT_ACCESS_BIT == 0 {
            self.metadata.fetch_or(DIRECT_ACCESS_BIT, Ordering::Relaxed);
        }
    }

    pub(crate) fn take_accessed(&self) -> bool {
        self.metadata
            .fetch_and(!DIRECT_ACCESS_BIT, Ordering::Relaxed)
            & DIRECT_ACCESS_BIT
            != 0
    }
}

pub(crate) struct DirectArenaValue<V> {
    owner: Arc<DirectArenaOwner<V>>,
    reader_epoch: usize,
    reader_shard: usize,
    pointer: NonNull<DirectArenaEntry<V>>,
}

pub(crate) struct DirectValueArena<V> {
    owner: Arc<DirectArenaOwner<V>>,
}

#[cfg(feature = "cache-diagnostics")]
pub(crate) struct DirectArenaReclamationSnapshot {
    pub(crate) current_epoch: usize,
    pub(crate) readers: [usize; DIRECT_EPOCHS],
    pub(crate) retired_values: usize,
    pub(crate) published_retired_values: usize,
    pub(crate) recyclable_allocations: usize,
}

pub(crate) struct DirectArenaReservation<'arena, V> {
    arena: &'arena DirectValueArena<V>,
    pointers: [*mut DirectArenaEntry<V>; DIRECT_RESERVATION_SIZE],
    next: usize,
    len: usize,
}

#[cfg(feature = "prepared-keys")]
pub(crate) struct DirectRecycledBoxReservation<'arena, V> {
    arena: &'arena DirectValueArena<V>,
    thread_id: usize,
    pointers: [*mut DirectArenaEntry<V>; DIRECT_RECLAIM_PUBLISH_BATCH],
    next: usize,
    len: usize,
}

pub(crate) struct DirectArenaPin<'arena, V> {
    arena: &'arena DirectValueArena<V>,
    thread_id: usize,
    reader_epoch: usize,
    reader_shard: usize,
    active: bool,
    _not_send: PhantomData<*mut ()>,
}

pub(crate) struct DirectArenaMutationPin<'arena, V> {
    arena: &'arena DirectValueArena<V>,
    thread_id: usize,
    reservation: DirectMutationReservation,
    _not_send: PhantomData<*mut ()>,
}

#[derive(Clone, Copy)]
enum DirectMutationReservation {
    Slot(usize),
    Overflow,
}

impl<V> DirectValueArena<V> {
    pub(crate) fn new(partitions: usize) -> Self {
        Self {
            owner: Arc::new(DirectArenaOwner::new(partitions)),
        }
    }

    #[allow(
        unsafe_code,
        reason = "initializes one uniquely reserved direct-arena allocation"
    )]
    pub(crate) fn allocate(&self, value: V, weight: u32, expires_at: u64) -> DirectHandle {
        let pointer = self.owner.state.allocate();
        // SAFETY: allocate returns a unique uninitialized or epoch-reclaimed
        // slot that cannot be observed until the returned handle is published.
        unsafe { pointer.write(DirectArenaEntry::new(value, weight, expires_at)) };
        DirectHandle::from_pointer(pointer)
    }

    pub(crate) fn thread_id() -> usize {
        direct_arena_thread()
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn allocate_boxed(&self, value: V, weight: u32, expires_at: u64) -> DirectHandle {
        DirectHandle::from_boxed_pointer(Box::into_raw(Box::new(DirectArenaEntry::new(
            value, weight, expires_at,
        ))))
    }

    #[cfg(feature = "prepared-keys")]
    pub(crate) fn recycled_boxed_reservation(
        &self,
        count: usize,
    ) -> DirectRecycledBoxReservation<'_, V> {
        assert!(
            count <= DIRECT_RECLAIM_PUBLISH_BATCH,
            "recycled box reservations contain at most 64 items"
        );
        let thread_id = direct_arena_thread();
        let mut pointers = [ptr::null_mut(); DIRECT_RECLAIM_PUBLISH_BATCH];
        self.owner
            .reserve_recycled_boxes(thread_id, &mut pointers[..count]);
        DirectRecycledBoxReservation {
            arena: self,
            thread_id,
            pointers,
            next: 0,
            len: count,
        }
    }

    pub(crate) fn pin(&self) -> DirectArenaPin<'_, V> {
        let thread_id = direct_arena_thread();
        let reader_shard = thread_id & (DIRECT_READER_SHARDS - 1);
        let reader_epoch = self.owner.enter(reader_shard);
        DirectArenaPin {
            arena: self,
            thread_id,
            reader_epoch,
            reader_shard,
            active: true,
            _not_send: PhantomData,
        }
    }

    pub(crate) fn mutation_pin(&self) -> DirectArenaMutationPin<'_, V> {
        let thread_id = direct_arena_thread();
        let mutator_shard = thread_id & (DIRECT_READER_SHARDS - 1);
        self.owner.activate_mutators();
        let reservation = self.owner.enter_mutator(mutator_shard);
        DirectArenaMutationPin {
            arena: self,
            thread_id,
            reservation,
            _not_send: PhantomData,
        }
    }

    /// Reclaims a candidate that was never published in an index.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this exact arena, must not be reachable from
    /// any index, and no reader may hold or later observe it.
    #[allow(
        unsafe_code,
        reason = "declares the unpublished direct-allocation ownership boundary"
    )]
    pub(crate) unsafe fn drop_unpublished(&self, handle: DirectHandle) {
        // SAFETY: callers use this only for a candidate that was never
        // published in the index, so no concurrent reader can hold it. The
        // slot is returned directly rather than entering epoch retirement.
        unsafe { self.owner.reclaim_immediate(handle) };
    }

    /// Reads an exactly removed entry before it is queued for reclamation.
    ///
    /// # Safety
    ///
    /// `handle` must still be allocated and belong to the arena whose removal
    /// operation transferred its unique retirement responsibility.
    #[allow(
        unsafe_code,
        reason = "dereferences one exact removed direct allocation"
    )]
    pub(crate) unsafe fn removed_weight(handle: &RemovedDirectHandle) -> u64 {
        // SAFETY: this private helper is used only after an exact index
        // removal transfers the handle's retirement responsibility.
        unsafe { (&*handle.published().pointer::<V>()).weight() }
    }

    pub(crate) fn reservation(&self) -> DirectArenaReservation<'_, V> {
        DirectArenaReservation {
            arena: self,
            pointers: [ptr::null_mut(); DIRECT_RESERVATION_SIZE],
            next: 0,
            len: 0,
        }
    }

    pub(crate) fn reclaim_retired(&self) {
        for _ in 0..DIRECT_EPOCHS {
            self.owner.try_advance(true);
        }
    }

    #[cfg(feature = "cache-diagnostics")]
    pub(crate) fn reclamation_snapshot(&self) -> DirectArenaReclamationSnapshot {
        self.owner.reclamation_snapshot()
    }
}

impl<V> DirectArenaReservation<'_, V> {
    #[allow(
        unsafe_code,
        reason = "initializes one uniquely reserved direct-arena slot"
    )]
    pub(crate) fn allocate(&mut self, value: V, weight: u32, expires_at: u64) -> DirectHandle {
        if self.next == self.len {
            self.arena.owner.state.reserve(&mut self.pointers);
            self.next = 0;
            self.len = self.pointers.len();
        }
        let pointer = self.pointers[self.next];
        self.next += 1;
        // SAFETY: reserve returns a unique slot marked occupied in the arena.
        // It cannot be observed until the returned handle is published.
        unsafe { pointer.write(DirectArenaEntry::new(value, weight, expires_at)) };
        DirectHandle::from_pointer(pointer)
    }
}

#[cfg(feature = "prepared-keys")]
#[allow(unsafe_code, reason = "initializes uniquely reserved recycled boxes")]
impl<V> DirectRecycledBoxReservation<'_, V> {
    pub(crate) fn allocate(&mut self, value: V, weight: u32, expires_at: u64) -> DirectHandle {
        assert!(self.next < self.len, "recycled box reservation exhausted");
        let pointer = self.pointers[self.next];
        self.next += 1;
        // SAFETY: the reservation owns a unique uninitialized box until this
        // initialized handle is published or reclaimed by its caller.
        unsafe { pointer.write(DirectArenaEntry::new(value, weight, expires_at)) };
        DirectHandle::from_recycled_boxed_pointer(pointer)
    }
}

#[cfg(feature = "prepared-keys")]
impl<V> Drop for DirectRecycledBoxReservation<'_, V> {
    #[allow(
        unsafe_code,
        reason = "returns only the reservation's uniquely owned unused boxes"
    )]
    fn drop(&mut self) {
        // SAFETY: allocation consumes entries in order, so this suffix contains
        // exactly the unique boxes that remain uninitialized and unpublished.
        unsafe {
            self.arena.owner.release_recycled_uninitialized(
                self.thread_id,
                &self.pointers[self.next..self.len],
            );
        }
    }
}

impl<V> Drop for DirectArenaReservation<'_, V> {
    #[allow(
        unsafe_code,
        reason = "returns only the reservation's uniquely owned unused slots"
    )]
    fn drop(&mut self) {
        for pointer in &self.pointers[self.next..self.len] {
            // SAFETY: allocation consumes slots in order, so this suffix
            // contains exactly the unique slots never initialized or published.
            unsafe { self.arena.owner.release_uninitialized(*pointer) };
        }
    }
}

impl<V> DirectArenaPin<'_, V> {
    /// Binds a live direct handle to this pin's reader epoch.
    ///
    /// # Safety
    ///
    /// `handle` must have been loaded unchanged from this pin's cache index, or
    /// name an unpublished allocation owned by this exact arena. It must remain
    /// live for this pin's reader epoch.
    #[inline]
    #[allow(
        unsafe_code,
        clippy::unused_self,
        reason = "declares the same-arena protected-handle lifetime boundary"
    )]
    pub(crate) unsafe fn protect(&self, handle: DirectHandle) -> ProtectedDirectHandle<'_, V> {
        ProtectedDirectHandle {
            handle,
            marker: PhantomData,
        }
    }

    /// Queues one exactly removed entry for epoch reclamation.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this pin's arena, be unreachable from every
    /// index, and carry the sole retirement responsibility for its entry. This
    /// pin must have been active before the index mutation made `handle`
    /// unreachable and must remain active through this call.
    #[allow(
        unsafe_code,
        reason = "declares the exact removed-allocation retirement boundary"
    )]
    pub(crate) unsafe fn retire(&self, handle: RemovedDirectHandle) {
        // SAFETY: this function's contract binds the unique removed token to
        // this pin's exact arena owner.
        unsafe { self.arena.owner.retire(self.thread_id, handle) };
    }

    /// Queues exactly removed entries while retaining the mutator epoch.
    ///
    /// # Safety
    ///
    /// Every handle must belong to this pin's arena, be unreachable from every
    /// index, and carry the sole retirement responsibility for its entry. This
    /// pin must have been active before every corresponding index mutation and
    /// remain active through this call.
    #[allow(
        unsafe_code,
        reason = "binds exact batch retirement to its live mutator pin"
    )]
    pub(crate) unsafe fn retire_batch(&self, handles: &mut Vec<RemovedDirectHandle>) {
        // SAFETY: this function's contract binds the exact removed tokens and
        // their complete publication window to this pin's exact arena owner.
        unsafe { self.arena.owner.retire_batch(self.thread_id, handles) };
    }

    /// Transfers this pin's reader protection into an owned protected value.
    ///
    /// # Safety
    ///
    /// `handle` must name a live entry in this pin's exact arena and remain
    /// protected by this pin's reader epoch during the transfer.
    #[allow(
        unsafe_code,
        reason = "transfers one active reader epoch into an owned protected value"
    )]
    pub(crate) unsafe fn into_protected(mut self, handle: DirectHandle) -> DirectArenaValue<V> {
        self.active = false;
        // SAFETY: this function's contract binds the live handle to this exact
        // pin. Disabling `self.active` transfers its one reader reservation to
        // the returned owner, which keeps the arena allocation alive.
        unsafe {
            DirectArenaValue::protected(
                &self.arena.owner,
                self.reader_epoch,
                self.reader_shard,
                handle.pointer::<V>(),
            )
        }
    }

    /// Retires an exactly removed entry while retaining reader protection.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this pin's exact arena, be unreachable from all
    /// indices, and carry the sole retirement responsibility for its entry.
    /// This pin must have been active before the exact removal.
    #[allow(
        unsafe_code,
        reason = "retires an exact allocation while transferring reader protection"
    )]
    pub(crate) unsafe fn into_retired(self, handle: RemovedDirectHandle) -> DirectArenaValue<V> {
        let published = handle.published();
        // SAFETY: this function's contract proves both operations use the same
        // pin/arena and the same uniquely removed allocation.
        unsafe { self.retire(handle) };
        // SAFETY: retirement defers reclamation beyond this pin's reader epoch.
        unsafe { self.into_protected(published) }
    }

    pub(crate) fn refresh(&mut self) {
        self.arena.owner.leave(self.reader_epoch, self.reader_shard);
        self.thread_id = direct_arena_thread();
        self.reader_shard = self.thread_id & (DIRECT_READER_SHARDS - 1);
        self.reader_epoch = self.arena.owner.enter(self.reader_shard);
    }
}

impl<V> DirectArenaMutationPin<'_, V> {
    /// Queues one exact removal while retaining the mutation window.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this pin's arena, be unreachable from every
    /// index, and carry its sole retirement responsibility. This mutation pin
    /// must have been active before the exact removal and remain active here.
    #[allow(
        unsafe_code,
        reason = "binds exact retirement to one active mutation window"
    )]
    pub(crate) unsafe fn retire(&self, handle: RemovedDirectHandle) {
        // SAFETY: this function's contract binds the exact removed token and
        // its complete mutation window to this exact arena owner.
        unsafe { self.arena.owner.retire(self.thread_id, handle) };
    }

    /// Queues exact removals while retaining their shared mutation window.
    ///
    /// # Safety
    ///
    /// Every handle must satisfy [`Self::retire`]'s ownership and mutation
    /// window requirements for this exact pin and arena.
    #[allow(
        unsafe_code,
        reason = "binds exact batch retirement to one active mutation window"
    )]
    pub(crate) unsafe fn retire_batch(&self, handles: &mut Vec<RemovedDirectHandle>) {
        // SAFETY: this function's contract extends the same exact proof to
        // every unique token in the batch.
        unsafe { self.arena.owner.retire_batch(self.thread_id, handles) };
    }
}

impl<V> DirectArenaValue<V> {
    /// Transfers one active direct-reader epoch into an owned protected value.
    ///
    /// # Safety
    ///
    /// `reader_epoch` and `reader_shard` must describe one active reader held
    /// by `owner`, and `pointer` must be a live allocation owned by that exact
    /// arena and protected for the transferred reader epoch.
    #[allow(
        unsafe_code,
        reason = "declares the raw pointer and reader-epoch ownership transfer"
    )]
    unsafe fn protected(
        owner: &Arc<DirectArenaOwner<V>>,
        reader_epoch: usize,
        reader_shard: usize,
        pointer: *mut DirectArenaEntry<V>,
    ) -> Self {
        let pointer = NonNull::new(pointer).expect("protected direct-arena pointer is non-null");
        Self {
            owner: Arc::clone(owner),
            reader_epoch,
            reader_shard,
            pointer,
        }
    }
}

#[allow(
    unsafe_code,
    reason = "dereferences a pointer retained by an owned reader epoch"
)]
impl<V> Deref for DirectArenaValue<V> {
    type Target = DirectArenaEntry<V>;

    fn deref(&self) -> &Self::Target {
        // SAFETY: construction requires a collector-protected pointer, and
        // the owned guard remains active for the complete lifetime of self.
        unsafe { self.pointer.as_ref() }
    }
}

impl<V> Drop for DirectArenaValue<V> {
    fn drop(&mut self) {
        self.owner.leave(self.reader_epoch, self.reader_shard);
    }
}

impl<V> Drop for DirectArenaPin<'_, V> {
    fn drop(&mut self) {
        if self.active {
            self.arena.owner.leave(self.reader_epoch, self.reader_shard);
        }
    }
}

impl<V> Drop for DirectArenaMutationPin<'_, V> {
    fn drop(&mut self) {
        self.arena.owner.leave_mutator(self.reservation);
    }
}

struct DirectArenaBlock<V> {
    state: UnsafeCell<DirectArenaBlockState>,
    entries: [UnsafeCell<MaybeUninit<DirectArenaEntry<V>>>; DIRECT_BLOCK_SIZE],
}

struct DirectArenaBlockState {
    occupied: [u64; DIRECT_BLOCK_SIZE / u64::BITS as usize],
    live: usize,
}

// SAFETY: slot allocation and release are serialized by the owning partition.
// An occupied slot is immutable except for DirectArenaEntry's atomic metadata.
#[allow(
    unsafe_code,
    reason = "slot mutation is serialized by the owning partition"
)]
// SAFETY: allocation and release are serialized by the owning partition; an
// occupied value is otherwise accessed only through its atomic metadata.
unsafe impl<V: Send + Sync> Sync for DirectArenaBlock<V> {}

impl<V> DirectArenaBlock<V> {
    #[allow(clippy::unnecessary_box_returns)]
    fn new() -> Box<Self> {
        Box::new(Self {
            state: UnsafeCell::new(DirectArenaBlockState {
                occupied: [0; DIRECT_BLOCK_SIZE / u64::BITS as usize],
                live: 0,
            }),
            entries: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
        })
    }

    fn entry(&self, offset: usize) -> *mut DirectArenaEntry<V> {
        self.entries[offset].get().cast::<DirectArenaEntry<V>>()
    }

    fn start(&self) -> usize {
        self.entry(0).expose_provenance()
    }

    /// Claims one vacant slot while the owning partition is locked.
    ///
    /// # Safety
    ///
    /// The caller must hold the mutex for the partition that owns this block
    /// for the complete operation.
    #[allow(
        unsafe_code,
        reason = "mutates slot state under a caller-proven partition lock"
    )]
    unsafe fn claim(&self) -> Option<*mut DirectArenaEntry<V>> {
        // SAFETY: the owning partition lock serializes every state mutation.
        let state = unsafe { &mut *self.state.get() };
        for (word_index, word) in state.occupied.iter_mut().enumerate() {
            let vacant = !*word;
            if vacant == 0 {
                continue;
            }
            let bit = vacant.trailing_zeros() as usize;
            *word |= 1_u64 << bit;
            state.live += 1;
            return Some(self.entry(word_index * u64::BITS as usize + bit));
        }
        None
    }

    /// Claims vacant slots while the owning partition is locked.
    ///
    /// # Safety
    ///
    /// The caller must hold the mutex for the partition that owns this block
    /// for the complete operation.
    #[allow(
        unsafe_code,
        reason = "mutates slot state under a caller-proven partition lock"
    )]
    unsafe fn claim_many(&self, output: &mut [*mut DirectArenaEntry<V>]) -> usize {
        // SAFETY: the owning partition lock serializes every state mutation.
        let state = unsafe { &mut *self.state.get() };
        let mut claimed = 0;
        for (word_index, word) in state.occupied.iter_mut().enumerate() {
            let mut vacant = !*word;
            while vacant != 0 && claimed < output.len() {
                let bit = vacant.trailing_zeros() as usize;
                vacant &= vacant - 1;
                *word |= 1_u64 << bit;
                output[claimed] = self.entry(word_index * u64::BITS as usize + bit);
                claimed += 1;
            }
            if claimed == output.len() {
                break;
            }
        }
        state.live += claimed;
        claimed
    }

    /// Reports whether every slot is occupied.
    ///
    /// # Safety
    ///
    /// The caller must hold the mutex for the partition that owns this block
    /// for the complete operation.
    #[allow(
        unsafe_code,
        reason = "reads slot state under a caller-proven partition lock"
    )]
    unsafe fn is_full(&self) -> bool {
        // SAFETY: callers hold the owning partition lock.
        unsafe { (*self.state.get()).live == DIRECT_BLOCK_SIZE }
    }

    /// Returns the occupied-slot count.
    ///
    /// # Safety
    ///
    /// The caller must hold the mutex for the partition that owns this block
    /// for the complete operation.
    #[allow(
        unsafe_code,
        reason = "reads slot state under a caller-proven partition lock"
    )]
    unsafe fn live(&self) -> usize {
        // SAFETY: callers hold the owning partition lock.
        unsafe { (*self.state.get()).live }
    }

    /// Releases one occupied slot while the owning partition is locked.
    ///
    /// # Safety
    ///
    /// The caller must hold the mutex for the partition that owns this block,
    /// and `offset` must identify a currently occupied slot in this block.
    #[allow(
        unsafe_code,
        reason = "mutates slot state under a caller-proven partition lock"
    )]
    unsafe fn release(&self, offset: usize) -> (bool, bool) {
        // SAFETY: the owning partition lock serializes every state mutation.
        let state = unsafe { &mut *self.state.get() };
        let was_full = state.live == DIRECT_BLOCK_SIZE;
        let word = offset / u64::BITS as usize;
        let bit = offset % u64::BITS as usize;
        let mask = 1_u64 << bit;
        assert_ne!(
            state.occupied[word] & mask,
            0,
            "direct arena slot released twice"
        );
        state.occupied[word] &= !mask;
        state.live -= 1;
        (was_full, state.live == 0)
    }
}

#[allow(unsafe_code, reason = "drops exactly the slots still marked occupied")]
impl<V> Drop for DirectArenaBlock<V> {
    fn drop(&mut self) {
        let occupied_words = self.state.get_mut().occupied;
        for (word_index, mut occupied) in occupied_words.into_iter().enumerate() {
            while occupied != 0 {
                let bit = occupied.trailing_zeros() as usize;
                occupied &= occupied - 1;
                // SAFETY: occupied tracks every initialized live entry.
                unsafe {
                    ptr::drop_in_place(self.entry(word_index * u64::BITS as usize + bit));
                }
            }
        }
    }
}

struct DirectArenaPartitionState<V> {
    blocks: BTreeMap<usize, Box<DirectArenaBlock<V>>>,
    current: Option<DirectAvailableBlock<V>>,
    reclaimed: Vec<DirectAvailableBlock<V>>,
}

struct DirectAvailableBlock<V> {
    start: usize,
    pointer: NonNull<DirectArenaBlock<V>>,
}

impl<V> Copy for DirectAvailableBlock<V> {}

impl<V> Clone for DirectAvailableBlock<V> {
    fn clone(&self) -> Self {
        *self
    }
}

// SAFETY: the pointer targets a Box owned by the same partition state. It is
// dereferenced only while that partition's mutex is held, and post-publication
// mutation is confined to DirectArenaBlock's UnsafeCell state.
#[allow(
    unsafe_code,
    reason = "the owning partition keeps the pointed-to Box alive"
)]
// SAFETY: the pointer's Box is owned by the same partition state and is
// dereferenced only while that partition is locked.
unsafe impl<V: Send + Sync> Send for DirectAvailableBlock<V> {}
// SAFETY: shared access has the same partition-lock and UnsafeCell discipline
// described above; the pointee itself requires `V: Send + Sync`.
#[allow(
    unsafe_code,
    reason = "shared dereference requires the owning partition lock"
)]
// SAFETY: shared access follows the same partition-lock discipline and the
// pointee requires `V: Send + Sync`.
unsafe impl<V: Send + Sync> Sync for DirectAvailableBlock<V> {}

#[repr(align(64))]
struct DirectArenaPartition<V> {
    state: Mutex<DirectArenaPartitionState<V>>,
}

impl<V> DirectArenaPartition<V> {
    fn new() -> Self {
        Self {
            state: Mutex::new(DirectArenaPartitionState {
                blocks: BTreeMap::new(),
                current: None,
                reclaimed: Vec::new(),
            }),
        }
    }

    #[allow(
        unsafe_code,
        reason = "accesses block state while holding its owning partition lock"
    )]
    fn try_allocate_home(
        &self,
        partition_index: usize,
        availability: &[AtomicU64],
    ) -> Option<*mut DirectArenaEntry<V>> {
        let mut state = self.state.lock();
        if let Some(available) = state.current {
            // SAFETY: current retains the owning Box and the partition lock
            // prevents block removal for the duration of this access.
            let block = unsafe { available.pointer.as_ref() };
            // SAFETY: `state` is guarded by this partition's mutex, which
            // serializes the pointed-to block's slot state.
            if let Some(pointer) = unsafe { block.claim() } {
                // SAFETY: the same partition guard remains held.
                if unsafe { block.is_full() } {
                    state.current = None;
                }
                return Some(pointer);
            }
            state.current = None;
        }
        Self::try_allocate_reclaimed_locked(&mut state, partition_index, availability)
    }

    fn try_allocate_reclaimed(
        &self,
        partition_index: usize,
        availability: &[AtomicU64],
    ) -> Option<*mut DirectArenaEntry<V>> {
        let mut state = self.state.lock();
        Self::try_allocate_reclaimed_locked(&mut state, partition_index, availability)
    }

    #[allow(
        unsafe_code,
        reason = "accesses block state through exclusively locked partition state"
    )]
    fn try_allocate_reclaimed_locked(
        state: &mut DirectArenaPartitionState<V>,
        partition_index: usize,
        availability: &[AtomicU64],
    ) -> Option<*mut DirectArenaEntry<V>> {
        loop {
            if let Some(available) = state.reclaimed.last().copied() {
                // SAFETY: reclaimed retains the owning Box and the partition
                // lock prevents block removal for the duration of this access.
                let block = unsafe { available.pointer.as_ref() };
                // SAFETY: exclusive access to the partition state comes from
                // its held mutex guard and serializes this block's slot state.
                if let Some(pointer) = unsafe { block.claim() } {
                    // SAFETY: the same exclusive partition access remains held.
                    if unsafe { block.is_full() } {
                        state.reclaimed.pop();
                        if state.reclaimed.is_empty() {
                            direct_mark_partition_available(availability, partition_index, false);
                        }
                    }
                    return Some(pointer);
                }
                state.reclaimed.pop();
                continue;
            }
            direct_mark_partition_available(availability, partition_index, false);
            return None;
        }
    }

    fn allocate_new(
        &self,
        partition_index: usize,
        block_lookup: &DirectBlockLookup,
    ) -> *mut DirectArenaEntry<V> {
        let mut state = self.state.lock();
        Self::allocate_new_locked(&mut state, partition_index, block_lookup)
    }

    #[allow(
        unsafe_code,
        reason = "initializes block state through exclusively locked partition state"
    )]
    fn allocate_new_locked(
        state: &mut DirectArenaPartitionState<V>,
        partition_index: usize,
        block_lookup: &DirectBlockLookup,
    ) -> *mut DirectArenaEntry<V> {
        let block = DirectArenaBlock::new();
        let start = block.start();
        let previous = state.blocks.insert(start, block);
        assert!(previous.is_none(), "direct block address is unique");
        let stored = state
            .blocks
            .get(&start)
            .expect("new direct block remains registered");
        // SAFETY: the caller holds exclusive access to the locked partition
        // state, and this newly inserted block is reachable only through it.
        let pointer =
            unsafe { stored.claim() }.expect("a new direct block has a vacant first slot");
        let block_pointer = NonNull::from(stored.as_ref());
        debug_assert!(state.current.is_none());
        state.current = Some(DirectAvailableBlock {
            start,
            pointer: block_pointer,
        });
        block_lookup.register(
            start,
            std::mem::size_of::<DirectArenaEntry<V>>() * DIRECT_BLOCK_SIZE,
            partition_index,
        );
        pointer
    }

    #[allow(
        unsafe_code,
        reason = "accesses block state while holding its owning partition lock"
    )]
    fn reserve_home(
        &self,
        partition_index: usize,
        block_lookup: &DirectBlockLookup,
        availability: &[AtomicU64],
        pointers: &mut [*mut DirectArenaEntry<V>],
    ) {
        let mut state = self.state.lock();
        let mut filled = 0;
        while filled < pointers.len() {
            if let Some(available) = state.current {
                // SAFETY: current retains the owning Box and this lock
                // prevents removal while the slots are claimed.
                let block = unsafe { available.pointer.as_ref() };
                // SAFETY: `state` is protected by this partition's mutex for the
                // complete bulk claim.
                let claimed = unsafe { block.claim_many(&mut pointers[filled..]) };
                filled += claimed;
                // SAFETY: the same partition guard remains held.
                if claimed == 0 || unsafe { block.is_full() } {
                    state.current = None;
                }
                if claimed != 0 {
                    continue;
                }
            }
            let claimed =
                Self::try_allocate_reclaimed_locked(&mut state, partition_index, availability);
            pointers[filled] = claimed.unwrap_or_else(|| {
                Self::allocate_new_locked(&mut state, partition_index, block_lookup)
            });
            filled += 1;
        }
    }

    #[allow(
        unsafe_code,
        reason = "releases block slots while holding their owning partition lock"
    )]
    fn release_entries(
        &self,
        partition_index: usize,
        entries: &[(usize, usize)],
        block_lookup: &DirectBlockLookup,
        availability: &[AtomicU64],
    ) {
        let mut state = self.state.lock();
        let was_available = !state.reclaimed.is_empty();
        let mut empty = Vec::new();
        let mut begin = 0;
        while begin < entries.len() {
            let start = entries[begin].0;
            let mut end = begin + 1;
            while end < entries.len() && entries[end].0 == start {
                end += 1;
            }
            // SAFETY: `state` is protected by this partition's mutex while the
            // block's occupancy is inspected and updated below.
            let old_live = unsafe {
                state
                    .blocks
                    .get(&start)
                    .expect("retired direct block remains registered")
                    .live()
            };
            if old_live == DIRECT_BLOCK_SIZE {
                let pointer = NonNull::from(
                    state
                        .blocks
                        .get(&start)
                        .expect("retired direct block remains registered")
                        .as_ref(),
                );
                state
                    .reclaimed
                    .push(DirectAvailableBlock { start, pointer });
            }
            let block = state
                .blocks
                .get(&start)
                .expect("retired direct block remains registered");
            for &(_, offset) in &entries[begin..end] {
                // SAFETY: the partition mutex remains held and the retirement
                // batch contains each verified occupied slot exactly once.
                unsafe { block.release(offset) };
            }
            // SAFETY: the same partition guard remains held.
            let live = unsafe { block.live() };
            if live == 0 {
                empty.push(start);
            }
            begin = end;
        }
        if empty.is_empty() {
            if !was_available && !state.reclaimed.is_empty() {
                direct_mark_partition_available(availability, partition_index, true);
            }
            return;
        }
        state
            .reclaimed
            .retain(|available| empty.binary_search(&available.start).is_err());
        if state
            .current
            .is_some_and(|available| empty.binary_search(&available.start).is_ok())
        {
            state.current = None;
        }
        let mut removed = Vec::with_capacity(empty.len());
        for start in &empty {
            removed.push(
                state
                    .blocks
                    .remove(start)
                    .expect("empty direct block remains registered"),
            );
        }
        for start in empty {
            block_lookup.unregister(
                start,
                std::mem::size_of::<DirectArenaEntry<V>>() * DIRECT_BLOCK_SIZE,
            );
        }
        let is_available = !state.reclaimed.is_empty();
        if was_available != is_available {
            direct_mark_partition_available(availability, partition_index, is_available);
        }
        drop(state);
        drop(removed);
    }

    #[cold]
    #[allow(
        unsafe_code,
        reason = "releases one block slot while holding its owning partition lock"
    )]
    fn release_entry(
        &self,
        partition_index: usize,
        start: usize,
        offset: usize,
        block_lookup: &DirectBlockLookup,
        availability: &[AtomicU64],
    ) {
        let mut state = self.state.lock();
        let was_available = !state.reclaimed.is_empty();
        let newly_available = {
            let block = state
                .blocks
                .get(&start)
                .expect("unpublished direct block remains registered");
            // SAFETY: `state` is protected by this partition's mutex.
            (unsafe { block.live() } == DIRECT_BLOCK_SIZE).then(|| NonNull::from(block.as_ref()))
        };
        if let Some(pointer) = newly_available {
            state
                .reclaimed
                .push(DirectAvailableBlock { start, pointer });
        }
        // SAFETY: the partition mutex remains held and `offset` identifies the
        // one occupied unpublished slot being returned.
        unsafe {
            state
                .blocks
                .get(&start)
                .expect("unpublished direct block remains registered")
                .release(offset);
        }
        // SAFETY: the same partition guard remains held.
        if unsafe {
            state
                .blocks
                .get(&start)
                .expect("unpublished direct block remains registered")
                .live()
        } != 0
        {
            if !was_available && !state.reclaimed.is_empty() {
                direct_mark_partition_available(availability, partition_index, true);
            }
            return;
        }
        state.reclaimed.retain(|available| available.start != start);
        if state
            .current
            .is_some_and(|available| available.start == start)
        {
            state.current = None;
        }
        let removed = state
            .blocks
            .remove(&start)
            .expect("empty direct block remains registered");
        block_lookup.unregister(
            start,
            std::mem::size_of::<DirectArenaEntry<V>>() * DIRECT_BLOCK_SIZE,
        );
        let is_available = !state.reclaimed.is_empty();
        if was_available != is_available {
            direct_mark_partition_available(availability, partition_index, is_available);
        }
        drop(state);
        drop(removed);
    }
}

struct DirectArenaState<V> {
    partitions: Box<[DirectArenaPartition<V>]>,
    availability: Box<[AtomicU64]>,
    block_lookup: DirectBlockLookup,
}

struct DirectUnwindReleaseGuard<'a, V> {
    state: &'a DirectArenaState<V>,
    pointers: &'a [*mut DirectArenaEntry<V>],
    completed: usize,
    requeue: Option<(&'a DirectRetiredQueue, &'a AtomicUsize)>,
}

impl<V> Drop for DirectUnwindReleaseGuard<'_, V> {
    #[allow(
        unsafe_code,
        reason = "releases only quiescent slots whose destructors already unwound"
    )]
    fn drop(&mut self) {
        // This guard is forgotten after every destructor returns normally.
        // Therefore this deliberately slower one-at-a-time lookup runs only
        // while unwinding from user `Drop` code.
        for pointer in &self.pointers[..self.completed] {
            // SAFETY: `completed` contains only distinct slots from this state
            // whose initialized values have already completed destruction or
            // unwound out of their destructor.
            unsafe { self.state.release_pointer(*pointer) };
        }
        let Some((queue, retired_count)) = self.requeue else {
            return;
        };
        let unprocessed = &self.pointers[self.completed..];
        if unprocessed.is_empty() {
            return;
        }
        let mut retired = queue.0.lock();
        for pointer in unprocessed {
            let published = DirectHandle::from_pointer(*pointer);
            // SAFETY: these distinct quiescent arena entries were not reached
            // by the destructor loop, and this unwind path transfers their
            // original retirement responsibility back to the owner queue.
            retired.push(unsafe { RemovedDirectHandle::from_exact_removal(published) });
        }
        retired_count.fetch_add(unprocessed.len(), Ordering::Relaxed);
    }
}

struct DirectSingleReleaseGuard<'a, V> {
    state: &'a DirectArenaState<V>,
    partition: usize,
    block_start: usize,
    offset: usize,
}

impl<V> Drop for DirectSingleReleaseGuard<'_, V> {
    fn drop(&mut self) {
        self.state.partitions[self.partition].release_entry(
            self.partition,
            self.block_start,
            self.offset,
            &self.state.block_lookup,
            &self.state.availability,
        );
    }
}

struct DirectBlockLookup {
    shards: [RwLock<BTreeMap<usize, (usize, usize)>>; DIRECT_LOOKUP_SHARDS],
}

impl DirectBlockLookup {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
        }
    }

    fn register(&self, start: usize, bytes: usize, partition: usize) {
        Self::for_each_segment(start, bytes, |shard, key| {
            let previous = self.shards[shard].write().insert(key, (start, partition));
            assert!(previous.is_none(), "direct block lookup address is unique");
        });
    }

    fn unregister(&self, start: usize, bytes: usize) {
        Self::for_each_segment(start, bytes, |shard, key| {
            let removed = self.shards[shard].write().remove(&key);
            debug_assert!(removed.is_some());
        });
    }

    fn locate(&self, address: usize, block_bytes: usize) -> (usize, usize) {
        let segment = address >> DIRECT_LOOKUP_SEGMENT_BITS;
        let shard = segment & (DIRECT_LOOKUP_SHARDS - 1);
        let lookup = self.shards[shard].read();
        let (&_, &(start, partition)) = lookup
            .range(..=address)
            .next_back()
            .expect("arena pointer belongs to a registered direct block");
        assert!(
            address < start + block_bytes,
            "arena pointer is inside its direct block"
        );
        (start, partition)
    }

    fn for_each_segment(start: usize, bytes: usize, mut visit: impl FnMut(usize, usize)) {
        let first = start >> DIRECT_LOOKUP_SEGMENT_BITS;
        let last = (start + bytes - 1) >> DIRECT_LOOKUP_SEGMENT_BITS;
        for segment in first..=last {
            let shard = segment & (DIRECT_LOOKUP_SHARDS - 1);
            let segment_start = segment << DIRECT_LOOKUP_SEGMENT_BITS;
            visit(shard, start.max(segment_start));
        }
    }
}

fn direct_mark_partition_available(availability: &[AtomicU64], partition: usize, available: bool) {
    let word = partition / u64::BITS as usize;
    let mask = 1_u64 << (partition % u64::BITS as usize);
    if available {
        availability[word].fetch_or(mask, Ordering::Release);
    } else {
        availability[word].fetch_and(!mask, Ordering::Release);
    }
}

#[repr(align(64))]
struct DirectReaderCounter(AtomicUsize);

impl DirectReaderCounter {
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }
}

#[repr(align(64))]
struct DirectMutatorSlot(AtomicBool);

impl DirectMutatorSlot {
    fn new() -> Self {
        Self(AtomicBool::new(false))
    }
}

#[repr(align(64))]
struct DirectRetiredQueue(Mutex<Vec<RemovedDirectHandle>>);

impl DirectRetiredQueue {
    fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }
}

/// Owns retirements removed from one epoch queue until reclamation consumes
/// them. Its exceptional `Drop` path returns any unprocessed tokens to an
/// owner queue without invoking user code.
struct DirectRetiredBatch<'a> {
    handles: Vec<RemovedDirectHandle>,
    requeue: &'a DirectRetiredQueue,
    retired_count: &'a AtomicUsize,
}

impl<'a> DirectRetiredBatch<'a> {
    fn new(
        handles: Vec<RemovedDirectHandle>,
        requeue: &'a DirectRetiredQueue,
        retired_count: &'a AtomicUsize,
    ) -> Self {
        Self {
            handles,
            requeue,
            retired_count,
        }
    }

    fn pop(&mut self) -> Option<RemovedDirectHandle> {
        self.handles.pop()
    }

    fn push(&mut self, handle: RemovedDirectHandle) {
        self.handles.push(handle);
    }

    fn disarm(mut self) -> Vec<RemovedDirectHandle> {
        std::mem::take(&mut self.handles)
    }
}

impl Drop for DirectRetiredBatch<'_> {
    fn drop(&mut self) {
        if self.handles.is_empty() {
            return;
        }
        let count = self.handles.len();
        self.requeue.0.lock().append(&mut self.handles);
        self.retired_count.fetch_add(count, Ordering::Relaxed);
    }
}

#[cfg(feature = "prepared-keys")]
struct DirectRecycledBoxGuard<V> {
    pointer: *mut DirectArenaEntry<V>,
}

#[cfg(feature = "prepared-keys")]
#[allow(
    unsafe_code,
    reason = "deallocates one uniquely owned recycled box after value destruction"
)]
impl<V> Drop for DirectRecycledBoxGuard<V> {
    fn drop(&mut self) {
        // SAFETY: the value has already completed destruction or unwound, and
        // the guard uniquely owns the original Box allocation.
        unsafe {
            drop(Box::from_raw(
                self.pointer.cast::<MaybeUninit<DirectArenaEntry<V>>>(),
            ));
        }
    }
}

struct DirectArenaOwner<V> {
    state: DirectArenaState<V>,
    // Monotonic token. The low two bits select physical slot 0, 1, or 2; the
    // remaining bits prevent a stalled reader from accepting a slot after the
    // collector completes a full cycle. Encoding the slot avoids division on
    // reader entry.
    current_epoch: AtomicUsize,
    mutator_overflow: AtomicUsize,
    mutator_activity: AtomicBool,
    mutators: [DirectMutatorSlot; DIRECT_READER_SHARDS],
    readers: [DirectReaderCounter; DIRECT_EPOCHS * DIRECT_READER_SHARDS],
    retired: [DirectRetiredQueue; DIRECT_EPOCHS * DIRECT_RETIRED_SHARDS],
    retired_count: [AtomicUsize; DIRECT_EPOCHS],
    #[cfg(feature = "prepared-keys")]
    recycled_boxes: OnceLock<Box<[Mutex<Vec<DirectHandle>>]>>,
    reclaim_gate: Mutex<()>,
}

impl<V> DirectArenaOwner<V> {
    fn new(partitions: usize) -> Self {
        Self {
            state: DirectArenaState::new(partitions),
            current_epoch: AtomicUsize::new(0),
            mutator_overflow: AtomicUsize::new(0),
            mutator_activity: AtomicBool::new(false),
            mutators: std::array::from_fn(|_| DirectMutatorSlot::new()),
            readers: std::array::from_fn(|_| DirectReaderCounter::new()),
            retired: std::array::from_fn(|_| DirectRetiredQueue::new()),
            retired_count: std::array::from_fn(|_| AtomicUsize::new(0)),
            #[cfg(feature = "prepared-keys")]
            recycled_boxes: OnceLock::new(),
            reclaim_gate: Mutex::new(()),
        }
    }

    #[cfg(feature = "cache-diagnostics")]
    fn reclamation_snapshot(&self) -> DirectArenaReclamationSnapshot {
        let readers = std::array::from_fn(|epoch| {
            self.readers[epoch * DIRECT_READER_SHARDS..(epoch + 1) * DIRECT_READER_SHARDS]
                .iter()
                .map(|counter| counter.0.load(Ordering::SeqCst))
                .sum()
        });
        let retired_values = self.retired.iter().map(|queue| queue.0.lock().len()).sum();
        let published_retired_values = self
            .retired_count
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .sum();
        #[cfg(feature = "prepared-keys")]
        let recyclable_allocations = self.recycled_boxes.get().map_or(0, |shards| {
            shards.iter().map(|shard| shard.lock().len()).sum()
        });
        #[cfg(not(feature = "prepared-keys"))]
        let recyclable_allocations = 0;
        DirectArenaReclamationSnapshot {
            current_epoch: direct_epoch_slot(self.current_epoch.load(Ordering::SeqCst)),
            readers,
            retired_values,
            published_retired_values,
            recyclable_allocations,
        }
    }

    #[cfg(feature = "prepared-keys")]
    fn recycled_boxes(&self) -> &[Mutex<Vec<DirectHandle>>] {
        self.recycled_boxes.get_or_init(|| {
            (0..DIRECT_RETIRED_SHARDS)
                .map(|_| Mutex::new(Vec::new()))
                .collect()
        })
    }

    #[cfg(feature = "prepared-keys")]
    fn reserve_recycled_boxes(&self, thread_id: usize, pointers: &mut [*mut DirectArenaEntry<V>]) {
        let shard = thread_id & (DIRECT_RETIRED_SHARDS - 1);
        let mut recycled = self.recycled_boxes()[shard].lock();
        let recycled_count = recycled.len().min(pointers.len());
        for output in &mut pointers[..recycled_count] {
            *output = recycled
                .pop()
                .expect("recycled box count was checked")
                .pointer();
        }
        drop(recycled);
        for output in &mut pointers[recycled_count..] {
            let allocation = Box::new(MaybeUninit::<DirectArenaEntry<V>>::uninit());
            *output = Box::into_raw(allocation).cast();
        }
    }

    #[cfg(feature = "prepared-keys")]
    /// Returns never-initialized recycled boxes to this exact owner's pool.
    ///
    /// # Safety
    ///
    /// Every pointer must be a unique, still-uninitialized box reserved from
    /// this owner and must not be released or initialized anywhere else.
    #[allow(
        unsafe_code,
        reason = "returns caller-proven recycled boxes to their exact owner"
    )]
    unsafe fn release_recycled_uninitialized(
        &self,
        thread_id: usize,
        pointers: &[*mut DirectArenaEntry<V>],
    ) {
        if pointers.is_empty() {
            return;
        }
        let shard = thread_id & (DIRECT_RETIRED_SHARDS - 1);
        let mut recycled = self.recycled_boxes()[shard].lock();
        let available = DIRECT_RECYCLED_BOX_CAPACITY_PER_SHARD.saturating_sub(recycled.len());
        let keep = pointers.len().min(available);
        recycled.extend(
            pointers[..keep]
                .iter()
                .map(|pointer| DirectHandle::from_recycled_boxed_pointer(*pointer)),
        );
        drop(recycled);
        for pointer in &pointers[keep..] {
            // SAFETY: the reservation uniquely owns these never-initialized
            // allocations and they use the matching Box layout.
            unsafe {
                drop(Box::from_raw(
                    pointer.cast::<MaybeUninit<DirectArenaEntry<V>>>(),
                ));
            }
        }
    }

    fn enter(&self, reader_shard: usize) -> usize {
        enter_epoch(
            || self.current_epoch.load(Ordering::SeqCst),
            |epoch| {
                let counter = &self.readers[epoch * DIRECT_READER_SHARDS + reader_shard];
                acquire_direct_reader(&counter.0);
            },
            |epoch| {
                self.readers[epoch * DIRECT_READER_SHARDS + reader_shard]
                    .0
                    .fetch_sub(1, Ordering::SeqCst)
            },
        )
    }

    fn activate_mutators(&self) {
        if self.mutator_activity.load(Ordering::SeqCst) {
            return;
        }
        let owner = ptr::from_ref(self).addr();
        if DIRECT_RECLAIM_OWNER.with(|current| current.get() == owner) {
            // A value destructor has re-entered this exact arena while the
            // current thread already owns `reclaim_gate`. Announce mutation
            // observation directly: trying to acquire the non-reentrant gate
            // again would deadlock. This is still serialized by that gate and
            // affects only the first reentrant mutation, not a hot path.
            self.mutator_activity.store(true, Ordering::SeqCst);
            return;
        }
        // First use synchronizes once with the collector gate before any index
        // mutation. A collector either finishes first or observes the
        // permanent flag; it can never skip a concurrently active first slot.
        let _reclaim = self.reclaim_gate.lock();
        if !self.mutator_activity.load(Ordering::Relaxed) {
            self.mutator_activity.store(true, Ordering::SeqCst);
        }
    }

    fn enter_mutator(&self, preferred_shard: usize) -> DirectMutationReservation {
        for offset in 0..DIRECT_READER_SHARDS {
            let slot = (preferred_shard + offset) & (DIRECT_READER_SHARDS - 1);
            if self.mutators[slot]
                .0
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return DirectMutationReservation::Slot(slot);
            }
        }
        acquire_direct_reader(&self.mutator_overflow);
        DirectMutationReservation::Overflow
    }

    fn leave(&self, reader_epoch: usize, reader_shard: usize) {
        leave_epoch(reader_epoch, |epoch| {
            self.readers[epoch * DIRECT_READER_SHARDS + reader_shard]
                .0
                .fetch_sub(1, Ordering::SeqCst)
        });
    }

    fn leave_mutator(&self, reservation: DirectMutationReservation) {
        match reservation {
            DirectMutationReservation::Slot(slot) => {
                assert!(
                    self.mutators[slot].0.load(Ordering::Relaxed),
                    "direct mutation slot left twice"
                );
                self.mutators[slot].0.store(false, Ordering::SeqCst);
            }
            DirectMutationReservation::Overflow => {
                let previous = self.mutator_overflow.fetch_sub(1, Ordering::SeqCst);
                assert_ne!(previous, 0, "direct overflow mutation pin left twice");
            }
        }
    }

    /// Queues one exactly removed allocation for epoch reclamation.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this owner and carry the sole retirement
    /// responsibility for an allocation unreachable from every index. One of
    /// this owner's reader or mutation pins must span the exact index mutation
    /// through this queue publication.
    #[allow(
        unsafe_code,
        reason = "accepts one caller-proven exact-owner retirement token"
    )]
    unsafe fn retire(&self, thread_id: usize, handle: RemovedDirectHandle) {
        // A mutator pin prevents more than one rotation through publication
        // and queuing. Reclamation deliberately ages this slot for two
        // rotations, so either side of one racing rotation is safe here.
        let epoch = direct_epoch_slot(self.current_epoch.load(Ordering::SeqCst));
        let shard = thread_id & (DIRECT_RETIRED_SHARDS - 1);
        let mut retired = self.retired[epoch * DIRECT_RETIRED_SHARDS + shard].0.lock();
        retired.push(handle);
        let queued = retired.len();
        drop(retired);
        if queued.is_multiple_of(DIRECT_RECLAIM_PUBLISH_BATCH) {
            let published = self.retired_count[epoch]
                .fetch_add(DIRECT_RECLAIM_PUBLISH_BATCH, Ordering::Relaxed)
                + DIRECT_RECLAIM_PUBLISH_BATCH;
            if published >= DIRECT_RECLAIM_BATCH {
                self.try_advance(false);
            }
        }
    }

    /// Queues exactly removed allocations for epoch reclamation.
    ///
    /// # Safety
    ///
    /// Every handle must belong to this owner and carry the sole retirement
    /// responsibility for an allocation unreachable from every index. One of
    /// this owner's reader or mutation pins must span every exact mutation
    /// through this queue publication.
    #[allow(
        unsafe_code,
        reason = "accepts caller-proven exact-owner retirement tokens"
    )]
    unsafe fn retire_batch(&self, thread_id: usize, handles: &mut Vec<RemovedDirectHandle>) {
        if handles.is_empty() {
            return;
        }
        // See `retire`: the live batch pin bounds this load to either side of
        // at most one rotation, and two-rotation aging covers both slots.
        let epoch = direct_epoch_slot(self.current_epoch.load(Ordering::SeqCst));
        let shard = thread_id & (DIRECT_RETIRED_SHARDS - 1);
        let mut retired = self.retired[epoch * DIRECT_RETIRED_SHARDS + shard].0.lock();
        let published_before = retired.len() / DIRECT_RECLAIM_PUBLISH_BATCH;
        retired.append(handles);
        let published_after = retired.len() / DIRECT_RECLAIM_PUBLISH_BATCH;
        drop(retired);
        let publish = (published_after - published_before) * DIRECT_RECLAIM_PUBLISH_BATCH;
        if publish != 0 {
            let published =
                self.retired_count[epoch].fetch_add(publish, Ordering::Relaxed) + publish;
            if published >= DIRECT_RECLAIM_BATCH {
                self.try_advance(false);
            }
        }
    }

    fn epoch_is_quiescent(&self, epoch: usize) -> bool {
        self.readers[epoch * DIRECT_READER_SHARDS..(epoch + 1) * DIRECT_READER_SHARDS]
            .iter()
            // A zero-delta RMW must observe the immediately preceding counter
            // modification. A load may legally read an older value while a
            // reader registration races this scan, even with SeqCst ordering.
            // This runs only during reclamation, never on the read hot path.
            .all(|counter| counter.0.fetch_add(0, Ordering::SeqCst) == 0)
    }

    fn mutators_are_quiescent(&self) -> bool {
        if self.mutator_overflow.fetch_add(0, Ordering::SeqCst) != 0 {
            return false;
        }
        self.mutators.iter().all(|slot| {
            slot.0
                .compare_exchange(false, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        })
    }

    fn epoch_has_retired(&self, epoch: usize) -> bool {
        if self.retired_count[epoch].load(Ordering::Relaxed) != 0 {
            return true;
        }
        let begin = epoch * DIRECT_RETIRED_SHARDS;
        self.retired[begin..begin + DIRECT_RETIRED_SHARDS]
            .iter()
            .any(|queue| !queue.0.lock().is_empty())
    }

    fn try_advance(&self, force: bool) {
        let observed = direct_epoch_slot(self.current_epoch.load(Ordering::SeqCst));
        if !force && self.retired_count[observed].load(Ordering::Relaxed) < DIRECT_RECLAIM_BATCH {
            return;
        }
        let Some(_reclaim) = self.reclaim_gate.try_lock() else {
            return;
        };
        // A successful standalone removal announces its short publication
        // window here. A racing mutation may overlap one rotation, but the
        // next scan must observe it before a second rotation can proceed.
        if self.mutator_activity.load(Ordering::SeqCst) && !self.mutators_are_quiescent() {
            return;
        }
        let current_generation = self.current_epoch.load(Ordering::SeqCst);
        let current = direct_epoch_slot(current_generation);
        if !force && self.retired_count[current].load(Ordering::Relaxed) < DIRECT_RECLAIM_BATCH {
            return;
        }
        // A rotation may leave current readers behind, but every older slot
        // must first be quiescent. Consequently a mutator pin spanning index
        // publication through retirement prevents a second rotation, while
        // continuous readers in the current slot do not block the first.
        let Some(advance) =
            plan_epoch_advance(current_generation, |epoch| self.epoch_is_quiescent(epoch))
        else {
            return;
        };
        // `next_epoch` was current two rotations ago. Checking every
        // non-current slot above proves that both readers already present at
        // removal and readers admitted during one racing rotation have left.
        if self.epoch_has_retired(advance.next_epoch) {
            self.retired_count[advance.next_epoch].swap(0, Ordering::Relaxed);
            // Reclamation invokes arbitrary `V::drop` code while this thread
            // owns `reclaim_gate`. Mark the exact owner only for that cold
            // interval so a destructor's first same-arena mutation can
            // announce itself without recursively locking the gate.
            let _owner_scope = DirectReclaimOwnerScope::enter(ptr::from_ref(self).addr());
            self.drain_retired_epoch(advance.next_epoch);
        }
        debug_assert_eq!(
            advance.next_epoch,
            direct_epoch_slot(advance.next_generation)
        );
        self.current_epoch
            .compare_exchange(
                current_generation,
                advance.next_generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .unwrap_or_else(|_| std::process::abort());
    }

    #[allow(
        unsafe_code,
        reason = "reclaims only one quiescent owner-controlled retirement epoch"
    )]
    fn drain_retired_epoch(&self, epoch: usize) {
        let begin = epoch * DIRECT_RETIRED_SHARDS;
        let fallback_queue = &self.retired[begin];
        let retired_count = &self.retired_count[epoch];
        let mut arena_handles = DirectRetiredBatch::new(Vec::new(), fallback_queue, retired_count);
        for (shard, retired) in self.retired[begin..begin + DIRECT_RETIRED_SHARDS]
            .iter()
            .enumerate()
        {
            let handles = std::mem::take(&mut *retired.0.lock());
            let mut handles = DirectRetiredBatch::new(handles, retired, retired_count);
            self.reclaim_handles(&mut handles, shard, &mut arena_handles);
        }
        let arena_handles = arena_handles.disarm();
        let arena_pointers = arena_handles
            .iter()
            .map(|handle| handle.published().pointer::<V>())
            .collect::<Vec<_>>();
        // SAFETY: the handles came from this owner's retired queues, the
        // selected epoch is quiescent, and the queue owns each retirement once.
        unsafe {
            self.state
                .reclaim_entries(&arena_pointers, Some((fallback_queue, retired_count)));
        };
    }

    /// Immediately destroys one allocation that was never published.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this owner, remain uniquely owned by the caller,
    /// and never have been reachable by a reader.
    #[allow(
        unsafe_code,
        reason = "destroys one caller-proven unpublished direct allocation"
    )]
    unsafe fn reclaim_immediate(&self, handle: DirectHandle) {
        if handle.is_boxed() {
            // SAFETY: boxed handles originate from Box::into_raw and an
            // unpublished handle has never been visible to another thread.
            unsafe { drop(Box::from_raw(handle.pointer::<V>())) };
        } else {
            // SAFETY: this function's contract proves the arena slot belongs to
            // this owner and was initialized but never published.
            unsafe { self.state.reclaim_entry(handle.pointer::<V>()) };
        }
    }

    /// Releases one never-initialized slot reserved from this owner.
    ///
    /// # Safety
    ///
    /// `pointer` must be a unique reserved slot from this owner that has not
    /// been initialized, published, or released already.
    #[allow(
        unsafe_code,
        reason = "releases one caller-proven uninitialized owner slot"
    )]
    unsafe fn release_uninitialized(&self, pointer: *mut DirectArenaEntry<V>) {
        // SAFETY: this function's contract proves exact-state membership and
        // unique ownership of one still-uninitialized reservation.
        unsafe { self.state.release_pointer(pointer) };
    }

    #[cfg_attr(not(feature = "prepared-keys"), allow(clippy::unused_self))]
    #[allow(
        unsafe_code,
        reason = "destroys owner-controlled boxed handles after epoch quiescence"
    )]
    fn reclaim_handles(
        &self,
        handles: &mut DirectRetiredBatch<'_>,
        retired_shard: usize,
        arena_handles: &mut DirectRetiredBatch<'_>,
    ) {
        #[cfg(not(feature = "prepared-keys"))]
        let _ = retired_shard;
        #[cfg(feature = "prepared-keys")]
        let mut recycled = None;
        while let Some(removed) = handles.pop() {
            let handle = removed.published();
            #[cfg(feature = "prepared-keys")]
            if handle.is_recycled_boxed() {
                let pointer = handle.pointer::<V>();
                let allocation = DirectRecycledBoxGuard { pointer };
                // SAFETY: boxed handles originate from Box::into_raw and the
                // quiescent-state proof gives this reclaimer unique access.
                unsafe { ptr::drop_in_place(pointer) };
                let recycled =
                    recycled.get_or_insert_with(|| self.recycled_boxes()[retired_shard].lock());
                if recycled.len() < DIRECT_RECYCLED_BOX_CAPACITY_PER_SHARD {
                    std::mem::forget(allocation);
                    recycled.push(handle);
                }
                continue;
            }
            if handle.is_boxed() {
                // SAFETY: boxed handles originate from Box::into_raw and the
                // quiescent-state proof gives this reclaimer unique access.
                unsafe { drop(Box::from_raw(handle.pointer::<V>())) };
            } else {
                arena_handles.push(removed);
            }
        }
    }
}

#[allow(
    unsafe_code,
    reason = "deallocates uniquely owned recycled boxes during final drop"
)]
impl<V> Drop for DirectArenaOwner<V> {
    fn drop(&mut self) {
        debug_assert!(
            self.mutators
                .iter()
                .all(|slot| !slot.0.load(Ordering::Relaxed))
                && self.mutator_overflow.load(Ordering::Relaxed) == 0,
            "direct arena owner outlives every mutator"
        );
        debug_assert!(
            self.readers
                .iter()
                .all(|counter| counter.0.load(Ordering::Relaxed) == 0),
            "direct arena owner outlives every reader"
        );
        for epoch in 0..DIRECT_EPOCHS {
            self.drain_retired_epoch(epoch);
        }
        #[cfg(feature = "prepared-keys")]
        if let Some(recycled_boxes) = self.recycled_boxes.get_mut() {
            for recycled in recycled_boxes {
                for handle in recycled.get_mut().drain(..) {
                    // SAFETY: recycled boxes contain no initialized value and
                    // are uniquely owned by this arena during final drop.
                    unsafe {
                        drop(Box::from_raw(
                            handle
                                .pointer::<V>()
                                .cast::<MaybeUninit<DirectArenaEntry<V>>>(),
                        ));
                    }
                }
            }
        }
    }
}

impl<V> DirectArenaState<V> {
    fn new(partitions: usize) -> Self {
        let partitions = partitions
            .clamp(1, DIRECT_MAX_PARTITIONS)
            .next_power_of_two()
            .min(DIRECT_MAX_PARTITIONS);
        Self {
            partitions: std::iter::repeat_with(DirectArenaPartition::new)
                .take(partitions)
                .collect(),
            availability: (0..partitions.div_ceil(u64::BITS as usize))
                .map(|_| AtomicU64::new(0))
                .collect(),
            block_lookup: DirectBlockLookup::new(),
        }
    }

    fn allocate(&self) -> *mut DirectArenaEntry<V> {
        let partition_mask = self.partitions.len() - 1;
        let thread_id = direct_arena_thread();
        let home = thread_id & partition_mask;
        if let Some(pointer) = self.partitions[home].try_allocate_home(home, &self.availability) {
            return pointer;
        }

        // Fresh partial blocks are private to their home partition. Search
        // other partitions only for epoch-reclaimed capacity, avoiding the
        // startup herd where every writer claims the first partial block.
        for step in 0..self.partitions.len() {
            let partition = home.wrapping_add(step + 1) & partition_mask;
            let word = partition / u64::BITS as usize;
            let mask = 1_u64 << (partition % u64::BITS as usize);
            if self.availability[word].load(Ordering::Acquire) & mask != 0
                && let Some(pointer) =
                    self.partitions[partition].try_allocate_reclaimed(partition, &self.availability)
            {
                return pointer;
            }
        }
        self.partitions[home].allocate_new(home, &self.block_lookup)
    }

    fn reserve(&self, pointers: &mut [*mut DirectArenaEntry<V>]) {
        let partition_mask = self.partitions.len() - 1;
        let home = direct_arena_thread() & partition_mask;
        self.partitions[home].reserve_home(home, &self.block_lookup, &self.availability, pointers);
    }

    /// Destroys and releases a batch of quiescent arena entries.
    ///
    /// # Safety
    ///
    /// Every pointer must identify a distinct initialized slot owned by this
    /// state, and epoch quiescence must exclude all concurrent access.
    #[allow(
        unsafe_code,
        reason = "destroys caller-proven quiescent direct-arena entries"
    )]
    unsafe fn reclaim_entries(
        &self,
        pointers: &[*mut DirectArenaEntry<V>],
        requeue: Option<(&DirectRetiredQueue, &AtomicUsize)>,
    ) {
        if pointers.is_empty() {
            return;
        }
        let entry_bytes = std::mem::size_of::<DirectArenaEntry<V>>();
        let block_bytes = entry_bytes * DIRECT_BLOCK_SIZE;
        let mut grouped = (0..self.partitions.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<(usize, usize)>>>();
        let mut cached_block = None;
        for pointer in pointers {
            let address = pointer.expose_provenance();
            let (start, partition) = cached_block
                .filter(|(start, _)| address >= *start && address - *start < block_bytes)
                .unwrap_or_else(|| {
                    let located = self.block_lookup.locate(address, block_bytes);
                    cached_block = Some(located);
                    located
                });
            let distance = address - start;
            assert_eq!(distance % entry_bytes, 0, "arena pointer is entry-aligned");
            grouped[partition].push((start, distance / entry_bytes));
        }

        // `release_entries` already needs block/offset order. Establish that
        // order before running any destructor so a duplicated retirement
        // fails closed instead of dropping one value twice and discovering the
        // duplicate only while releasing its arena slot.
        for entries in &mut grouped {
            entries.sort_unstable();
            assert!(
                entries.windows(2).all(|pair| pair[0] != pair[1]),
                "direct arena entry retired more than once"
            );
        }

        let mut release_on_unwind = DirectUnwindReleaseGuard {
            state: self,
            pointers,
            completed: 0,
            requeue,
        };
        for pointer in pointers {
            // Include the current slot before invoking user code. If its
            // destructor unwinds, the guard releases this slot only after the
            // destructor's own cleanup has completed.
            release_on_unwind.completed += 1;
            // SAFETY: quiescence proves no reader can retain this initialized
            // entry, and duplicate validation above proves every retired
            // pointer appears exactly once.
            unsafe { ptr::drop_in_place(*pointer) };
        }
        // All user destructors completed. Restore the original batched release
        // path without running the exceptional one-at-a-time cleanup.
        std::mem::forget(release_on_unwind);
        for (partition, entries) in grouped.into_iter().enumerate() {
            if !entries.is_empty() {
                self.partitions[partition].release_entries(
                    partition,
                    &entries,
                    &self.block_lookup,
                    &self.availability,
                );
            }
        }
    }

    #[cold]
    /// Destroys and releases one initialized but unpublished arena entry.
    ///
    /// # Safety
    ///
    /// `pointer` must identify a uniquely owned initialized slot from this
    /// state that was never published and has not already been reclaimed.
    #[allow(
        unsafe_code,
        reason = "destroys one caller-proven unpublished direct-arena entry"
    )]
    unsafe fn reclaim_entry(&self, pointer: *mut DirectArenaEntry<V>) {
        let entry_bytes = std::mem::size_of::<DirectArenaEntry<V>>();
        let block_bytes = entry_bytes * DIRECT_BLOCK_SIZE;
        let address = pointer.expose_provenance();
        let (start, partition) = self.block_lookup.locate(address, block_bytes);
        let distance = address - start;
        assert_eq!(distance % entry_bytes, 0, "arena pointer is entry-aligned");
        let release_on_unwind = DirectSingleReleaseGuard {
            state: self,
            partition,
            block_start: start,
            offset: distance / entry_bytes,
        };
        // SAFETY: unpublished entries have never been visible through the
        // index, so this caller has unique access to the initialized value. The
        // release guard clears ownership only after this destructor finishes.
        unsafe { ptr::drop_in_place(pointer) };
        std::mem::forget(release_on_unwind);
        self.partitions[partition].release_entry(
            partition,
            start,
            distance / entry_bytes,
            &self.block_lookup,
            &self.availability,
        );
    }

    #[cold]
    /// Releases one never-initialized arena slot.
    ///
    /// # Safety
    ///
    /// `pointer` must identify a unique occupied slot from this state whose
    /// storage is uninitialized and has not already been released.
    #[allow(
        unsafe_code,
        reason = "releases one caller-proven uninitialized direct-arena slot"
    )]
    unsafe fn release_pointer(&self, pointer: *mut DirectArenaEntry<V>) {
        let entry_bytes = std::mem::size_of::<DirectArenaEntry<V>>();
        let block_bytes = entry_bytes * DIRECT_BLOCK_SIZE;
        let address = pointer.expose_provenance();
        let (start, partition) = self.block_lookup.locate(address, block_bytes);
        let distance = address - start;
        assert_eq!(distance % entry_bytes, 0, "arena pointer is entry-aligned");
        self.partitions[partition].release_entry(
            partition,
            start,
            distance / entry_bytes,
            &self.block_lookup,
            &self.availability,
        );
    }
}

fn direct_arena_thread() -> usize {
    DIRECT_ARENA_THREAD.with(|thread| {
        let current = thread.get();
        if current != usize::MAX {
            return current;
        }
        let assigned = NEXT_DIRECT_ARENA_THREAD.fetch_add(1, Ordering::Relaxed);
        thread.set(assigned);
        assigned
    })
}

struct ArenaBlock<V> {
    entries: Box<[AtomicPtr<ArenaEntry<V>>]>,
    generations: Box<[AtomicU32]>,
}

struct ArenaPage<V> {
    blocks: [OnceLock<Box<ArenaBlock<V>>>; PAGE_SIZE],
}

impl<V> ArenaPage<V> {
    fn new() -> Self {
        Self {
            blocks: std::array::from_fn(|_| OnceLock::new()),
        }
    }
}

struct ArenaDirectory<V> {
    pages: [OnceLock<Box<ArenaPage<V>>>; DIRECTORY_SIZE],
}

impl<V> ArenaDirectory<V> {
    fn new() -> Self {
        Self {
            pages: std::array::from_fn(|_| OnceLock::new()),
        }
    }

    fn block(&self, block: usize) -> Option<&ArenaBlock<V>> {
        let page = self.pages.get(block >> PAGE_BITS)?.get()?;
        page.blocks.get(block & PAGE_MASK)?.get().map(Box::as_ref)
    }

    fn ensure_block(&self, block: usize) -> &ArenaBlock<V> {
        let page = self.pages[block >> PAGE_BITS].get_or_init(|| Box::new(ArenaPage::new()));
        page.blocks[block & PAGE_MASK].get_or_init(|| Box::new(ArenaBlock::new()))
    }
}

impl<V> ArenaBlock<V> {
    fn new() -> Self {
        Self {
            entries: std::iter::repeat_with(|| AtomicPtr::new(ptr::null_mut()))
                .take(BLOCK_SIZE)
                .collect(),
            generations: std::iter::repeat_with(|| AtomicU32::new(1))
                .take(BLOCK_SIZE)
                .collect(),
        }
    }
}

#[derive(Default)]
struct PartitionState {
    free: Vec<u32>,
}

#[repr(align(64))]
struct ArenaPartition<V> {
    directory: OnceLock<Box<ArenaDirectory<V>>>,
    state: Mutex<PartitionState>,
    next_slot: AtomicU32,
    free_available: AtomicBool,
    resident_entries: AtomicUsize,
    resident_weight: AtomicU64,
}

impl<V> ArenaPartition<V> {
    fn new() -> Self {
        Self {
            directory: OnceLock::new(),
            state: Mutex::new(PartitionState::default()),
            next_slot: AtomicU32::new(0),
            free_available: AtomicBool::new(false),
            resident_entries: AtomicUsize::new(0),
            resident_weight: AtomicU64::new(0),
        }
    }

    fn block(&self, slot: u32) -> Option<&ArenaBlock<V>> {
        self.directory.get()?.block((slot as usize) >> BLOCK_BITS)
    }

    fn slot(&self, slot: u32) -> Option<(&AtomicPtr<ArenaEntry<V>>, &AtomicU32)> {
        let block = self.block(slot)?;
        let offset = slot as usize & BLOCK_MASK;
        Some((block.entries.get(offset)?, block.generations.get(offset)?))
    }

    fn ensure_slot(&self, slot: u32) -> (&AtomicPtr<ArenaEntry<V>>, &AtomicU32) {
        let slot = slot as usize;
        let directory = self
            .directory
            .get_or_init(|| Box::new(ArenaDirectory::new()));
        let block = directory.ensure_block(slot >> BLOCK_BITS);
        let offset = slot & BLOCK_MASK;
        (&block.entries[offset], &block.generations[offset])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArenaAllocationError {
    Full,
}

pub(crate) struct ValueArena<V> {
    partitions: Box<[ArenaPartition<V>]>,
    active_partitions: AtomicUsize,
    collector: Arc<Collector>,
}

impl<V> ValueArena<V> {
    pub(crate) fn new(partitions: usize) -> Self {
        let partitions = partitions.clamp(1, 256).next_power_of_two().min(256);
        Self {
            partitions: std::iter::repeat_with(ArenaPartition::new)
                .take(partitions)
                .collect(),
            active_partitions: AtomicUsize::new(1),
            collector: Arc::new(Collector::new().batch_size(512)),
        }
    }

    pub(crate) fn pin(&self) -> ArenaPin<'_, V> {
        ArenaPin {
            arena: self,
            guard: self.collector.enter(),
            blocks: std::array::from_fn(|_| std::cell::Cell::new(CachedBlock::EMPTY)),
        }
    }

    #[cfg(test)]
    pub(crate) fn allocate(
        &self,
        value: V,
        weight: u32,
        expires_at: u64,
        partition_hint: u64,
    ) -> Result<ArenaHandle, ArenaAllocationError> {
        self.allocate_entry(ArenaEntry::new(value, weight, expires_at), partition_hint)
    }

    #[allow(deprecated)] // `try_update` is newer than the crate's Rust 1.88 MSRV.
    pub(crate) fn allocate_entry(
        &self,
        entry: Box<ArenaEntry<V>>,
        partition_hint: u64,
    ) -> Result<ArenaHandle, ArenaAllocationError> {
        let active_partitions = self.active_partitions.load(Ordering::Relaxed);
        let partition_mask =
            u64::try_from(active_partitions - 1).expect("partition count is at most 256");
        let shard = usize::try_from(partition_hint & partition_mask)
            .expect("masked partition index is at most 255");
        let partition = &self.partitions[shard];
        let reused = if partition.free_available.load(Ordering::Acquire) {
            let mut state = partition.state.lock();
            let slot = state.free.pop();
            if state.free.is_empty() {
                partition.free_available.store(false, Ordering::Release);
            }
            slot
        } else {
            None
        };
        let slot_index = if let Some(slot) = reused {
            slot
        } else {
            partition
                .next_slot
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |slot| {
                    (u64::from(slot) <= SLOT_MASK).then_some(slot + 1)
                })
                .map_err(|_| ArenaAllocationError::Full)?
        };

        let block_index = (slot_index as usize) >> BLOCK_BITS;
        if block_index > 0 && active_partitions < self.partitions.len() {
            let _ = self.active_partitions.compare_exchange(
                active_partitions,
                (active_partitions * 2).min(self.partitions.len()),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        let (slot, generation) = partition.ensure_slot(slot_index);
        debug_assert!(slot.load(Ordering::Relaxed).is_null());
        let generation_value = generation.load(Ordering::Relaxed) & MAX_GENERATION;
        let weight = entry.weight();
        slot.store(Box::into_raw(entry), Ordering::Release);
        generation.store(generation_value, Ordering::Release);
        partition.resident_entries.fetch_add(1, Ordering::Relaxed);
        partition
            .resident_weight
            .fetch_add(weight, Ordering::Relaxed);
        Ok(ArenaHandle::new(shard, slot_index, generation_value))
    }

    #[allow(
        unsafe_code,
        reason = "constructs a value only after collector protection and generation revalidation"
    )]
    pub(crate) fn get(&self, handle: ArenaHandle) -> Option<ArenaValue<ArenaEntry<V>>> {
        let partition = self.partitions.get(handle.shard())?;
        let (slot, generation) = partition.slot(handle.slot())?;
        let before = generation.load(Ordering::Acquire) & MAX_GENERATION;
        if before != handle.generation() {
            return None;
        }
        let guard = self.collector.enter();
        let pointer = guard.protect(slot, Ordering::Acquire);
        let after = generation.load(Ordering::Acquire) & MAX_GENERATION;
        if pointer.is_null() || before != after {
            return None;
        }
        // SAFETY: `guard` came from this collector and protected the non-null
        // slot pointer across the matching generation validation above.
        Some(unsafe { ArenaValue::protected(&self.collector, guard, pointer) })
    }

    /// Removes a published allocation and returns ownership protected by the
    /// collector.
    ///
    /// # Safety
    ///
    /// The caller must either hold the authoritative key's mutation stripe, or
    /// prove that `handle` has never been published and cannot be replaced or
    /// removed concurrently.
    #[allow(
        unsafe_code,
        reason = "removes and retires one caller-proven exclusively mutable slot"
    )]
    pub(crate) unsafe fn remove(&self, handle: ArenaHandle) -> Option<ArenaValue<ArenaEntry<V>>> {
        let partition = self.partitions.get(handle.shard())?;
        let mut state = partition.state.lock();
        let (slot, generation) = partition.slot(handle.slot())?;
        if generation.load(Ordering::Acquire) & MAX_GENERATION != handle.generation() {
            return None;
        }
        let guard = self.collector.enter();
        let pointer = guard.swap(slot, ptr::null_mut(), Ordering::AcqRel);
        if pointer.is_null() {
            return None;
        }
        // SAFETY: the successful collector-protected swap returns the unique
        // non-null pointer that was stored in this arena slot.
        let weight = unsafe { &*pointer }.weight();
        generation.store(next_generation(handle.generation()), Ordering::Release);
        state.free.push(handle.slot());
        partition.free_available.store(true, Ordering::Release);
        partition.resident_entries.fetch_sub(1, Ordering::Relaxed);
        partition
            .resident_weight
            .fetch_sub(weight, Ordering::Relaxed);
        // SAFETY: this exact collector guard swapped the allocation out of its
        // authoritative slot. The generation advanced before the slot can be
        // reused, and this function's mutation contract excludes a second
        // remover for the same publication.
        Some(unsafe { ArenaValue::retired(&self.collector, guard, pointer) })
    }

    /// Replaces the allocation identified by `handle`.
    ///
    /// # Safety
    ///
    /// The caller must hold the cache's same-key mutation stripe for the key
    /// whose authoritative index value produced `handle`. The stripe must stay
    /// held until the returned old value is no longer used to publish cache
    /// state.
    #[allow(
        unsafe_code,
        reason = "replaces and retires one caller-proven exclusively mutable slot"
    )]
    pub(crate) unsafe fn replace(
        &self,
        handle: ArenaHandle,
        replacement: Box<ArenaEntry<V>>,
    ) -> Result<ArenaValue<ArenaEntry<V>>, Box<ArenaEntry<V>>> {
        let Some(partition) = self.partitions.get(handle.shard()) else {
            return Err(replacement);
        };
        let Some((slot, generation)) = partition.slot(handle.slot()) else {
            return Err(replacement);
        };
        if generation.load(Ordering::Acquire) & MAX_GENERATION != handle.generation() {
            return Err(replacement);
        }
        let guard = self.collector.enter();
        let current = guard.protect(slot, Ordering::Acquire);
        if current.is_null() {
            return Err(replacement);
        }
        // SAFETY: `guard.protect` keeps this non-null slot value alive until
        // the replacement has been published and the old value retired.
        let previous_weight = unsafe { &*current }.weight();
        let replacement_weight = replacement.weight();
        let previous = guard.swap(slot, Box::into_raw(replacement), Ordering::AcqRel);
        generation.fetch_or(ACCESSED_BIT, Ordering::Relaxed);
        debug_assert_eq!(previous, current);
        adjust_weight(
            &partition.resident_weight,
            previous_weight,
            replacement_weight,
        );
        // SAFETY: the mutation-stripe contract makes `previous` the exact live
        // allocation protected above and the swap made it unreachable. This
        // guard belongs to the same collector and retirement occurs once.
        Ok(unsafe { ArenaValue::retired(&self.collector, guard, previous) })
    }

    /// Replaces and retires the allocation identified by `handle`.
    ///
    /// # Safety
    ///
    /// The caller must hold the cache's same-key mutation stripe for the key
    /// whose authoritative index value produced `handle` until replacement is
    /// complete.
    #[allow(
        unsafe_code,
        reason = "replaces and retires one caller-proven exclusively mutable slot"
    )]
    pub(crate) unsafe fn replace_discard(
        &self,
        handle: ArenaHandle,
        replacement: Box<ArenaEntry<V>>,
    ) -> Result<(), Box<ArenaEntry<V>>> {
        let Some(partition) = self.partitions.get(handle.shard()) else {
            return Err(replacement);
        };
        let Some((slot, generation)) = partition.slot(handle.slot()) else {
            return Err(replacement);
        };
        if generation.load(Ordering::Acquire) & MAX_GENERATION != handle.generation() {
            return Err(replacement);
        }
        let current = slot.load(Ordering::Acquire);
        if current.is_null() {
            return Err(replacement);
        }
        // SAFETY: cache-level same-key mutation stripes prevent removal or a
        // second replacement while current remains published in this slot.
        let previous_weight = unsafe { &*current }.weight();
        let replacement_weight = replacement.weight();
        let previous = slot.swap(Box::into_raw(replacement), Ordering::AcqRel);
        generation.fetch_or(ACCESSED_BIT, Ordering::Relaxed);
        adjust_weight(
            &partition.resident_weight,
            previous_weight,
            replacement_weight,
        );
        // SAFETY: swap made previous unreachable to future loads; it came from
        // Box::into_raw and this thread does not access it after retirement.
        unsafe { self.collector.retire(previous, reclaim::boxed) };
        Ok(())
    }

    /// Removes and retires an allocation without returning its value.
    ///
    /// # Safety
    ///
    /// The caller must either hold the authoritative key's mutation stripe, or
    /// prove that `handle` has never been published and cannot be replaced or
    /// removed concurrently.
    #[allow(
        unsafe_code,
        reason = "removes and retires one caller-proven exclusively mutable slot"
    )]
    pub(crate) unsafe fn remove_discard(&self, handle: ArenaHandle) -> bool {
        let Some(partition) = self.partitions.get(handle.shard()) else {
            return false;
        };
        let mut state = partition.state.lock();
        let Some((slot, generation)) = partition.slot(handle.slot()) else {
            return false;
        };
        if generation.load(Ordering::Acquire) & MAX_GENERATION != handle.generation() {
            return false;
        }
        let pointer = slot.load(Ordering::Acquire);
        if pointer.is_null() {
            return false;
        }
        // SAFETY: the caller's same-key stripe (or unpublished ownership)
        // keeps this pointer stable until the following swap. The partition
        // lock alone serializes slot reuse, not published replacement.
        let weight = unsafe { &*pointer }.weight();
        let removed = slot.swap(ptr::null_mut(), Ordering::AcqRel);
        generation.store(next_generation(handle.generation()), Ordering::Release);
        state.free.push(handle.slot());
        partition.free_available.store(true, Ordering::Release);
        partition.resident_entries.fetch_sub(1, Ordering::Relaxed);
        partition
            .resident_weight
            .fetch_sub(weight, Ordering::Relaxed);
        // SAFETY: removed is no longer reachable and came from Box::into_raw.
        unsafe { self.collector.retire(removed, reclaim::boxed) };
        true
    }

    pub(crate) fn mark_accessed(&self, handle: ArenaHandle) {
        let Some(partition) = self.partitions.get(handle.shard()) else {
            return;
        };
        let Some((_, generation)) = partition.slot(handle.slot()) else {
            return;
        };
        let current = generation.load(Ordering::Relaxed);
        if current & MAX_GENERATION == handle.generation() {
            generation.fetch_or(ACCESSED_BIT, Ordering::Relaxed);
        }
    }

    pub(crate) fn take_accessed(&self, handle: ArenaHandle) -> bool {
        let Some(partition) = self.partitions.get(handle.shard()) else {
            return false;
        };
        let Some((_, generation)) = partition.slot(handle.slot()) else {
            return false;
        };
        let current = generation.load(Ordering::Relaxed);
        if current & MAX_GENERATION != handle.generation() {
            return false;
        }
        generation.fetch_and(MAX_GENERATION, Ordering::Relaxed) & ACCESSED_BIT != 0
    }

    /// Updates the expiry field of the allocation identified by `handle`.
    ///
    /// # Safety
    ///
    /// The caller must hold the cache's same-key mutation stripe for the key
    /// whose authoritative index value produced `handle` until the update and
    /// any decision based on the returned liveness state are complete.
    #[allow(
        unsafe_code,
        reason = "updates one caller-proven exclusively mutable live allocation"
    )]
    pub(crate) unsafe fn update_expiry(
        &self,
        handle: ArenaHandle,
        now: u64,
        expires_at: u64,
    ) -> Option<bool> {
        let partition = self.partitions.get(handle.shard())?;
        let (slot, generation) = partition.slot(handle.slot())?;
        if generation.load(Ordering::Acquire) & MAX_GENERATION != handle.generation() {
            return None;
        }
        let pointer = slot.load(Ordering::Acquire);
        if pointer.is_null() {
            return None;
        }
        // SAFETY: cache-level same-key mutation stripes prevent replacement or
        // removal while this pointer is inspected and its expiry is updated.
        let entry = unsafe { &*pointer };
        let current = entry.expires_at();
        if current != u64::MAX && current <= now {
            return Some(false);
        }
        entry.set_expires_at(expires_at);
        generation.fetch_or(ACCESSED_BIT, Ordering::Relaxed);
        Some(true)
    }

    pub(crate) fn len(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.resident_entries.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn weight(&self) -> u64 {
        self.partitions
            .iter()
            .map(|partition| partition.resident_weight.load(Ordering::Relaxed))
            .sum()
    }
}

#[allow(
    unsafe_code,
    reason = "retires remaining uniquely owned arena pointers on final drop"
)]
impl<V> Drop for ValueArena<V> {
    fn drop(&mut self) {
        for partition in &self.partitions {
            if let Some(directory) = partition.directory.get() {
                for page in directory.pages.iter().filter_map(OnceLock::get) {
                    for block in page.blocks.iter().filter_map(OnceLock::get) {
                        for slot in &block.entries {
                            let pointer = slot.swap(ptr::null_mut(), Ordering::AcqRel);
                            if !pointer.is_null() {
                                // SAFETY: the pointer has just become unreachable to
                                // future arena loads and originated from Box::into_raw.
                                unsafe { self.collector.retire(pointer, reclaim::boxed) };
                            }
                        }
                    }
                }
            }
        }
    }
}

fn encode_expiry(expires_at: u64) -> u32 {
    if expires_at == u64::MAX {
        u32::MAX
    } else {
        u32::try_from(expires_at.min(MAX_EXPIRY_TICK)).expect("expiry tick is capped to u32")
    }
}

fn encode_direct_expiry(expires_at: u64) -> u32 {
    if expires_at == u64::MAX {
        DIRECT_NEVER_EXPIRES
    } else {
        u32::try_from(expires_at.min(DIRECT_MAX_EXPIRY_TICK))
            .expect("direct expiry tick is capped to 31 bits")
    }
}

const fn next_generation(generation: u32) -> u32 {
    if generation == MAX_GENERATION {
        1
    } else {
        generation + 1
    }
}

fn adjust_weight(total: &AtomicU64, previous: u64, replacement: u64) {
    if replacement >= previous {
        total.fetch_add(replacement - previous, Ordering::Relaxed);
    } else {
        total.fetch_sub(previous - replacement, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test proves one unpublished handle belongs to this exact arena pin"
    )]
    fn protected_direct_handle_is_zero_cost_and_keeps_the_pin_lifetime() {
        assert_eq!(
            std::mem::size_of::<ProtectedDirectHandle<'static, u64>>(),
            std::mem::size_of::<DirectHandle>()
        );
        assert_eq!(
            std::mem::align_of::<ProtectedDirectHandle<'static, u64>>(),
            std::mem::align_of::<DirectHandle>()
        );

        let arena = DirectValueArena::new(1);
        let handle = arena.allocate(41_u64, 7, u64::MAX);
        let pin = arena.pin();
        // SAFETY: `handle` was allocated by `arena`, has not been published or
        // retired, and `pin` protects that exact arena.
        let protected = unsafe { pin.protect(handle) };
        assert_eq!(*protected.entry().value(), 41);
        assert_eq!(protected.entry().weight(), 7);
        drop(pin);
        // SAFETY: the handle belongs to `arena` and was never published.
        unsafe { arena.drop_unpublished(handle) };
    }

    #[test]
    fn direct_entry_keeps_metadata_beside_the_value_prefix() {
        assert_eq!(std::mem::size_of::<DirectArenaEntry<[u8; 64]>>(), 72);
        assert_eq!(
            std::mem::offset_of!(DirectArenaEntry<[u8; 64]>, metadata),
            0
        );
        assert_eq!(std::mem::offset_of!(DirectArenaEntry<[u8; 64]>, value), 8);
    }

    #[test]
    fn direct_entry_access_bit_round_trips() {
        let entry = DirectArenaEntry::new(7_u64, 8, u64::MAX);
        assert!(!entry.take_accessed());
        entry.mark_accessed();
        entry.mark_accessed();
        assert!(entry.take_accessed());
        assert!(!entry.take_accessed());
    }

    #[test]
    fn concurrent_access_mark_and_expiry_update_preserve_both_metadata_fields() {
        let entry = Arc::new(DirectArenaEntry::new(7_u64, 8, u64::MAX));
        let start = Arc::new(std::sync::Barrier::new(9));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let entry = Arc::clone(&entry);
            let start = Arc::clone(&start);
            workers.push(std::thread::spawn(move || {
                start.wait();
                entry.mark_accessed();
            }));
        }
        start.wait();
        entry.set_expires_at(42);
        for worker in workers {
            worker.join().expect("access marker must not panic");
        }

        assert_eq!(entry.expires_at(), 42);
        assert!(entry.take_accessed());
        assert_eq!(entry.weight(), 8);
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test initializes and reclaims one uniquely allocated arena slot"
    )]
    fn stale_direct_availability_hint_cannot_skip_or_corrupt_allocation() {
        let state = DirectArenaState::<u64>::new(u64::BITS as usize);
        let home = direct_arena_thread() & (state.partitions.len() - 1);
        let stale_partition = home.wrapping_add(1) & (state.partitions.len() - 1);
        let stale_bit = 1_u64 << stale_partition;
        state.availability[0].store(stale_bit, Ordering::Relaxed);

        let pointer = state.allocate();
        // SAFETY: `allocate` returned one unique unpublished slot.
        unsafe { pointer.write(DirectArenaEntry::new(41, 7, u64::MAX)) };

        assert_eq!(state.availability[0].load(Ordering::Relaxed) & stale_bit, 0);
        // SAFETY: the initialized pointer is unique, unpublished, and belongs to `state`.
        unsafe { state.reclaim_entry(pointer) };
    }

    #[test]
    fn direct_epoch_token_exhaustion_never_wraps() {
        assert_eq!(next_direct_epoch_token(0), Some(1));
        assert_eq!(next_direct_epoch_token(1), Some(2));
        assert_eq!(next_direct_epoch_token(2), Some(4));
        assert_eq!(next_direct_epoch_token(usize::MAX - 1), None);
    }

    #[test]
    fn direct_reader_counter_reserves_half_the_integer_space() {
        let counter = AtomicUsize::new(DIRECT_MAX_READERS - 1);
        acquire_direct_reader(&counter);
        assert_eq!(counter.load(Ordering::Relaxed), DIRECT_MAX_READERS);
    }

    #[test]
    fn direct_mutation_slots_fall_back_and_release_exactly() {
        let owner = DirectArenaOwner::<u64>::new(1);
        owner.activate_mutators();
        let reservations = (0..=DIRECT_READER_SHARDS)
            .map(|_| owner.enter_mutator(0))
            .collect::<Vec<_>>();
        assert!(matches!(
            reservations.last(),
            Some(DirectMutationReservation::Overflow)
        ));
        assert!(!owner.mutators_are_quiescent());
        for reservation in reservations {
            owner.leave_mutator(reservation);
        }
        assert!(owner.mutators_are_quiescent());
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test transfers one exclusively owned unreachable handle into retirement"
    )]
    fn retired_value_drop_can_activate_first_mutation_on_the_same_arena() {
        struct ReentrantDrop {
            owner: std::sync::Weak<DirectArenaOwner<ReentrantDrop>>,
        }

        impl Drop for ReentrantDrop {
            fn drop(&mut self) {
                let Some(owner) = self.owner.upgrade() else {
                    return;
                };
                owner.activate_mutators();
                let reservation = owner.enter_mutator(0);
                owner.leave_mutator(reservation);
            }
        }

        let arena = DirectValueArena::new(1);
        let handle = arena.allocate(
            ReentrantDrop {
                owner: Arc::downgrade(&arena.owner),
            },
            1,
            u64::MAX,
        );
        let pin = arena.pin();
        // SAFETY: this test exclusively owns the unpublished handle, treats
        // that unique ownership as the exact transition to unreachable, and
        // keeps this arena's pin active through retirement publication.
        unsafe {
            pin.retire(RemovedDirectHandle::from_exact_removal(handle));
        }
        drop(pin);

        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            arena.reclaim_retired();
            finished_tx.send(()).expect("test receiver remains live");
        });
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("reentrant value destruction must not deadlock the collector gate");
        worker.join().expect("reclamation worker must not panic");
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test protects one unpublished handle from this exact arena"
    )]
    fn partial_direct_reservation_releases_only_unused_slots() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let arena = DirectValueArena::new(1);
        let handle = {
            let mut reservation = arena.reservation();
            reservation.allocate(CountDrop(Arc::clone(&drops)), 1, u64::MAX)
        };
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        let pin = arena.pin();
        // SAFETY: the reservation and pin belong to this exact arena, and the
        // unpublished handle remains live until after the assertion.
        assert_eq!(unsafe { pin.protect(handle) }.entry().weight(), 1);
        drop(pin);
        // SAFETY: the reservation handle belongs to `arena` and was never published.
        unsafe { arena.drop_unpublished(handle) };
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        for _ in 0..DIRECT_RESERVATION_SIZE {
            let handle = arena.allocate(CountDrop(Arc::clone(&drops)), 1, u64::MAX);
            // SAFETY: each handle belongs to `arena` and was never published.
            unsafe { arena.drop_unpublished(handle) };
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1 + DIRECT_RESERVATION_SIZE);
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test reclaims one unpublished handle from its exact arena"
    )]
    fn duplicate_arena_retirement_fails_before_running_a_destructor() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let arena = DirectValueArena::new(1);
        let handle = arena.allocate(CountDrop(Arc::clone(&drops)), 1, u64::MAX);
        let pointer = handle.pointer::<CountDrop>();

        let duplicate = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // SAFETY: both pointers name the same live unpublished slot from
            // this state intentionally; the test verifies duplicate validation
            // rejects them before either value is destroyed.
            unsafe { arena.owner.state.reclaim_entries(&[pointer, pointer], None) };
        }));
        assert!(duplicate.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        // SAFETY: the handle belongs to `arena` and was never published.
        unsafe { arena.drop_unpublished(handle) };
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test reclaims one unpublished handle from its exact arena"
    )]
    fn panicking_unpublished_drop_releases_direct_slot_once() {
        struct PanicOnFirstDrop<'a>(&'a AtomicUsize);

        impl Drop for PanicOnFirstDrop<'_> {
            fn drop(&mut self) {
                assert_ne!(
                    self.0.fetch_add(1, Ordering::Relaxed),
                    0,
                    "first value drop"
                );
            }
        }

        let drops = AtomicUsize::new(0);
        let arena = DirectValueArena::new(1);
        let handle = arena.allocate(PanicOnFirstDrop(&drops), 1, u64::MAX);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // SAFETY: the handle belongs to `arena` and was never published.
            unsafe { arena.drop_unpublished(handle) };
        }));
        assert!(result.is_err());
        drop(arena);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test directly exercises quiescent batch reclamation"
    )]
    fn panicking_batch_reclamation_releases_processed_direct_slots_once() {
        struct PanicOnFirstDrop<'a>(&'a AtomicUsize);

        impl Drop for PanicOnFirstDrop<'_> {
            fn drop(&mut self) {
                assert_ne!(
                    self.0.fetch_add(1, Ordering::Relaxed),
                    0,
                    "first value drop"
                );
            }
        }

        let drops = AtomicUsize::new(0);
        let arena = DirectValueArena::new(1);
        let handles = (0..3)
            .map(|_| arena.allocate(PanicOnFirstDrop(&drops), 1, u64::MAX))
            .collect::<Vec<_>>();
        let pointers = handles
            .iter()
            .map(|handle| handle.pointer::<PanicOnFirstDrop<'_>>())
            .collect::<Vec<_>>();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // SAFETY: every pointer is a distinct initialized unpublished slot
            // from this exact arena, with no concurrent reader or owner.
            unsafe { arena.owner.state.reclaim_entries(&pointers, None) };
        }));
        assert!(result.is_err());
        drop(arena);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test directly queues exact boxed retirements in one quiescent epoch"
    )]
    fn panicking_boxed_reclamation_preserves_mixed_unprocessed_batch() {
        struct PanicOnce {
            drops: Arc<AtomicUsize>,
            panicked: Arc<AtomicBool>,
        }

        impl Drop for PanicOnce {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
                assert!(
                    self.panicked.swap(true, Ordering::Relaxed),
                    "first boxed value drop"
                );
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let panicked = Arc::new(AtomicBool::new(false));
        let arena = DirectValueArena::new(1);
        let make_value = || PanicOnce {
            drops: Arc::clone(&drops),
            panicked: Arc::clone(&panicked),
        };
        let handles = [
            arena.allocate_boxed(make_value(), 1, u64::MAX),
            arena.allocate_boxed(make_value(), 1, u64::MAX),
            arena.allocate(make_value(), 1, u64::MAX),
        ]
        .into_iter()
        .map(|handle| {
            // SAFETY: every allocation is distinct, belongs to this arena,
            // and the test gives this queue its sole retirement.
            unsafe { RemovedDirectHandle::from_exact_removal(handle) }
        })
        .collect::<Vec<_>>();
        arena.owner.retired[0].0.lock().extend(handles);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            arena.owner.drain_retired_epoch(0);
        }));
        assert!(result.is_err());
        assert_eq!(
            arena.owner.retired[0].0.lock().len(),
            2,
            "unprocessed boxed retirements must remain owner-reachable"
        );

        arena.owner.drain_retired_epoch(0);
        assert!(arena.owner.retired[0].0.lock().is_empty());
        drop(arena);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test directly queues exact arena retirements in one quiescent epoch"
    )]
    fn panicking_arena_reclamation_requeues_entries_not_yet_destroyed() {
        struct PanicOnce {
            drops: Arc<AtomicUsize>,
            panicked: Arc<AtomicBool>,
        }

        impl Drop for PanicOnce {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
                assert!(
                    self.panicked.swap(true, Ordering::Relaxed),
                    "first arena value drop"
                );
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let panicked = Arc::new(AtomicBool::new(false));
        let arena = DirectValueArena::new(1);
        let handles = (0..3)
            .map(|_| {
                arena.allocate(
                    PanicOnce {
                        drops: Arc::clone(&drops),
                        panicked: Arc::clone(&panicked),
                    },
                    1,
                    u64::MAX,
                )
            })
            .map(|handle| {
                // SAFETY: every allocation is distinct, belongs to this arena,
                // and the test gives this queue its sole retirement.
                unsafe { RemovedDirectHandle::from_exact_removal(handle) }
            })
            .collect::<Vec<_>>();
        arena.owner.retired[0].0.lock().extend(handles);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            arena.owner.drain_retired_epoch(0);
        }));
        assert!(result.is_err());
        assert_eq!(
            arena.owner.retired[0].0.lock().len(),
            2,
            "arena entries whose destructor did not start must be requeued"
        );

        arena.owner.drain_retired_epoch(0);
        assert!(arena.owner.retired[0].0.lock().is_empty());
        drop(arena);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn concurrent_direct_owner_lifecycles_do_not_reuse_live_registry_keys() {
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        drop(DirectArenaOwner::<u64>::new(1));
                    }
                });
            }
        });
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "single-threaded test exclusively owns the unpublished arena handle"
    )]
    fn stale_handle_cannot_read_reused_slot() {
        let arena = ValueArena::new(1);
        let first = arena.allocate("first", 5, u64::MAX, 0).unwrap();
        let pin = arena.pin();
        assert_eq!(pin.get(first).unwrap().value(), &"first");
        assert_eq!(arena.get(first).unwrap().value(), &"first");
        // SAFETY: this test exclusively owns the arena and `first`; no removal
        // or replacement can race it.
        let removed = unsafe { arena.remove(first) }.unwrap();
        assert_eq!(removed.value(), &"first");
        let second = arena.allocate("second", 6, u64::MAX, 0).unwrap();
        assert_eq!(first.slot(), second.slot());
        assert_ne!(first.generation(), second.generation());
        assert!(pin.get(first).is_none());
        assert_eq!(pin.get(second).unwrap().value(), &"second");
        assert!(arena.get(first).is_none());
        assert_eq!(arena.get(second).unwrap().value(), &"second");
        assert_eq!(removed.value(), &"first");
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.weight(), 6);
    }
}
