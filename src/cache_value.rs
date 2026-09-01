//! One-allocation epoch-protected cache values.
//!
//! Each allocation contains its access/expiry word, byte length, and immutable
//! trailing value. This avoids the separate arena slot plus `Box<[u8]>` used by
//! a sized value while preserving arbitrary binary lengths.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use seize::{Collector, Guard, LocalGuard};

use crate::NonMaxU64;

const ACCESSED_BIT: u64 = 1_u64 << 63;
const EXPIRY_MASK: u64 = !ACCESSED_BIT;
const ENCODED_NEVER_EXPIRES: u64 = EXPIRY_MASK;
const MAX_POOL_CLASSES: usize = 16;
const MAX_POOLED_LAYOUT_SIZE: usize = 4 * 1_024;
const MAX_POOLED_BYTES: usize = 256 * 1_024;
const SHARED_TRANSFER_BLOCKS: usize = 128;
const MAX_SHARED_POOL_CLASSES: usize = 64;
const MAX_SHARED_POOLED_BYTES: usize = 512 * 1_024;
pub(crate) const MAX_CACHE_VALUE_BYTES: usize = u32::MAX as usize;
static CACHE_VALUE_POOL_ACTIVE: AtomicBool = AtomicBool::new(false);
static SHARED_CACHE_VALUE_POOL_ACTIVE: AtomicBool = AtomicBool::new(false);
static SHARED_CACHE_VALUE_POOL: OnceLock<Mutex<SharedCacheValuePool>> = OnceLock::new();

#[repr(C)]
struct CacheValueHeader {
    metadata: AtomicU64,
}

const VALUE_LEN_OFFSET: usize = size_of::<CacheValueHeader>();
const VALUE_OFFSET: usize = VALUE_LEN_OFFSET + size_of::<u32>();

struct CacheValueFreeClass {
    layout_size: usize,
    head: *mut CacheValueHeader,
    blocks: usize,
}

struct CacheValueBatch {
    layout_size: usize,
    head: *mut CacheValueHeader,
    tail: *mut CacheValueHeader,
    blocks: usize,
}

#[derive(Default)]
struct CacheValuePool {
    classes: Vec<CacheValueFreeClass>,
    retained_bytes: usize,
}

#[derive(Default)]
struct SharedCacheValuePool {
    classes: Vec<CacheValueFreeClass>,
    retained_bytes: usize,
}

// SAFETY: pointers in either pool refer to unreachable, uninitialized
// allocations. Thread-local pools never cross threads; the shared pool is
// accessed only while its mutex is held.
#[allow(
    unsafe_code,
    reason = "free-list nodes cross threads only under the shared-pool mutex"
)]
// SAFETY: the raw pointers refer only to unreachable free blocks, and any
// cross-thread access is serialized by the shared-pool mutex.
unsafe impl Send for CacheValueFreeClass {}

thread_local! {
    static CACHE_VALUE_POOL: RefCell<CacheValuePool> = RefCell::new(CacheValuePool::default());
}

#[derive(Clone, Copy)]
pub(crate) struct CacheValueHandle(NonMaxU64);

/// A decoded cache-value handle whose tag proves that it represents a live
/// arena allocation rather than an immediate tombstone.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct LiveCacheValueHandle(CacheValueHandle);

/// Unique reclamation ownership for a live cache value that has been made
/// unreachable from every segment index.
#[repr(transparent)]
pub(crate) struct RemovedCacheValueHandle(LiveCacheValueHandle);

/// A live value handle proven to belong to the collector protected for
/// `'pin`.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct ProtectedCacheValueHandle<'pin> {
    handle: LiveCacheValueHandle,
    marker: PhantomData<&'pin CacheValueHeader>,
}

const _: () =
    assert!(std::mem::size_of::<LiveCacheValueHandle>() == std::mem::size_of::<CacheValueHandle>());
const _: () = assert!(
    std::mem::align_of::<LiveCacheValueHandle>() == std::mem::align_of::<CacheValueHandle>()
);
const _: () = assert!(
    std::mem::size_of::<RemovedCacheValueHandle>() == std::mem::size_of::<CacheValueHandle>()
);
const _: () = assert!(
    std::mem::align_of::<RemovedCacheValueHandle>() == std::mem::align_of::<CacheValueHandle>()
);

impl<'pin> ProtectedCacheValueHandle<'pin> {
    #[inline]
    pub(crate) fn value(self) -> CacheValueRef<'pin> {
        CacheValueRef {
            pointer: self.handle.pointer(),
            marker: PhantomData,
        }
    }
}

pub(crate) struct CacheValueArena {
    collector: Collector,
}

pub(crate) struct CacheValuePin<'arena> {
    _arena: &'arena CacheValueArena,
    guard: LocalGuard<'arena>,
}

pub(crate) struct CacheValueRef<'pin> {
    pointer: NonNull<CacheValueHeader>,
    marker: PhantomData<&'pin CacheValueHeader>,
}

impl CacheValueHandle {
    /// Reconstructs an encoded handle loaded unchanged from a segment-cache
    /// index or packed override slot.
    ///
    /// # Safety
    ///
    /// `value` must have been published from a live arena handle or one of the
    /// segment cache's validated immediate encodings.
    #[allow(unsafe_code, reason = "declares the raw segment-index handle boundary")]
    pub(crate) const unsafe fn from_index_value(value: NonMaxU64) -> Self {
        Self(value)
    }

    /// Constructs a non-pointer immediate handle. Immediate tags must reserve
    /// at least one low alignment bit so they can never decode as live.
    #[inline]
    pub(crate) const fn from_immediate_value(value: NonMaxU64) -> Self {
        assert!(
            value.get() & 0b111 != 0,
            "immediate cache handles must carry a nonzero low tag"
        );
        Self(value)
    }

    pub(crate) const fn index_value(self) -> NonMaxU64 {
        self.0
    }

    /// Decodes a live pointer handle. Immediate encodings use low tag bits,
    /// while cache allocations are eight-byte aligned.
    #[inline]
    #[allow(
        clippy::verbose_bit_mask,
        reason = "the explicit tag mask is the cache's pointer-decoding protocol"
    )]
    pub(crate) const fn live(self) -> Option<LiveCacheValueHandle> {
        if self.0.get() & 0b111 == 0 {
            Some(LiveCacheValueHandle(self))
        } else {
            None
        }
    }

    fn from_pointer(pointer: NonNull<CacheValueHeader>) -> LiveCacheValueHandle {
        debug_assert_eq!(pointer.as_ptr().expose_provenance() & 0b111, 0);
        let raw = u64::try_from(pointer.as_ptr().expose_provenance()).expect("pointer fits u64");
        LiveCacheValueHandle(Self(
            NonMaxU64::new(raw).expect("cache allocation is not the reserved handle"),
        ))
    }
}

impl LiveCacheValueHandle {
    #[inline]
    pub(crate) const fn encoded(self) -> CacheValueHandle {
        self.0
    }

    fn pointer(self) -> NonNull<CacheValueHeader> {
        let address =
            usize::try_from(self.0.index_value().get()).expect("pointer handle fits usize");
        NonNull::new(std::ptr::with_exposed_provenance_mut(address))
            .expect("cache value handle is non-null")
    }
}

impl RemovedCacheValueHandle {
    /// Claims the sole retirement responsibility transferred by an exact
    /// index replacement/removal or by exclusive arena teardown.
    ///
    /// # Safety
    ///
    /// No other retirement token may exist for the same live publication.
    #[inline]
    #[allow(
        unsafe_code,
        reason = "declares the unique cache-value retirement transition"
    )]
    pub(crate) const unsafe fn from_exact_removal(handle: LiveCacheValueHandle) -> Self {
        Self(handle)
    }

    #[inline]
    const fn into_live(self) -> LiveCacheValueHandle {
        self.0
    }
}

impl CacheValueArena {
    pub(crate) fn new() -> Self {
        Self {
            collector: Collector::new().batch_size(512),
        }
    }

    #[allow(
        unsafe_code,
        clippy::unused_self,
        clippy::cast_ptr_alignment,
        reason = "allocates and initializes one value with trailing bytes"
    )]
    pub(crate) fn allocate(&self, value: &[u8], expires_at: u64) -> LiveCacheValueHandle {
        let (layout, value_offset) = value_layout(value.len());
        debug_assert!(value.len() <= MAX_CACHE_VALUE_BYTES);
        let value_len = u32::try_from(value.len()).expect("validated cache value length fits u32");
        // SAFETY: the header makes this layout non-empty. Allocation failure
        // follows the standard global allocator path.
        let raw = if let Some(pointer) = take_pooled(layout) {
            pointer.cast()
        } else {
            // SAFETY: `layout` is non-zero and valid; null is handled below
            // with the standard allocation-error path.
            unsafe { alloc(layout) }
        };
        let Some(pointer) = NonNull::new(raw.cast::<CacheValueHeader>()) else {
            handle_alloc_error(layout);
        };
        let encoded_expiry = if expires_at == u64::MAX {
            ENCODED_NEVER_EXPIRES
        } else {
            expires_at.min(ENCODED_NEVER_EXPIRES - 1)
        };
        // SAFETY: the allocation satisfies the header layout and includes the
        // complete trailing byte region. Neither initialized region overlaps.
        unsafe {
            pointer.as_ptr().write(CacheValueHeader {
                metadata: AtomicU64::new(encoded_expiry),
            });
            raw.add(VALUE_LEN_OFFSET)
                .cast::<u32>()
                .write_unaligned(value_len);
            ptr::copy_nonoverlapping(value.as_ptr(), raw.add(value_offset), value.len());
        }
        CacheValueHandle::from_pointer(pointer)
    }

    pub(crate) fn pin(&self) -> CacheValuePin<'_> {
        CacheValuePin {
            _arena: self,
            guard: self.collector.enter(),
        }
    }
}

impl CacheValuePin<'_> {
    /// Binds a live value handle to this pin's collector guard.
    ///
    /// # Safety
    ///
    /// `handle` must have been loaded unchanged from an index protected by
    /// this exact arena collector, or name an unpublished allocation owned by
    /// this arena. It must remain live for the guard's lifetime.
    #[inline]
    #[allow(
        unsafe_code,
        clippy::unused_self,
        reason = "declares the same-arena protected-handle lifetime boundary"
    )]
    pub(crate) unsafe fn protect(
        &self,
        handle: LiveCacheValueHandle,
    ) -> ProtectedCacheValueHandle<'_> {
        ProtectedCacheValueHandle {
            handle,
            marker: PhantomData,
        }
    }

    /// Queues an exactly removed value for epoch reclamation.
    ///
    /// # Safety
    ///
    /// `handle` must belong to this pin's exact arena collector, be
    /// unreachable from every index, and carry the sole retirement
    /// responsibility for its allocation.
    #[allow(
        unsafe_code,
        reason = "retires one exact removed allocation through its epoch collector"
    )]
    pub(crate) unsafe fn retire(&self, handle: RemovedCacheValueHandle) {
        let handle = handle.into_live();
        // SAFETY: the caller has made this exact handle unreachable from its
        // index while this guard protects any preceding observation.
        unsafe {
            self.guard
                .defer_retire(handle.pointer().as_ptr(), reclaim_cache_value);
        };
    }
}

impl<'pin> CacheValueRef<'pin> {
    #[allow(
        unsafe_code,
        reason = "borrows collector-protected trailing value bytes"
    )]
    pub(crate) fn value(&self) -> &'pin [u8] {
        // SAFETY: the pin's guard prevents reclamation and allocation stores
        // the immutable trailing bytes before publishing the handle.
        unsafe {
            let value_len = self
                .pointer
                .as_ptr()
                .cast::<u8>()
                .add(VALUE_LEN_OFFSET)
                .cast::<u32>()
                .read_unaligned() as usize;
            slice::from_raw_parts(
                self.pointer.as_ptr().cast::<u8>().add(VALUE_OFFSET),
                value_len,
            )
        }
    }

    #[allow(
        unsafe_code,
        reason = "borrows one collector-protected cache-value header"
    )]
    pub(crate) fn expires_at(&self) -> u64 {
        // The index publishes the immutable expiry. This relaxed load is only
        // independent access-state observation after that acquire publication.
        // SAFETY: a `CacheValueRef` is created only from a collector-protected
        // non-null arena pointer, which remains live for this borrow.
        let expiry = unsafe { self.pointer.as_ref() }
            .metadata
            .load(Ordering::Relaxed)
            & EXPIRY_MASK;
        if expiry == ENCODED_NEVER_EXPIRES {
            u64::MAX
        } else {
            expiry
        }
    }

    pub(crate) fn is_live(&self, now_tick: u64) -> bool {
        let expires_at = self.expires_at();
        expires_at == u64::MAX || expires_at > now_tick
    }

    #[allow(
        unsafe_code,
        reason = "updates one collector-protected cache-value header"
    )]
    pub(crate) fn mark_accessed(&self) {
        // SAFETY: the collector guard backing this reference keeps the header
        // allocated, and metadata is independently atomic.
        let metadata = &unsafe { self.pointer.as_ref() }.metadata;
        if metadata.load(Ordering::Relaxed) & ACCESSED_BIT == 0 {
            metadata.fetch_or(ACCESSED_BIT, Ordering::Relaxed);
        }
    }
}

fn value_layout(value_len: usize) -> (Layout, usize) {
    let size = VALUE_OFFSET
        .checked_add(value_len)
        .expect("cache value layout fits address space");
    let layout = Layout::from_size_align(size, align_of::<CacheValueHeader>())
        .expect("cache value layout fits address space");
    (layout, VALUE_OFFSET)
}

impl CacheValuePool {
    #[allow(
        unsafe_code,
        reason = "transfers a valid shared batch into the exclusive local pool"
    )]
    fn take(&mut self, layout: Layout) -> Option<*mut CacheValueHeader> {
        if let Some(pointer) = self.take_local(layout) {
            return Some(pointer);
        }
        if !SHARED_CACHE_VALUE_POOL_ACTIVE.load(Ordering::Relaxed) {
            return None;
        }
        let batch = take_shared_batch(layout.size())?;
        // SAFETY: `take_shared_batch` transfers exclusive ownership of a valid
        // same-layout detached list. A rejection returns that ownership intact.
        if let Err(batch) = unsafe { self.absorb(batch) } {
            // SAFETY: the rejected batch is unchanged and remains exclusively
            // owned by this call.
            unsafe { return_batch_to_shared(batch) };
            return None;
        }
        self.take_local(layout)
    }

    #[allow(
        unsafe_code,
        reason = "detaches one block from an exclusively owned intrusive list"
    )]
    fn take_local(&mut self, layout: Layout) -> Option<*mut CacheValueHeader> {
        let class = self
            .classes
            .iter()
            .position(|class| class.layout_size == layout.size())?;
        let head = self.classes[class].head;
        debug_assert!(!head.is_null());
        // SAFETY: every pooled block stores the previous free-list head in its
        // first pointer-sized word and has the requested layout.
        let next = unsafe { head.cast::<*mut CacheValueHeader>().read() };
        self.retained_bytes = self.retained_bytes.saturating_sub(layout.size());
        self.classes[class].blocks -= 1;
        if self.classes[class].blocks == 0 {
            debug_assert!(next.is_null());
            self.classes.swap_remove(class);
        } else {
            debug_assert!(!next.is_null());
            self.classes[class].head = next;
        }
        Some(head)
    }

    /// Adds one quiescent, uninitialized allocation to this local pool.
    ///
    /// # Safety
    ///
    /// `pointer` must be uniquely owned, allocated with `layout`, large and
    /// aligned enough for the intrusive link, and unused after this transfer.
    #[allow(
        unsafe_code,
        reason = "links one caller-proven free allocation into the local pool"
    )]
    unsafe fn put(&mut self, pointer: *mut CacheValueHeader, layout: Layout) -> bool {
        if layout.size() > MAX_POOLED_LAYOUT_SIZE {
            return false;
        }
        while self.retained_bytes.saturating_add(layout.size()) > MAX_POOLED_BYTES {
            let Some(batch) = self.detach_batch(SHARED_TRANSFER_BLOCKS) else {
                return false;
            };
            // SAFETY: `detach_batch` transfers one valid, exclusively owned
            // linked batch out of this local pool.
            unsafe { return_batch_to_shared(batch) };
        }
        let class = self
            .classes
            .iter()
            .position(|class| class.layout_size == layout.size());
        let class = if let Some(class) = class {
            class
        } else {
            if self.classes.len() == MAX_POOL_CLASSES {
                return false;
            }
            self.classes.push(CacheValueFreeClass {
                layout_size: layout.size(),
                head: ptr::null_mut(),
                blocks: 0,
            });
            self.classes.len() - 1
        };
        let previous = self.classes[class].head;
        // SAFETY: the retired allocation is no longer initialized or visible,
        // and every pooled layout is large and aligned enough for a pointer.
        unsafe { pointer.cast::<*mut CacheValueHeader>().write(previous) };
        self.classes[class].head = pointer;
        self.classes[class].blocks += 1;
        self.retained_bytes += layout.size();
        true
    }

    /// Transfers a valid detached batch into this local pool.
    ///
    /// # Safety
    ///
    /// `batch` must exclusively own the linked blocks described by its layout
    /// and count. No duplicate ownership of any block may exist.
    #[allow(
        unsafe_code,
        reason = "links one caller-proven detached batch into the local pool"
    )]
    unsafe fn absorb(&mut self, batch: CacheValueBatch) -> Result<(), CacheValueBatch> {
        let batch_bytes = batch.layout_size * batch.blocks;
        if self.retained_bytes.saturating_add(batch_bytes) > MAX_POOLED_BYTES {
            return Err(batch);
        }
        let class = self
            .classes
            .iter()
            .position(|class| class.layout_size == batch.layout_size);
        let class = if let Some(class) = class {
            class
        } else {
            if self.classes.len() == MAX_POOL_CLASSES {
                return Err(batch);
            }
            self.classes.push(CacheValueFreeClass {
                layout_size: batch.layout_size,
                head: ptr::null_mut(),
                blocks: 0,
            });
            self.classes.len() - 1
        };
        // SAFETY: the batch tail is a free block and its intrusive link may be
        // changed while both lists are exclusively owned by this thread.
        unsafe {
            batch
                .tail
                .cast::<*mut CacheValueHeader>()
                .write(self.classes[class].head);
        }
        self.classes[class].head = batch.head;
        self.classes[class].blocks += batch.blocks;
        self.retained_bytes += batch_bytes;
        Ok(())
    }

    #[allow(
        unsafe_code,
        reason = "detaches an ownership batch from one exclusive local free list"
    )]
    fn detach_batch(&mut self, maximum: usize) -> Option<CacheValueBatch> {
        let class = self
            .classes
            .iter()
            .enumerate()
            .max_by_key(|(_, class)| class.blocks)
            .map(|(index, _)| index)?;
        let blocks = maximum.min(self.classes[class].blocks);
        // SAFETY: class accounting proves `head` owns at least `blocks` linked
        // allocations, all with the recorded layout.
        let batch = unsafe {
            detach_free_batch(
                self.classes[class].layout_size,
                &mut self.classes[class].head,
                blocks,
            )
        };
        self.classes[class].blocks -= blocks;
        self.retained_bytes -= batch.layout_size * blocks;
        if self.classes[class].blocks == 0 {
            debug_assert!(self.classes[class].head.is_null());
            self.classes.swap_remove(class);
        }
        Some(batch)
    }
}

impl SharedCacheValuePool {
    #[allow(
        unsafe_code,
        reason = "detaches an ownership batch while holding the shared-pool mutex"
    )]
    fn take(&mut self, layout_size: usize) -> Option<CacheValueBatch> {
        let class = self
            .classes
            .iter()
            .position(|class| class.layout_size == layout_size)?;
        let blocks = SHARED_TRANSFER_BLOCKS
            .min(MAX_POOLED_BYTES / layout_size)
            .min(self.classes[class].blocks);
        // SAFETY: class accounting proves `head` owns at least `blocks` linked
        // allocations, all with `layout_size`.
        let batch =
            unsafe { detach_free_batch(layout_size, &mut self.classes[class].head, blocks) };
        self.classes[class].blocks -= blocks;
        self.retained_bytes -= layout_size * blocks;
        if self.classes[class].blocks == 0 {
            debug_assert!(self.classes[class].head.is_null());
            self.classes.swap_remove(class);
        }
        Some(batch)
    }

    /// Transfers a valid detached batch into the shared pool.
    ///
    /// # Safety
    ///
    /// `batch` must exclusively own the linked blocks described by its layout
    /// and count. No duplicate ownership of any block may exist.
    #[allow(
        unsafe_code,
        reason = "links one caller-proven batch while holding the shared-pool mutex"
    )]
    unsafe fn put(&mut self, batch: CacheValueBatch) -> Result<(), CacheValueBatch> {
        let batch_bytes = batch.layout_size * batch.blocks;
        if self.retained_bytes.saturating_add(batch_bytes) > MAX_SHARED_POOLED_BYTES {
            return Err(batch);
        }
        let class = self
            .classes
            .iter()
            .position(|class| class.layout_size == batch.layout_size);
        let class = if let Some(class) = class {
            class
        } else {
            if self.classes.len() == MAX_SHARED_POOL_CLASSES {
                return Err(batch);
            }
            self.classes.push(CacheValueFreeClass {
                layout_size: batch.layout_size,
                head: ptr::null_mut(),
                blocks: 0,
            });
            self.classes.len() - 1
        };
        // SAFETY: the mutex exclusively owns both uninitialized free lists.
        unsafe {
            batch
                .tail
                .cast::<*mut CacheValueHeader>()
                .write(self.classes[class].head);
        }
        self.classes[class].head = batch.head;
        self.classes[class].blocks += batch.blocks;
        self.retained_bytes += batch_bytes;
        Ok(())
    }
}

#[allow(
    unsafe_code,
    reason = "splits an exclusively owned intrusive free list"
)]
/// Detaches exclusive ownership of the first `blocks` nodes from a free list.
///
/// # Safety
///
/// `head` must own a valid intrusive list containing at least `blocks` nodes,
/// all allocated with `layout_size`; `blocks` must be nonzero.
unsafe fn detach_free_batch(
    layout_size: usize,
    head: &mut *mut CacheValueHeader,
    blocks: usize,
) -> CacheValueBatch {
    debug_assert!(blocks > 0);
    debug_assert!(!head.is_null());
    let batch_head = *head;
    let mut tail = batch_head;
    for _ in 1..blocks {
        // SAFETY: class accounting guarantees this many linked free blocks.
        tail = unsafe { tail.cast::<*mut CacheValueHeader>().read() };
        debug_assert!(!tail.is_null());
    }
    // SAFETY: the tail is an uninitialized free block whose intrusive link
    // currently points to the remaining class list.
    let remaining = unsafe { tail.cast::<*mut CacheValueHeader>().read() };
    // SAFETY: the same exclusively owned free block can have its intrusive
    // next pointer detached in place.
    unsafe { tail.cast::<*mut CacheValueHeader>().write(ptr::null_mut()) };
    *head = remaining;
    CacheValueBatch {
        layout_size,
        head: batch_head,
        tail,
        blocks,
    }
}

fn shared_cache_value_pool() -> &'static Mutex<SharedCacheValuePool> {
    SHARED_CACHE_VALUE_POOL.get_or_init(|| Mutex::new(SharedCacheValuePool::default()))
}

fn take_shared_batch(layout_size: usize) -> Option<CacheValueBatch> {
    let mut pool = shared_cache_value_pool().lock();
    let batch = pool.take(layout_size);
    SHARED_CACHE_VALUE_POOL_ACTIVE.store(pool.retained_bytes != 0, Ordering::Relaxed);
    batch
}

#[allow(
    unsafe_code,
    reason = "transfers or deallocates one exclusively owned free-list batch"
)]
unsafe fn return_batch_to_shared(batch: CacheValueBatch) {
    let mut pool = shared_cache_value_pool().lock();
    // SAFETY: this function requires a valid exclusively owned batch, and the
    // mutex serializes the shared pool receiving that ownership.
    let rejected = unsafe { pool.put(batch) }.err();
    SHARED_CACHE_VALUE_POOL_ACTIVE.store(pool.retained_bytes != 0, Ordering::Relaxed);
    drop(pool);
    if let Some(batch) = rejected {
        // SAFETY: rejection returned the valid batch unchanged with exclusive
        // ownership still held here.
        unsafe { deallocate_batch(batch) };
    }
}

#[allow(
    unsafe_code,
    clippy::needless_pass_by_value,
    reason = "deallocates an exclusively owned detached free-list batch"
)]
/// Deallocates every node in one detached free-list batch.
///
/// # Safety
///
/// `batch` must exclusively own exactly `blocks` valid linked allocations, all
/// created with its recorded layout size.
unsafe fn deallocate_batch(batch: CacheValueBatch) {
    let layout = Layout::from_size_align(batch.layout_size, align_of::<CacheValueHeader>())
        .expect("a pooled layout remains valid");
    let mut pointer = batch.head;
    for _ in 0..batch.blocks {
        debug_assert!(!pointer.is_null());
        // SAFETY: detached batches exclusively own all of their free blocks.
        let next = unsafe { pointer.cast::<*mut CacheValueHeader>().read() };
        // SAFETY: this block was allocated with `layout`, is uniquely owned by
        // the detached batch, and is not used after deallocation.
        unsafe { dealloc(pointer.cast(), layout) };
        pointer = next;
    }
    debug_assert!(pointer.is_null());
}

#[allow(
    unsafe_code,
    reason = "deallocates every exclusively owned local free-list block"
)]
impl Drop for CacheValuePool {
    fn drop(&mut self) {
        for class in &self.classes {
            let layout = Layout::from_size_align(class.layout_size, align_of::<CacheValueHeader>())
                .expect("a pooled layout remains valid");
            let mut pointer = class.head;
            while !pointer.is_null() {
                // SAFETY: the intrusive link is initialized while the block is
                // pooled, and every block in this list has `layout`.
                let next = unsafe { pointer.cast::<*mut CacheValueHeader>().read() };
                // SAFETY: pool destruction uniquely owns this block and uses
                // the exact layout with which it was allocated.
                unsafe { dealloc(pointer.cast(), layout) };
                pointer = next;
            }
        }
    }
}

fn take_pooled(layout: Layout) -> Option<*mut CacheValueHeader> {
    if !CACHE_VALUE_POOL_ACTIVE.load(Ordering::Relaxed) {
        return None;
    }
    CACHE_VALUE_POOL.with_borrow_mut(|pool| pool.take(layout))
}

/// Transfers one quiescent allocation into its exact-layout local pool.
///
/// # Safety
///
/// `pointer` must be uniquely owned, no longer initialized or reachable, and
/// allocated with `layout`.
#[allow(
    unsafe_code,
    reason = "transfers one caller-proven quiescent allocation into its pool"
)]
unsafe fn return_to_pool(pointer: *mut CacheValueHeader, layout: Layout) -> bool {
    let pooled = CACHE_VALUE_POOL
        .try_with(|pool| {
            pool.try_borrow_mut().is_ok_and(|mut pool| {
                // SAFETY: this function's contract supplies the exact pointer,
                // layout, quiescence, and exclusive-ownership requirements.
                unsafe { pool.put(pointer, layout) }
            })
        })
        .unwrap_or(false);
    if pooled {
        CACHE_VALUE_POOL_ACTIVE.store(true, Ordering::Relaxed);
    }
    pooled
}

#[allow(
    unsafe_code,
    reason = "collector callback drops or pools one quiescent allocation"
)]
unsafe fn reclaim_cache_value(pointer: *mut CacheValueHeader, _collector: &Collector) {
    // SAFETY: seize invokes this only after every reader has left its epoch.
    // The inline length reconstructs the exact allocation layout.
    let value_len = unsafe {
        pointer
            .cast::<u8>()
            .add(VALUE_LEN_OFFSET)
            .cast::<u32>()
            .read_unaligned() as usize
    };
    let (layout, _) = value_layout(value_len);
    // SAFETY: epoch quiescence gives unique access to the initialized header;
    // `value_len` reconstructs its exact allocation layout. After dropping the
    // header, the allocation is either transferred to the pool or deallocated.
    unsafe {
        ptr::drop_in_place(pointer);
        if !return_to_pool(pointer, layout) {
            dealloc(pointer.cast(), layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{alloc, handle_alloc_error};
    use std::ptr;
    use std::sync::Arc;
    use std::thread;

    use super::{
        CacheValueArena, CacheValueBatch, CacheValueHandle, CacheValuePool, LiveCacheValueHandle,
        MAX_POOLED_BYTES, ProtectedCacheValueHandle, RemovedCacheValueHandle, deallocate_batch,
        value_layout,
    };
    use crate::NonMaxU64;

    #[allow(
        unsafe_code,
        reason = "tests transfer sole ownership of unpublished arena values"
    )]
    unsafe fn removed_unpublished(handle: LiveCacheValueHandle) -> RemovedCacheValueHandle {
        // SAFETY: each test-created handle is unpublished and converted once.
        unsafe { RemovedCacheValueHandle::from_exact_removal(handle) }
    }

    #[allow(
        unsafe_code,
        reason = "tests dereference unpublished handles from the exact arena protected by the pin"
    )]
    fn protected_unpublished<'pin>(
        pin: &'pin super::CacheValuePin<'_>,
        handle: LiveCacheValueHandle,
    ) -> super::CacheValueRef<'pin> {
        // SAFETY: callers pass handles allocated by the arena that created
        // `pin`, before publishing or retiring them.
        unsafe { pin.protect(handle) }.value()
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test proves one unpublished handle belongs to this exact arena pin"
    )]
    fn protected_cache_value_handle_is_zero_cost_and_keeps_the_pin_lifetime() {
        assert_eq!(
            std::mem::size_of::<ProtectedCacheValueHandle<'static>>(),
            std::mem::size_of::<LiveCacheValueHandle>()
        );
        assert_eq!(
            std::mem::align_of::<ProtectedCacheValueHandle<'static>>(),
            std::mem::align_of::<LiveCacheValueHandle>()
        );

        let arena = CacheValueArena::new();
        let handle = arena.allocate(b"protected", u64::MAX);
        let pin = arena.pin();
        // SAFETY: `handle` was allocated by `arena`, has not been published or
        // retired, and `pin` protects that exact arena.
        let protected = unsafe { pin.protect(handle) };
        assert_eq!(protected.value().value(), b"protected");
        // SAFETY: `handle` is the unique unpublished allocation from `arena`.
        unsafe { pin.retire(removed_unpublished(handle)) };
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "the test retires unpublished values through their exact arena pin"
    )]
    fn round_trips_binary_values_and_expiry() {
        let arena = CacheValueArena::new();
        for len in [0, 1, 7, 64, 129, 4097] {
            let value = (0..len)
                .map(|index| u8::try_from(index % 251).expect("byte remainder fits"))
                .collect::<Vec<_>>();
            let handle = arena.allocate(&value, 42);
            let pin = arena.pin();
            let entry = protected_unpublished(&pin, handle);
            assert_eq!(entry.value(), value);
            assert!(entry.is_live(41));
            assert!(!entry.is_live(42));
            entry.mark_accessed();
            // SAFETY: `handle` is the unique unpublished allocation from `arena`.
            unsafe { pin.retire(removed_unpublished(handle)) };
        }
    }

    #[test]
    fn immediate_handles_cannot_decode_as_live_allocations() {
        let tombstone = CacheValueHandle::from_immediate_value(
            NonMaxU64::new(2).expect("tombstone encoding is not reserved"),
        );

        assert!(tombstone.live().is_none());
    }

    #[test]
    #[allow(
        unsafe_code,
        clippy::cast_ptr_alignment,
        reason = "constructs one valid detached free-list batch to test rejected ownership transfer"
    )]
    fn rejected_local_batch_returns_its_unique_ownership_token() {
        let (layout, _) = value_layout(32);
        // SAFETY: `layout` is nonzero and valid; allocation failure follows the
        // standard global-allocator path.
        let pointer = unsafe { alloc(layout) }.cast::<super::CacheValueHeader>();
        if pointer.is_null() {
            handle_alloc_error(layout);
        }
        // SAFETY: the allocation is exclusively owned and large/aligned enough
        // for the intrusive next link used by a detached free block.
        unsafe {
            pointer
                .cast::<*mut super::CacheValueHeader>()
                .write(ptr::null_mut());
        };
        let batch = CacheValueBatch {
            layout_size: layout.size(),
            head: pointer,
            tail: pointer,
            blocks: 1,
        };
        let mut full = CacheValuePool {
            classes: Vec::new(),
            retained_bytes: MAX_POOLED_BYTES,
        };

        // SAFETY: `batch` uniquely owns one correctly linked block allocated
        // with its recorded layout. Capacity forces rejection before mutation.
        let rejected = match unsafe { full.absorb(batch) } {
            Ok(()) => panic!("a full local pool must reject the detached batch"),
            Err(batch) => batch,
        };
        assert_eq!(rejected.head, pointer);
        assert_eq!(rejected.tail, pointer);
        assert_eq!(rejected.blocks, 1);
        assert_eq!(rejected.layout_size, layout.size());

        // SAFETY: rejection returned the unchanged uniquely owned valid batch.
        unsafe { deallocate_batch(rejected) };
    }

    #[test]
    #[allow(
        unsafe_code,
        reason = "workers retire unpublished values through their exact arena pin"
    )]
    fn concurrent_retirement_and_reuse_preserve_values() {
        const THREADS: usize = 4;
        const ROUNDS: usize = 4;
        const VALUES: usize = 4_096;

        let arena = Arc::new(CacheValueArena::new());
        thread::scope(|scope| {
            for worker in 0..THREADS {
                let arena = Arc::clone(&arena);
                scope.spawn(move || {
                    for round in 0..ROUNDS {
                        let byte = u8::try_from(worker * ROUNDS + round).expect("test byte fits");
                        let pin = arena.pin();
                        let bytes = [byte; 127];
                        let handles = (0..VALUES)
                            .map(|index| {
                                let len = [8, 16, 32, 64, 127][index % 5];
                                arena.allocate(&bytes[..len], u64::MAX)
                            })
                            .collect::<Vec<_>>();
                        for handle in handles {
                            let value = protected_unpublished(&pin, handle);
                            assert!(value.value().iter().all(|candidate| *candidate == byte));
                            // SAFETY: every handle is uniquely allocated from
                            // `arena` and retired once through its pin.
                            unsafe { pin.retire(removed_unpublished(handle)) };
                        }
                        drop(pin);
                        drop(arena.pin());
                    }
                });
            }
        });
    }
}
