use core::marker::PhantomData;
use core::mem::{self, MaybeUninit};
use core::ptr;

use allocator_api2::alloc::{Allocator, Layout};

use super::config::{CACHE_LINE, GROUP_SIZE};
use super::control::{CTRL_EMPTY, CTRL_TOMBSTONE, ControlByte};
use super::error::TryReserveError;
use super::simd;

/// Owns one allocation backing a map's ctrl bytes + slot data.
/// No `Drop` impl — derived maps orchestrate teardown by calling
/// [`ArenaSlots::drop_values`] on each descriptor before [`Arena::deallocate`].
pub(crate) struct Arena {
    ptr: ptr::NonNull<u8>,
    layout: Layout,
}

#[allow(
    unsafe_code,
    reason = "owns and deallocates one raw allocator-backed extent"
)]
impl Arena {
    /// Sentinel placeholder for moved-from / zero-capacity maps.
    /// Layout size is 0 so `deallocate` is a no-op.
    #[inline]
    pub(crate) const fn empty() -> Self {
        Self {
            ptr: ptr::NonNull::dangling(),
            layout: Layout::new::<[u8; 0]>(),
        }
    }

    /// Allocates uninit memory, zeroing only the first `ctrl_bytes`. Slots
    /// past that are written-then-read, so skipping their memset cuts
    /// setup work + cache pollution. Size-0 layouts return dangling.
    pub(crate) fn try_allocate_with_ctrl_zeroed<A: Allocator>(
        layout: Layout,
        ctrl_bytes: usize,
        alloc: &A,
    ) -> Result<Self, TryReserveError> {
        if ctrl_bytes > layout.size() {
            return Err(TryReserveError::CapacityOverflow);
        }
        if layout.size() == 0 {
            return Ok(Self::empty());
        }
        let ptr = alloc
            .allocate(layout)
            .map_err(|_| TryReserveError::AllocError)?
            .cast::<u8>();
        if ctrl_bytes > 0 {
            // SAFETY: the checked layout covers `ctrl_bytes`, and the successful
            // allocation is writable for the complete layout.
            unsafe { ptr::write_bytes(ptr.as_ptr(), 0, ctrl_bytes) };
        }
        Ok(Self { ptr, layout })
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Size of the already-stored allocator layout backing this arena.
    #[inline]
    pub(crate) fn layout_size(&self) -> usize {
        self.layout.size()
    }

    /// Frees the backing allocation.
    ///
    /// # Safety
    ///
    /// `alloc` must be the allocator that created this arena, and the caller
    /// must already have run every destructor for values living inside it.
    pub(crate) unsafe fn deallocate<A: Allocator>(self, alloc: &A) {
        if self.layout.size() == 0 {
            return;
        }
        // SAFETY: `self.ptr` was allocated by `alloc` with exactly
        // `self.layout`, and consuming `self` transfers unique ownership here.
        unsafe { alloc.deallocate(self.ptr, self.layout) };
    }

    /// Tear down the arena from a map's `Drop`: swap it out, drop live values via
    /// `drop_values`, free the allocation. A [`DeallocGuard`] still frees on an
    /// unwinding value `Drop` — `Arena` has none of its own.
    ///
    /// # Safety
    ///
    /// `alloc` must be the allocator that created this arena. `drop_values`
    /// must destroy every initialized value in the allocation exactly once.
    #[inline]
    pub(crate) unsafe fn drop_table<A: Allocator>(
        &mut self,
        alloc: &A,
        drop_values: impl FnOnce(),
    ) {
        let arena = mem::replace(self, Arena::empty());
        // SAFETY: the caller supplies the matching allocator required by this
        // method; the guard preserves it through possible unwinding.
        let guard = unsafe { DeallocGuard::new(arena, alloc) };
        drop_values();
        drop(guard);
    }
}

/// Combined ctrl+data layout for an arena whose ctrl section holds
/// `total_ctrl` bytes and data section holds `total_ctrl` slots.
/// Returns `(layout, data_offset_within_arena)`.
pub(crate) fn layout_for<K, V>(total_ctrl: usize) -> Result<(Layout, usize), TryReserveError> {
    layout_for_extents::<K, V>(total_ctrl, total_ctrl)
}

/// Combined ctrl+data layout with independently sized control and slot extents.
pub(crate) fn layout_for_extents<K, V>(
    ctrl_bytes: usize,
    data_slots: usize,
) -> Result<(Layout, usize), TryReserveError> {
    if ctrl_bytes == 0 && data_slots == 0 {
        let layout =
            Layout::from_size_align(0, CACHE_LINE).map_err(|_| TryReserveError::AllocError)?;
        return Ok((layout, 0));
    }
    let ctrl_layout =
        Layout::from_size_align(ctrl_bytes, CACHE_LINE).map_err(|_| TryReserveError::AllocError)?;
    let data_layout =
        Layout::array::<SlotEntry<K, V>>(data_slots).map_err(|_| TryReserveError::AllocError)?;
    let (arena_layout, data_base_off) = ctrl_layout
        .extend(data_layout)
        .map_err(|_| TryReserveError::AllocError)?;
    Ok((arena_layout.pad_to_align(), data_base_off))
}

/// Hands out each arena region's `(ctrl_ptr, data_ptr)` in layout order,
/// advancing the ctrl + data offsets with checked arithmetic — a `u32`
/// overflow yields `CapacityOverflow`, never a wrapped pointer.
pub(crate) struct LayoutCursor<T> {
    base: *mut u8,
    ctrl_off: u32,
    data_off: u32,
    slot_size: u32,
    _marker: PhantomData<fn() -> T>,
}

#[allow(
    unsafe_code,
    reason = "projects checked offsets into the owning arena extent"
)]
impl<T> LayoutCursor<T> {
    /// `data_base_off` is the data section's byte offset (from [`layout_for`]).
    pub(crate) fn new(base: *mut u8, data_base_off: usize) -> Result<Self, TryReserveError> {
        Ok(Self {
            base,
            ctrl_off: 0,
            data_off: u32::try_from(data_base_off)
                .map_err(|_| TryReserveError::CapacityOverflow)?,
            slot_size: u32::try_from(mem::size_of::<T>())
                .map_err(|_| TryReserveError::CapacityOverflow)?,
            _marker: PhantomData,
        })
    }

    /// Current region's `(ctrl_ptr, data_ptr)`, then advances past its `cap`
    /// ctrl bytes + `cap * slot_size` data bytes.
    ///
    /// # Safety
    /// `base` must point at a [`layout_for`] arena covering every reserved
    /// region, in order.
    pub(crate) unsafe fn reserve(
        &mut self,
        cap: u32,
    ) -> Result<(*mut u8, *mut MaybeUninit<T>), TryReserveError> {
        // Both offsets computed before committing, so overflow leaves the
        // cursor unchanged.
        let next_ctrl_off = self
            .ctrl_off
            .checked_add(cap)
            .ok_or(TryReserveError::CapacityOverflow)?;
        let data_bytes = cap
            .checked_mul(self.slot_size)
            .ok_or(TryReserveError::CapacityOverflow)?;
        let next_data_off = self
            .data_off
            .checked_add(data_bytes)
            .ok_or(TryReserveError::CapacityOverflow)?;

        // SAFETY: offsets stay within the caller's arena layout.
        let ctrl_ptr = unsafe { self.base.add(self.ctrl_off as usize) };
        // SAFETY: `data_off` advances only through the caller-proven arena data
        // extent, and `layout_for` aligns its base for `T`.
        let data_ptr = unsafe {
            self.base
                .add(self.data_off as usize)
                .cast::<MaybeUninit<T>>()
        };
        self.ctrl_off = next_ctrl_off;
        self.data_off = next_data_off;
        Ok((ctrl_ptr, data_ptr))
    }
}

/// O(N²) alias check for [`get_disjoint_mut`]-style APIs: panics if two
/// `Some` locations collide. `T: PartialEq` so it works for both raw
/// `(level_idx, slot_idx)` tuples and richer `SlotLocation` enums.
#[inline]
pub(crate) fn check_disjoint_aliasing<T: PartialEq, const N: usize>(locations: &[Option<T>; N]) {
    for (i, li) in locations.iter().enumerate() {
        let Some(li) = li else { continue };
        for other in &locations[i + 1..] {
            assert!(
                other.as_ref() != Some(li),
                "get_disjoint_mut: duplicate keys resolve to the same entry",
            );
        }
    }
}

/// Drop-guard: deallocates the arena on drop, so `V::drop` panics in the
/// map's Drop still free the allocation.
pub(crate) struct DeallocGuard<'a, A: Allocator> {
    arena: Option<Arena>,
    alloc: &'a A,
}

#[allow(unsafe_code, reason = "records the caller-proven allocator identity")]
impl<'a, A: Allocator> DeallocGuard<'a, A> {
    /// # Safety
    ///
    /// `alloc` must be the allocator that created `arena`.
    #[inline]
    pub(crate) unsafe fn new(arena: Arena, alloc: &'a A) -> Self {
        Self {
            arena: Some(arena),
            alloc,
        }
    }
}

#[allow(
    unsafe_code,
    reason = "deallocates through the allocator proven at construction"
)]
impl<A: Allocator> Drop for DeallocGuard<'_, A> {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            // SAFETY: construction requires this exact allocator relationship.
            unsafe { arena.deallocate(self.alloc) };
        }
    }
}

/// A map's complete set of arena regions, for panic-safe teardown. Each
/// backend's region collection implements it so [`ArenaDropGuard`] can drop
/// every region's live values from one place.
pub(crate) trait RegionSet {
    /// Drops the value in every occupied slot across all regions.
    fn drop_all_values(&mut self);
}

/// Owns a half-built (clone) or being-rehashed (resize) arena and its regions.
/// If a `clone`/`insert` unwinds, [`Drop`] drops the regions' live values then
/// deallocates — `Arena` has no `Drop`, so this is what prevents the leak. On
/// the success path call [`disarm`](Self::disarm) to reclaim both.
pub(crate) struct ArenaDropGuard<RS: RegionSet, A: Allocator> {
    arena: Option<Arena>,
    regions: Option<RS>,
    alloc: A,
}

#[allow(
    unsafe_code,
    reason = "records one arena's regions and matching allocator"
)]
impl<RS: RegionSet, A: Allocator> ArenaDropGuard<RS, A> {
    /// # Safety
    ///
    /// `alloc` must be the allocator that created `arena`, and `regions` must
    /// describe every initialized value owned by that arena.
    #[inline]
    pub(crate) unsafe fn new(arena: Arena, regions: RS, alloc: A) -> Self {
        Self {
            arena: Some(arena),
            regions: Some(regions),
            alloc,
        }
    }

    /// Mutable access to the guarded regions (the clone/drain loop writes here).
    #[inline]
    pub(crate) fn regions_mut(&mut self) -> &mut RS {
        self.regions.as_mut().unwrap()
    }

    /// Success path: reclaim `(arena, regions)`; the guard's `Drop` no-ops.
    #[inline]
    pub(crate) fn disarm(mut self) -> (Arena, RS) {
        (self.arena.take().unwrap(), self.regions.take().unwrap())
    }
}

#[allow(
    unsafe_code,
    reason = "runs guarded raw-value teardown before arena deallocation"
)]
impl<RS: RegionSet, A: Allocator> Drop for ArenaDropGuard<RS, A> {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            // Deallocate even if a value's `Drop` unwinds out of
            // `drop_all_values` — otherwise the arena would leak.
            // SAFETY: `ArenaDropGuard::new` established that `self.alloc`
            // created this arena.
            let _dealloc = unsafe { DeallocGuard::new(arena, &self.alloc) };
            if let Some(mut regions) = self.regions.take() {
                regions.drop_all_values();
            }
        }
    }
}

/// One slot's `(key, value)` pair. Co-located so `read`/`drop_in_place`
/// touches both in one shot.
pub(crate) struct SlotEntry<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K: Clone, V: Clone> Clone for SlotEntry<K, V> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }
}

/// Per-region view of a map's arena. Descriptors borrow into the arena
/// allocation, parameterized by slot type `T`.
///
/// # Safety
///
/// Implementors must return pointers into one live arena allocation. The
/// control pointer must address `capacity()` initialized, writable bytes and
/// the data pointer must address `capacity()` correctly aligned slots for `T`.
/// An occupied control byte must correspond to exactly one initialized slot;
/// a free or tombstone control byte must correspond to an uninitialized slot.
/// Mutable access to a descriptor must exclude every other access that could
/// mutate either extent.
#[allow(
    unsafe_code,
    reason = "defines the bounded raw slot and control-byte kernel"
)]
pub(crate) unsafe trait ArenaSlots<T> {
    fn ctrl_ptr(&self) -> *mut u8;
    fn data_ptr(&self) -> *mut MaybeUninit<T>;
    fn capacity(&self) -> usize;

    /// # Safety
    /// `idx` must be smaller than this region's capacity.
    #[inline]
    unsafe fn slot_ptr(&self, idx: usize) -> *mut T {
        debug_assert!(idx < self.capacity());
        // SAFETY: the caller proves that `idx` is inside the slot extent
        // represented by this region descriptor.
        unsafe { self.data_ptr().add(idx).cast::<T>() }
    }

    /// # Safety
    /// `idx` must be smaller than this region's capacity.
    #[inline]
    unsafe fn control_at(&self, idx: usize) -> u8 {
        debug_assert!(idx < self.capacity());
        // SAFETY: the caller proves that `idx` is inside this region's
        // initialized control-byte extent.
        unsafe { *self.ctrl_ptr().add(idx) }
    }

    /// # Safety
    /// `idx` must be smaller than this region's capacity.
    #[inline]
    unsafe fn set_control(&mut self, idx: usize, ctrl: u8) {
        debug_assert!(idx < self.capacity());
        // SAFETY: the caller proves that `idx` is inside this exclusively
        // borrowed region's control-byte extent.
        unsafe { *self.ctrl_ptr().add(idx) = ctrl }
    }

    /// # Safety
    /// `idx` must be smaller than this region's capacity.
    #[inline]
    unsafe fn mark_tombstone(&mut self, idx: usize) {
        // SAFETY: the caller supplies the same in-bounds proof required by
        // `set_control`.
        unsafe { self.set_control(idx, CTRL_TOMBSTONE) };
    }

    /// Wipe every ctrl byte in this region to FREE.
    /// Caller is responsible for having dropped occupied values first.
    #[inline]
    fn clear_all_controls(&mut self) {
        if self.capacity() == 0 {
            return;
        }
        // SAFETY: the descriptor owns exactly `capacity` writable control
        // bytes, and `&mut self` excludes another safe mutation.
        unsafe { ptr::write_bytes(self.ctrl_ptr(), 0, self.capacity()) }
    }

    /// # Safety
    /// `idx` must be smaller than this region's capacity and identify a free,
    /// uninitialized slot.
    #[inline]
    unsafe fn write_with_control(&mut self, idx: usize, entry: T, ctrl: u8) {
        // SAFETY: the caller proves that `idx` is in-bounds.
        debug_assert!(unsafe { self.control_at(idx) }.is_free());
        // SAFETY: the caller proves that the in-bounds destination is free and
        // therefore currently uninitialized.
        unsafe { self.slot_ptr(idx).write(entry) }
        // SAFETY: the caller proves that `idx` is in-bounds; publishing follows
        // complete value initialization.
        unsafe { self.set_control(idx, ctrl) };
    }

    /// SAFETY: caller ensures `idx` is in-bounds and the slot is initialized.
    #[inline]
    unsafe fn get_ref(&self, idx: usize) -> &T {
        debug_assert!(idx < self.capacity());
        // SAFETY: the caller proves that `idx` is in-bounds.
        debug_assert!(unsafe { self.control_at(idx) }.is_occupied());
        // SAFETY: the caller promises an initialized in-bounds slot; the
        // control assertion checks that invariant in debug builds.
        unsafe { &*self.slot_ptr(idx) }
    }

    /// `&mut self` is a type-level proof of exclusive access — without it,
    /// two calls with the same `idx` could hand out aliasing `&mut T` (UB).
    ///
    /// SAFETY: caller ensures `idx` is in-bounds and the slot is initialized.
    #[inline]
    unsafe fn get_mut(&mut self, idx: usize) -> &mut T {
        debug_assert!(idx < self.capacity());
        // SAFETY: the caller proves that `idx` is in-bounds.
        debug_assert!(unsafe { self.control_at(idx) }.is_occupied());
        // SAFETY: the caller promises an initialized in-bounds slot, and
        // `&mut self` prevents a second safe mutable reference to the region.
        unsafe { &mut *self.slot_ptr(idx) }
    }

    /// SAFETY: caller ensures `idx` is in-bounds and the slot is initialized.
    /// The slot must not be read again before being re-written.
    #[inline]
    unsafe fn take(&mut self, idx: usize) -> T {
        debug_assert!(idx < self.capacity());
        // SAFETY: the caller proves that `idx` is in-bounds.
        debug_assert!(unsafe { self.control_at(idx) }.is_occupied());
        // SAFETY: the caller transfers ownership of one initialized in-bounds
        // slot and promises not to read it before another write.
        unsafe { self.slot_ptr(idx).read() }
    }

    /// Drop every value in occupied slots. Call before [`Arena::deallocate`].
    fn drop_values(&mut self) {
        if self.capacity() == 0 {
            return;
        }
        let ctrl = self.ctrl_ptr();
        for idx in 0..self.capacity() {
            // SAFETY: the loop index is inside the descriptor's control extent.
            if unsafe { (*ctrl.add(idx)).is_occupied() } {
                // SAFETY: an occupied control byte proves that this in-bounds
                // slot contains one initialized value owned by the region.
                unsafe { ptr::drop_in_place(self.slot_ptr(idx)) }
            }
        }
    }

    /// Clone every occupied slot of `src` into `self` in panic-safe order: clone
    /// → write → stamp OCCUPIED. A panic mid-loop leaves `self` OCCUPIED only on
    /// fully-written slots; TOMBSTONE bytes follow in a second pass. `self` must
    /// match `src`'s capacity.
    fn clone_region_from(&mut self, src: &Self)
    where
        Self: Sized,
        T: Clone,
    {
        let capacity = src.capacity();
        assert_eq!(capacity, self.capacity(), "clone dst must match src");
        let src_ctrl = src.ctrl_ptr();
        let dst_ctrl = self.ctrl_ptr();
        let src_slots = src.data_ptr();
        let dst_slots = self.data_ptr();
        for idx in 0..capacity {
            // SAFETY: `idx` is inside both equal-capacity control extents.
            let ctrl = unsafe { *src_ctrl.add(idx) };
            if ctrl.is_occupied() {
                // SAFETY: an occupied source control byte proves that the
                // corresponding in-bounds source slot is initialized.
                let cloned = unsafe { (*src_slots.add(idx)).assume_init_ref() }.clone();
                // SAFETY: the destination is exclusively borrowed and its
                // same-capacity slot has not yet been marked occupied.
                unsafe { dst_slots.add(idx).write(MaybeUninit::new(cloned)) };
                // SAFETY: `idx` is in the equal-capacity destination control
                // extent; publishing follows successful value initialization.
                unsafe { *dst_ctrl.add(idx) = ctrl };
            }
        }
        for idx in 0..capacity {
            // SAFETY: `idx` is inside the source control extent.
            let ctrl = unsafe { *src_ctrl.add(idx) };
            if ctrl == CTRL_TOMBSTONE {
                // SAFETY: `idx` is inside the equal-capacity destination
                // control extent and tombstones own no initialized value.
                unsafe { *dst_ctrl.add(idx) = CTRL_TOMBSTONE };
            }
        }
    }

    /// Reset controls for every occupied slot and invoke `visit` with that
    /// slot's pointer after its control byte has been cleared.
    #[inline]
    fn clear_occupied_slots_with<F: FnMut(*mut T)>(&mut self, visit: F) {
        let capacity = self.capacity();
        // SAFETY: this exclusively borrowed descriptor owns `capacity`
        // controls and slots, and occupied controls identify initialized
        // values that the callback may visit exactly once.
        unsafe {
            clear_occupied_slots_raw_with(self.ctrl_ptr(), self.data_ptr(), capacity, visit);
        }
    }

    /// Move every occupied value out and reset all controls to EMPTY.
    /// The current ctrl byte is cleared before `f` runs, so a panic cannot
    /// leave the moved-out slot marked occupied.
    fn drain_values_and_clear<F: FnMut(T)>(&mut self, mut f: F) {
        // SAFETY: `clear_occupied_slots_with` invokes the callback exactly once
        // for initialized slots after making each slot logically unreachable.
        self.clear_occupied_slots_with(|slot| unsafe { f(slot.read()) });
    }
}

/// Reset controls for occupied slots and invoke `visit` after each reset.
///
/// This is the raw-pointer core of [`ArenaSlots::clear_occupied_slots_with`].
/// Keeping the SIMD traversal here lets callers update metadata fields that
/// are disjoint from the arena allocation without manufacturing an aliased
/// mutable borrow of the whole region descriptor.
///
/// # Safety
///
/// `ctrl` must address `capacity` initialized, writable control bytes and
/// `data` must address `capacity` matching slots. Every occupied control must
/// identify one initialized `T` exclusively owned by the caller. `visit` must
/// consume each pointer at most once and must not access a slot after returning.
#[inline]
#[allow(
    unsafe_code,
    reason = "clears one caller-validated raw control and slot extent"
)]
pub(crate) unsafe fn clear_occupied_slots_raw_with<T, F: FnMut(*mut T)>(
    ctrl: *mut u8,
    data: *mut MaybeUninit<T>,
    capacity: usize,
    mut visit: F,
) {
    if capacity == 0 {
        return;
    }
    let full_groups = capacity / GROUP_SIZE;
    for group_idx in 0..full_groups {
        let group_start = group_idx * GROUP_SIZE;
        // SAFETY: each complete group begins inside the control extent.
        let group_ctrl = unsafe { ctrl.add(group_start) };
        // SAFETY: `full_groups` includes only groups with `GROUP_SIZE`
        // readable control bytes.
        for offset in unsafe { simd::occupied_mask_group(group_ctrl) } {
            let idx = group_start + offset;
            // SAFETY: the SIMD mask yields only lanes in this full group;
            // occupied lanes contain initialized in-bounds slots.
            unsafe {
                *ctrl.add(idx) = CTRL_EMPTY;
                visit(data.add(idx).cast::<T>());
            }
        }
        // SAFETY: this full group covers exactly `GROUP_SIZE` writable bytes
        // inside the caller's exclusively owned control extent.
        unsafe { ptr::write_bytes(group_ctrl, CTRL_EMPTY, GROUP_SIZE) };
    }
    for idx in full_groups * GROUP_SIZE..capacity {
        // SAFETY: the scalar tail range is wholly inside the region. An
        // occupied byte proves its matching slot is initialized.
        unsafe {
            let prev = *ctrl.add(idx);
            *ctrl.add(idx) = CTRL_EMPTY;
            if prev.is_occupied() {
                visit(data.add(idx).cast::<T>());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRegion {
        controls: [u8; 2],
        slots: [MaybeUninit<u64>; 2],
        capacity: usize,
    }

    impl TestRegion {
        fn new(capacity: usize) -> Self {
            assert!(capacity <= 2);
            Self {
                controls: [CTRL_EMPTY; 2],
                slots: [MaybeUninit::uninit(); 2],
                capacity,
            }
        }
    }

    #[allow(
        unsafe_code,
        reason = "the test descriptor owns matching bounded control and slot arrays"
    )]
    // SAFETY: both pointers address arrays of two elements, `capacity` is at
    // most two, and every control starts empty with every slot uninitialized.
    unsafe impl ArenaSlots<u64> for TestRegion {
        fn ctrl_ptr(&self) -> *mut u8 {
            self.controls.as_ptr().cast_mut()
        }

        fn data_ptr(&self) -> *mut MaybeUninit<u64> {
            self.slots.as_ptr().cast_mut()
        }

        fn capacity(&self) -> usize {
            self.capacity
        }
    }

    #[test]
    fn zero_ctrl_layout_does_not_allocate() {
        let (layout, data_offset) = layout_for::<u64, u64>(0).unwrap();
        assert_eq!(layout.size(), 0);
        assert_eq!(data_offset, 0);
    }

    #[test]
    fn allocation_rejects_control_extent_beyond_layout() {
        let layout = Layout::from_size_align(8, 8).unwrap();
        assert!(matches!(
            Arena::try_allocate_with_ctrl_zeroed(layout, 9, &allocator_api2::alloc::Global),
            Err(TryReserveError::CapacityOverflow)
        ));
    }

    #[test]
    #[should_panic(expected = "clone dst must match src")]
    fn clone_rejects_mismatched_region_capacities_before_pointer_access() {
        let mut destination = TestRegion::new(1);
        destination.clone_region_from(&TestRegion::new(2));
    }
}
