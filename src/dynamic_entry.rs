//! One-allocation, write-once storage for variable-length overlay entries.
//!
//! The public surface of this module is safe. Internally, one allocation owns
//! an aligned header followed immediately by the immutable key bytes. A slot
//! publishes that allocation once with release ordering and never removes it;
//! readers acquire the pointer, and the slot frees it only when the containing
//! generation is exclusively dropped.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::atomic::{AtomicPtr, Ordering};

#[repr(C)]
struct DynamicEntryHeader<C> {
    key_len: usize,
    cell: C,
}

/// One append-only pointer slot in the arbitrary-length overlay.
pub(crate) struct DynamicEntrySlot<C> {
    pointer: AtomicPtr<DynamicEntryHeader<C>>,
    ownership: PhantomData<Box<DynamicEntryHeader<C>>>,
}

/// Borrowed view of one fully initialized entry.
pub(crate) struct DynamicEntryRef<'a, C> {
    pointer: NonNull<DynamicEntryHeader<C>>,
    marker: PhantomData<&'a DynamicEntryHeader<C>>,
}

impl<C> DynamicEntrySlot<C> {
    pub(crate) const fn empty() -> Self {
        Self {
            pointer: AtomicPtr::new(ptr::null_mut()),
            ownership: PhantomData,
        }
    }

    /// Publishes one entry, returning an error only if this slot was already
    /// initialized. A failed candidate is destroyed before returning.
    #[allow(
        unsafe_code,
        reason = "destroys only the unpublished candidate retained after failed publication"
    )]
    pub(crate) fn set(&self, key: &[u8], cell: C) -> Result<(), ()> {
        let candidate = allocate_entry(key, cell);
        if self
            .pointer
            .compare_exchange(
                ptr::null_mut(),
                candidate.as_ptr(),
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            // SAFETY: `candidate` came from `allocate_entry` and was not
            // published because the compare-exchange failed.
            unsafe { drop_entry(candidate) };
            return Err(());
        }
        Ok(())
    }

    pub(crate) fn get(&self) -> Option<DynamicEntryRef<'_, C>> {
        NonNull::new(self.pointer.load(Ordering::Acquire)).map(|pointer| DynamicEntryRef {
            pointer,
            marker: PhantomData,
        })
    }
}

#[allow(unsafe_code, reason = "uniquely destroys the slot-owned allocation")]
impl<C> Drop for DynamicEntrySlot<C> {
    fn drop(&mut self) {
        let Some(pointer) = NonNull::new(*self.pointer.get_mut()) else {
            return;
        };
        // SAFETY: the slot owns the allocation after its only successful
        // publication, and `&mut self` proves no reader can still borrow it.
        unsafe { drop_entry(pointer) };
    }
}

impl<'a, C> DynamicEntryRef<'a, C> {
    #[allow(
        unsafe_code,
        reason = "projects the initialized trailing key through the owning slot lifetime"
    )]
    pub(crate) fn key(&self) -> &'a [u8] {
        // SAFETY: the slot keeps the allocation alive for `'a`; the stored
        // length and trailing bytes were initialized before publication.
        unsafe {
            let header = self.pointer.as_ref();
            slice::from_raw_parts(
                self.pointer
                    .as_ptr()
                    .cast::<u8>()
                    .add(size_of::<DynamicEntryHeader<C>>()),
                header.key_len,
            )
        }
    }

    #[allow(
        unsafe_code,
        reason = "projects the initialized cell through the owning slot lifetime"
    )]
    pub(crate) fn cell(&self) -> &'a C {
        // SAFETY: release/acquire publication makes the initialized header
        // visible, and the owning slot cannot be dropped during `'a`.
        unsafe { &self.pointer.as_ref().cell }
    }
}

fn entry_layout<C>(key_len: usize) -> (Layout, usize) {
    let key_layout = Layout::array::<u8>(key_len).expect("dynamic key layout fits address space");
    let (layout, key_offset) = Layout::new::<DynamicEntryHeader<C>>()
        .extend(key_layout)
        .expect("dynamic entry layout fits address space");
    (layout.pad_to_align(), key_offset)
}

#[allow(
    unsafe_code,
    reason = "allocates and initializes a header with trailing key bytes"
)]
fn allocate_entry<C>(key: &[u8], cell: C) -> NonNull<DynamicEntryHeader<C>> {
    let (layout, key_offset) = entry_layout::<C>(key.len());
    // SAFETY: `layout` is non-zero because the header always contains a
    // `usize`. Allocation failure follows the standard global-allocator path.
    let raw = unsafe { alloc(layout) };
    let Some(pointer) = NonNull::new(raw.cast::<DynamicEntryHeader<C>>()) else {
        handle_alloc_error(layout);
    };
    // SAFETY: the allocation satisfies the header layout and has room for all
    // trailing key bytes. The regions do not overlap.
    unsafe {
        pointer.as_ptr().write(DynamicEntryHeader {
            key_len: key.len(),
            cell,
        });
        ptr::copy_nonoverlapping(key.as_ptr(), raw.add(key_offset), key.len());
    }
    pointer
}

#[allow(
    unsafe_code,
    reason = "reconstructs and frees the trailing-key allocation"
)]
unsafe fn drop_entry<C>(pointer: NonNull<DynamicEntryHeader<C>>) {
    // SAFETY: callers provide the unique owning pointer created by
    // `allocate_entry`. Read the length before dropping the header.
    let key_len = unsafe { pointer.as_ref().key_len };
    let (layout, _) = entry_layout::<C>(key_len);
    // SAFETY: this is the unique owning pointer; the header is initialized and
    // `key_len` reconstructs the allocation's exact layout.
    unsafe {
        ptr::drop_in_place(pointer.as_ptr());
        dealloc(pointer.as_ptr().cast::<u8>(), layout);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::DynamicEntrySlot;

    #[test]
    fn stores_exact_variable_keys_and_rejects_a_second_value() {
        for key_len in [0, 1, 7, 64, 129, 4097] {
            let key = (0..key_len)
                .map(|index| u8::try_from(index % 251).expect("byte remainder fits"))
                .collect::<Vec<_>>();
            let slot = DynamicEntrySlot::empty();
            assert!(slot.set(&key, key_len).is_ok());
            let entry = slot.get().expect("published entry is visible");
            assert_eq!(entry.key(), key);
            assert_eq!(*entry.cell(), key_len);
            assert!(slot.set(b"duplicate", usize::MAX).is_err());
            assert_eq!(*slot.get().expect("first entry remains").cell(), key_len);
        }
    }

    #[test]
    fn drops_the_published_cell_exactly_once() {
        struct DropCount(Arc<AtomicUsize>);

        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        {
            let slot = DynamicEntrySlot::empty();
            assert!(slot.set(b"owned", DropCount(Arc::clone(&drops))).is_ok());
            assert_eq!(drops.load(Ordering::Relaxed), 0);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn preserves_overaligned_cell_storage() {
        #[repr(align(128))]
        struct Overaligned(u64);

        let slot = DynamicEntrySlot::empty();
        assert!(slot.set(b"aligned", Overaligned(42)).is_ok());
        let cell = slot.get().expect("published entry").cell();
        assert_eq!(cell.0, 42);
        assert_eq!(std::ptr::from_ref(cell) as usize % 128, 0);
    }

    #[test]
    fn trailing_key_starts_at_the_header_size() {
        let (_, key_offset) = super::entry_layout::<u64>(37);
        assert_eq!(
            key_offset,
            std::mem::size_of::<super::DynamicEntryHeader<u64>>()
        );
    }
}
