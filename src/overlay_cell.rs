use std::sync::Arc;

use arc_swap::{ArcSwapOption, Guard};

/// Atomically replaceable value stored behind one stable overlay key.
///
/// `None` is a deletion marker. Absence from the containing map means the
/// immutable base remains visible. Keeping the cell stable lets repeated point
/// mutations use borrowed key lookup and cell-local CAS instead of allocating
/// another owned key for every Papaya compute operation.
pub(crate) struct OverlayCell<V> {
    value: ArcSwapOption<V>,
}

pub(crate) trait StableCell: Sized {
    fn deleted() -> Self;

    /// Moves an unpublished cell value into an initially deleted stable slot.
    fn initialize_from(&self, source: Self);
}

pub(crate) trait GenerationCell<V>: StableCell {
    const COMPACT_FIXED32_OVERLAY: bool;

    fn present(value: V) -> Self;

    fn with_value<R>(&self, read: impl FnOnce(Option<&V>) -> R) -> R;

    fn replace(&self, value: V) -> Option<V>
    where
        V: Clone;

    fn insert_new(&self, value: V) -> bool;

    fn update(&self, update: &impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone;

    fn upsert(&self, insert_value: &V, update: &impl Fn(&V) -> V) -> (V, bool)
    where
        V: Clone;

    fn remove_if(&self, predicate: &impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone;
}

impl<V> OverlayCell<V> {
    pub(crate) fn present(value: V) -> Self {
        Self {
            value: ArcSwapOption::from(Some(Arc::new(value))),
        }
    }

    pub(crate) fn deleted() -> Self {
        Self {
            value: ArcSwapOption::empty(),
        }
    }

    pub(crate) fn with_value<R>(&self, read: impl FnOnce(Option<&V>) -> R) -> R {
        let value = self.value.load();
        read(value.as_deref())
    }

    pub(crate) fn replace(&self, value: V) -> Option<V>
    where
        V: Clone,
    {
        self.value
            .swap(Some(Arc::new(value)))
            .map(|previous| previous.as_ref().clone())
    }

    pub(crate) fn insert_new(&self, value: V) -> bool {
        let expected = None::<Arc<V>>;
        self.value
            .compare_and_swap(&expected, Some(Arc::new(value)))
            .is_none()
    }

    pub(crate) fn update(&self, update: &impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        let mut current = self.value.load_full();
        loop {
            let current_value = current.as_deref()?;
            let next = Arc::new(update(current_value));
            let result = next.as_ref().clone();
            let previous = self.value.compare_and_swap(&current, Some(next));
            if option_ptr_eq(current.as_ref(), previous.as_ref()) {
                return Some(result);
            }
            current = Guard::into_inner(previous);
        }
    }

    pub(crate) fn upsert(&self, insert_value: &V, update: &impl Fn(&V) -> V) -> (V, bool)
    where
        V: Clone,
    {
        let mut current = self.value.load_full();
        loop {
            let became_live = current.is_none();
            let next = Arc::new(
                current
                    .as_deref()
                    .map_or_else(|| insert_value.clone(), update),
            );
            let result = next.as_ref().clone();
            let previous = self.value.compare_and_swap(&current, Some(next));
            if option_ptr_eq(current.as_ref(), previous.as_ref()) {
                return (result, became_live);
            }
            current = Guard::into_inner(previous);
        }
    }

    pub(crate) fn remove_if(&self, predicate: &impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        let mut current = self.value.load_full();
        loop {
            let current_value = current.as_deref()?;
            if !predicate(current_value) {
                return None;
            }
            let removed = current_value.clone();
            let previous = self.value.compare_and_swap(&current, None);
            if option_ptr_eq(current.as_ref(), previous.as_ref()) {
                return Some(removed);
            }
            current = Guard::into_inner(previous);
        }
    }
}

impl<V> StableCell for OverlayCell<V> {
    fn deleted() -> Self {
        Self::deleted()
    }

    fn initialize_from(&self, source: Self) {
        self.value.store(source.value.load_full());
    }
}

impl<V> GenerationCell<V> for OverlayCell<V> {
    const COMPACT_FIXED32_OVERLAY: bool = false;

    fn present(value: V) -> Self {
        Self::present(value)
    }

    fn with_value<R>(&self, read: impl FnOnce(Option<&V>) -> R) -> R {
        self.with_value(read)
    }

    fn replace(&self, value: V) -> Option<V>
    where
        V: Clone,
    {
        self.replace(value)
    }

    fn insert_new(&self, value: V) -> bool {
        self.insert_new(value)
    }

    fn update(&self, update: &impl Fn(&V) -> V) -> Option<V>
    where
        V: Clone,
    {
        self.update(update)
    }

    fn upsert(&self, insert_value: &V, update: &impl Fn(&V) -> V) -> (V, bool)
    where
        V: Clone,
    {
        self.upsert(insert_value, update)
    }

    fn remove_if(&self, predicate: &impl Fn(&V) -> bool) -> Option<V>
    where
        V: Clone,
    {
        self.remove_if(predicate)
    }
}

fn option_ptr_eq<V>(left: Option<&Arc<V>>, right: Option<&Arc<V>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}
