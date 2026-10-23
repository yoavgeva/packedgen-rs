//! Shared direct-cache epoch state machine.
//!
//! Production instantiates these functions with standard atomics. The Loom
//! integration test includes this exact source and instantiates it with Loom
//! atomics, preventing the modeled epoch transitions from drifting.

pub(crate) const EPOCHS: usize = 3;
const EPOCH_SLOT_MASK: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EpochAdvance {
    pub(crate) next_generation: usize,
    pub(crate) next_epoch: usize,
}

#[inline]
pub(crate) const fn epoch_slot(token: usize) -> usize {
    let slot = token & EPOCH_SLOT_MASK;
    debug_assert!(slot < EPOCHS);
    slot
}

#[inline]
pub(crate) const fn next_epoch_token(token: usize) -> Option<usize> {
    match epoch_slot(token) {
        0 | 1 => token.checked_add(1),
        2 => token.checked_add(2),
        _ => unreachable!(),
    }
}

/// Generation advances between retiring a slot and reclaiming it.
pub(crate) const AGING_ADVANCES: usize = 3;

/// The first generation token at which a slot tagged `tag` may be
/// reclaimed: `AGING_ADVANCES` advances later. The advance that reuses the
/// tag's epoch slot found that slot and the next quiescent. Saturates, never
/// reclaiming, past the last representable token.
#[allow(dead_code, reason = "each including crate uses a different subset")]
#[inline]
pub(crate) fn reclaim_at(tag: usize) -> usize {
    let mut aged = tag;
    for _ in 0..AGING_ADVANCES {
        let Some(next) = next_epoch_token(aged) else {
            return usize::MAX;
        };
        aged = next;
    }
    aged
}

#[inline]
pub(crate) fn enter_epoch<Load, Acquire, Release>(
    mut load_generation: Load,
    mut acquire_reader: Acquire,
    mut release_reader: Release,
) -> usize
where
    Load: FnMut() -> usize,
    Acquire: FnMut(usize),
    Release: FnMut(usize) -> usize,
{
    loop {
        let generation = load_generation();
        let epoch = epoch_slot(generation);
        acquire_reader(epoch);
        if load_generation() == generation {
            return epoch;
        }
        let previous = release_reader(epoch);
        debug_assert_ne!(previous, 0);
    }
}

#[inline]
pub(crate) fn leave_epoch<Release>(epoch: usize, release_reader: Release)
where
    Release: FnOnce(usize) -> usize,
{
    let previous = release_reader(epoch);
    assert_ne!(previous, 0, "direct reader guard left twice");
}

pub(crate) fn plan_epoch_advance<IsQuiescent>(
    current_generation: usize,
    mut is_quiescent: IsQuiescent,
) -> Option<EpochAdvance>
where
    IsQuiescent: FnMut(usize) -> bool,
{
    let current_epoch = epoch_slot(current_generation);
    let next_generation = next_epoch_token(current_generation)?;
    let next_epoch = epoch_slot(next_generation);
    let previous_epoch = match current_epoch {
        0 => EPOCHS - 1,
        1 | 2 => current_epoch - 1,
        _ => unreachable!(),
    };
    // A just-completed rotation most often leaves readers in the immediately
    // previous epoch. Check it first so contended maintenance fails fast.
    if !is_quiescent(previous_epoch) || !is_quiescent(next_epoch) {
        return None;
    }
    Some(EpochAdvance {
        next_generation,
        next_epoch,
    })
}

/// An announcement word, written only by its thread: `pins << 3 |
/// ANNOUNCED | epoch`. A thread that leaves its last pin keeps the epoch
/// announced, parked, so its next pin can reuse it with a light fence.
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) const ANNOUNCED_EPOCH: usize = 0b11;
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) const ANNOUNCED: usize = 1 << 2;
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) const ANNOUNCED_PIN: usize = 1 << 3;
/// An announcement's eviction state: an advance set it before its heavy
/// fence...
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) const EVICTING: usize = 1;
/// ...and found the announcement parked after it.
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) const EVICTED: usize = 2;

/// Pins through an announcement whose word is `word`: under an outer pin, or
/// by reusing a parked epoch. Reuse stores the word active, issues the light
/// fence, and keeps the epoch only if no advance evicted it and the
/// generation still names it. Returns `None` when the caller must announce
/// afresh; the stored word then counts this pin.
#[allow(dead_code, reason = "each including crate uses a different subset")]
#[inline]
pub(crate) fn reuse_announcement<StoreWord, LightFence, LoadEvicted, LoadGeneration>(
    word: usize,
    store_word: StoreWord,
    light_fence: LightFence,
    load_evicted: LoadEvicted,
    load_generation: LoadGeneration,
) -> Option<usize>
where
    StoreWord: FnOnce(usize),
    LightFence: FnOnce(),
    LoadEvicted: FnOnce() -> usize,
    LoadGeneration: FnOnce() -> usize,
{
    let epoch = word & ANNOUNCED_EPOCH;
    if word >= ANNOUNCED_PIN {
        store_word(word + ANNOUNCED_PIN);
        return Some(epoch);
    }
    if word & ANNOUNCED == 0 {
        return None;
    }
    store_word(word + ANNOUNCED_PIN);
    light_fence();
    (load_evicted() == 0 && epoch_slot(load_generation()) == epoch).then_some(epoch)
}

/// Announces the current epoch for one pin, as `enter_epoch` enters it:
/// store, sequentially consistent fence, and a generation that has not
/// moved. Then clears any eviction, as the announcement is fresh. The store
/// must release: an advance that reads the new epoch counts the thread out
/// of its old one.
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) fn announce_afresh<LoadGeneration, StoreWord, Fence, LoadEvicted, ClearEvicted>(
    mut load_generation: LoadGeneration,
    mut store_word: StoreWord,
    mut fence: Fence,
    load_evicted: LoadEvicted,
    clear_evicted: ClearEvicted,
) -> usize
where
    LoadGeneration: FnMut() -> usize,
    StoreWord: FnMut(usize),
    Fence: FnMut(),
    LoadEvicted: FnOnce() -> usize,
    ClearEvicted: FnOnce(),
{
    loop {
        let generation = load_generation();
        let epoch = epoch_slot(generation);
        store_word(ANNOUNCED_PIN | ANNOUNCED | epoch);
        fence();
        if load_generation() == generation {
            if load_evicted() != 0 {
                clear_evicted();
            }
            return epoch;
        }
    }
}

/// The word once a pin leaves `word`: the last pin leaves it parked.
#[allow(dead_code, reason = "each including crate uses a different subset")]
#[inline]
pub(crate) fn leave_announcement(word: usize) -> usize {
    assert!(word >= ANNOUNCED_PIN, "direct reader guard left twice");
    word - ANNOUNCED_PIN
}

/// How an advance that needs `epoch` quiescent treats an announcement.
#[allow(dead_code, reason = "each including crate uses a different subset")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Announced {
    /// Not in `epoch`, or parked and already evicted.
    Clear,
    /// A pin holds it in `epoch`.
    Blocks,
    /// Parked in `epoch`: the advance must evict it.
    Evict,
}

#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) fn classify_announcement<LoadEvicted>(
    word: usize,
    load_evicted: LoadEvicted,
    epoch: usize,
) -> Announced
where
    LoadEvicted: FnOnce() -> usize,
{
    if word & ANNOUNCED == 0 || word & ANNOUNCED_EPOCH != epoch {
        Announced::Clear
    } else if word >= ANNOUNCED_PIN {
        Announced::Blocks
    } else if load_evicted() == EVICTED {
        Announced::Clear
    } else {
        Announced::Evict
    }
}

/// An eviction's outcome from the word read after the heavy fence, for an
/// advance from a generation whose epoch is `current`: `None` if a pin holds
/// a retiring epoch, so the advance must wait; else whether the
/// announcement is parked and now evicted. A pin in the current epoch
/// announced afresh and needs no eviction.
#[allow(dead_code, reason = "each including crate uses a different subset")]
pub(crate) fn eviction_outcome(word: usize, current: usize) -> Option<bool> {
    if word < ANNOUNCED_PIN {
        Some(true)
    } else if word & ANNOUNCED_EPOCH == current {
        Some(false)
    } else {
        None
    }
}
