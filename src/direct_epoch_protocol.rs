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

pub(crate) const fn epoch_slot(token: usize) -> usize {
    let slot = token & EPOCH_SLOT_MASK;
    debug_assert!(slot < EPOCHS);
    slot
}

pub(crate) const fn next_epoch_token(token: usize) -> Option<usize> {
    match epoch_slot(token) {
        0 | 1 => token.checked_add(1),
        2 => token.checked_add(2),
        _ => unreachable!(),
    }
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
