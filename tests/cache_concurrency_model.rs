//! Exhaustive small-state models for the direct cache publication and QSBR protocol.

#[path = "../src/direct_epoch_protocol.rs"]
mod direct_epoch_protocol;

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use loom::sync::{Arc, Mutex};
use loom::thread;

use direct_epoch_protocol::{
    ANNOUNCED, ANNOUNCED_PIN, Announced, EPOCHS, EVICTED, EVICTING, announce_afresh,
    classify_announcement, enter_epoch, epoch_slot, eviction_outcome, leave_announcement,
    leave_epoch, next_epoch_token, plan_epoch_advance, reclaim_at, reuse_announcement,
};

const ABSENT: usize = 0;
const OLD: usize = 1;
const NEW: usize = 2;
const NO_RETIRED_EPOCH: usize = usize::MAX;
const WRITER_COUNT_MASK: usize = 0b11;
const WRITER_CLOSED: usize = 0b100;

fn model_bounded(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    // Three preemptions cover every deliberately forced regression below and
    // keep the general three-thread models practical in ordinary CI. A caller
    // can request a deeper run with LOOM_MAX_PREEMPTIONS.
    if builder.preemption_bound.is_none() {
        builder.preemption_bound = Some(3);
    }
    builder.check(model);
}

struct ModelState {
    current_epoch: AtomicUsize,
    reclaim_gate: Mutex<()>,
    mutator_activity: AtomicBool,
    mutator: AtomicBool,
    readers: [AtomicUsize; EPOCHS],
    index: AtomicUsize,
    new_value: AtomicUsize,
    old_alive: AtomicBool,
    retired_epoch: AtomicUsize,
}

impl ModelState {
    fn new() -> Self {
        Self {
            current_epoch: AtomicUsize::new(0),
            reclaim_gate: Mutex::new(()),
            // Match production's real initial state. The first standalone
            // mutation must establish permanent observation under the gate.
            mutator_activity: AtomicBool::new(false),
            mutator: AtomicBool::new(false),
            readers: std::array::from_fn(|_| AtomicUsize::new(0)),
            index: AtomicUsize::new(OLD),
            new_value: AtomicUsize::new(0),
            old_alive: AtomicBool::new(true),
            retired_epoch: AtomicUsize::new(NO_RETIRED_EPOCH),
        }
    }

    fn enter(&self) -> usize {
        enter_epoch(
            || self.current_epoch.load(Ordering::SeqCst),
            |epoch| {
                self.readers[epoch].fetch_add(1, Ordering::SeqCst);
            },
            |epoch| self.readers[epoch].fetch_sub(1, Ordering::SeqCst),
        )
    }

    fn leave(&self, epoch: usize) {
        leave_epoch(epoch, |epoch| {
            self.readers[epoch].fetch_sub(1, Ordering::SeqCst)
        });
    }

    fn read_once(&self) {
        let epoch = self.enter();
        match self.index.load(Ordering::Acquire) {
            ABSENT => {}
            OLD => assert!(
                self.old_alive.load(Ordering::SeqCst),
                "an epoch-protected reader observed reclaimed storage"
            ),
            NEW => assert_eq!(
                self.new_value.load(Ordering::Relaxed),
                42,
                "release publication must expose the initialized replacement"
            ),
            other => panic!("unknown modeled handle {other}"),
        }
        self.leave(epoch);
    }

    fn activate_mutators(&self) {
        if self.mutator_activity.load(Ordering::SeqCst) {
            return;
        }
        let _reclaim = self.reclaim_gate.lock().unwrap();
        if !self.mutator_activity.load(Ordering::Relaxed) {
            self.mutator_activity.store(true, Ordering::SeqCst);
        }
    }

    fn retire_old_after(&self, replacement: usize) {
        self.activate_mutators();
        self.mutator
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("the modeled mutation slot is unoccupied");
        if replacement == NEW {
            self.new_value.store(42, Ordering::Relaxed);
        }
        self.index.store(replacement, Ordering::Release);
        let retirement_epoch =
            direct_epoch_protocol::epoch_slot(self.current_epoch.load(Ordering::SeqCst));
        self.retired_epoch
            .store(retirement_epoch, Ordering::Release);
        self.mutator.store(false, Ordering::SeqCst);
    }

    fn reclaim_after_full_epoch_cycle(&self) {
        for _ in 0..EPOCHS {
            self.force_advance_once();
        }
    }

    fn force_advance_once(&self) {
        let Ok(_reclaim) = self.reclaim_gate.try_lock() else {
            return;
        };
        if self.mutator_activity.load(Ordering::SeqCst)
            && self
                .mutator
                .compare_exchange(false, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return;
        }
        let generation = self.current_epoch.load(Ordering::SeqCst);
        let Some(advance) = plan_epoch_advance(generation, |epoch| {
            self.readers[epoch].fetch_add(0, Ordering::SeqCst) == 0
        }) else {
            return;
        };
        let retired_epoch = self.retired_epoch.load(Ordering::Acquire);
        if retired_epoch == advance.next_epoch {
            self.old_alive.store(false, Ordering::SeqCst);
            self.retired_epoch
                .store(NO_RETIRED_EPOCH, Ordering::Release);
        }
        self.current_epoch
            .compare_exchange(
                generation,
                advance.next_generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .expect("the modeled collector is serialized");
    }
}

fn wait_until(flag: &AtomicBool) {
    while !flag.load(Ordering::Acquire) {
        thread::yield_now();
    }
}

fn model_retirement(replacement: usize) {
    model_bounded(move || {
        let state = Arc::new(ModelState::new());

        let reader = {
            let state = Arc::clone(&state);
            thread::spawn(move || state.read_once())
        };
        let writer = {
            let state = Arc::clone(&state);
            thread::spawn(move || state.retire_old_after(replacement))
        };
        let reclaimer = {
            let state = Arc::clone(&state);
            thread::spawn(move || state.reclaim_after_full_epoch_cycle())
        };

        reader.join().unwrap();
        writer.join().unwrap();
        reclaimer.join().unwrap();
    });
}

fn try_acquire_writer(counter: &AtomicUsize) -> bool {
    let mut state = counter.load(Ordering::Relaxed);
    loop {
        if state & WRITER_CLOSED != 0 || state & WRITER_COUNT_MASK == WRITER_COUNT_MASK {
            return false;
        }
        match counter.compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => return true,
            Err(observed) => state = observed,
        }
    }
}

fn close_writer_counter(counter: &AtomicUsize) {
    let mut state = counter.load(Ordering::Acquire);
    loop {
        if state & WRITER_CLOSED != 0 || state & WRITER_COUNT_MASK != 0 {
            return;
        }
        match counter.compare_exchange_weak(
            state,
            state | WRITER_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(observed) => state = observed,
        }
    }
}

#[test]
fn replacement_publication_and_old_value_reclamation_are_ordered() {
    model_retirement(NEW);
}

#[test]
fn removal_cannot_reclaim_a_value_still_visible_to_a_reader() {
    model_retirement(ABSENT);
}

#[test]
fn forced_empty_epoch_advance_cannot_detach_a_reader_from_later_retirement() {
    model_bounded(|| {
        let state = Arc::new(ModelState::new());
        let reader_entered = Arc::new(AtomicBool::new(false));
        let first_advance_finished = Arc::new(AtomicBool::new(false));
        let reader_loaded = Arc::new(AtomicBool::new(false));
        let reclamation_finished = Arc::new(AtomicBool::new(false));

        let reader = {
            let state = Arc::clone(&state);
            let reader_entered = Arc::clone(&reader_entered);
            let first_advance_finished = Arc::clone(&first_advance_finished);
            let reader_loaded = Arc::clone(&reader_loaded);
            let reclamation_finished = Arc::clone(&reclamation_finished);
            thread::spawn(move || {
                let epoch = state.enter();
                reader_entered.store(true, Ordering::Release);
                wait_until(&first_advance_finished);
                assert_eq!(state.index.load(Ordering::Acquire), OLD);
                reader_loaded.store(true, Ordering::Release);
                wait_until(&reclamation_finished);
                assert!(
                    state.old_alive.load(Ordering::SeqCst),
                    "forced empty-epoch reuse detached a live reader from retirement"
                );
                state.leave(epoch);
            })
        };

        let maintainer = {
            let state = Arc::clone(&state);
            let reader_entered = Arc::clone(&reader_entered);
            let first_advance_finished = Arc::clone(&first_advance_finished);
            let reader_loaded = Arc::clone(&reader_loaded);
            let reclamation_finished = Arc::clone(&reclamation_finished);
            thread::spawn(move || {
                wait_until(&reader_entered);
                state.force_advance_once();
                first_advance_finished.store(true, Ordering::Release);
                wait_until(&reader_loaded);
                state.retire_old_after(ABSENT);
                for _ in 0..EPOCHS {
                    state.force_advance_once();
                }
                reclamation_finished.store(true, Ordering::Release);
            })
        };

        reader.join().unwrap();
        maintainer.join().unwrap();
    });
}

#[test]
fn rotation_during_a_mutation_cannot_reclaim_under_a_new_epoch_reader() {
    model_bounded(|| {
        let state = Arc::new(ModelState::new());
        let later_reader_loaded = Arc::new(AtomicBool::new(false));
        let reclamation_attempted = Arc::new(AtomicBool::new(false));

        let writer_epoch = state.enter();
        state.force_advance_once();

        let reader = {
            let state = Arc::clone(&state);
            let later_reader_loaded = Arc::clone(&later_reader_loaded);
            let reclamation_attempted = Arc::clone(&reclamation_attempted);
            thread::spawn(move || {
                let reader_epoch = state.enter();
                assert_eq!(state.index.load(Ordering::Acquire), OLD);
                later_reader_loaded.store(true, Ordering::Release);
                wait_until(&reclamation_attempted);
                assert!(
                    state.old_alive.load(Ordering::SeqCst),
                    "a second rotation reclaimed beneath a later-epoch reader"
                );
                state.leave(reader_epoch);
            })
        };

        wait_until(&later_reader_loaded);
        state.index.store(ABSENT, Ordering::Release);
        let retirement_epoch =
            direct_epoch_protocol::epoch_slot(state.current_epoch.load(Ordering::SeqCst));
        state
            .retired_epoch
            .store(retirement_epoch, Ordering::Release);
        state.leave(writer_epoch);
        for _ in 0..EPOCHS {
            state.force_advance_once();
        }
        reclamation_attempted.store(true, Ordering::Release);
        reader.join().unwrap();
    });
}

#[test]
fn full_writer_counter_never_looks_quiescent_to_a_closer() {
    model_bounded(|| {
        // Two long-lived guards are already present. Of two racing new
        // acquisitions, at most one can consume the final modeled count.
        let counter = Arc::new(AtomicUsize::new(WRITER_COUNT_MASK - 1));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let counter = Arc::clone(&counter);
            joins.push(thread::spawn(move || {
                if try_acquire_writer(&counter) {
                    let previous = counter.fetch_sub(1, Ordering::Release);
                    assert_ne!(previous & WRITER_COUNT_MASK, 0);
                }
            }));
        }
        let closer = {
            let counter = Arc::clone(&counter);
            thread::spawn(move || close_writer_counter(&counter))
        };

        for join in joins {
            join.join().unwrap();
        }
        closer.join().unwrap();
        let final_state = counter.load(Ordering::Acquire);
        assert_eq!(final_state & WRITER_COUNT_MASK, WRITER_COUNT_MASK - 1);
        assert_eq!(final_state & WRITER_CLOSED, 0);
    });
}

/// `ShardedCache`'s reclamation: a writer pinned before unpublishing a slot
/// tags it with the generation token read afterwards, and any thread may
/// free it once the generation reaches the slot's due token.
struct TokenModel {
    generation: AtomicUsize,
    readers: [AtomicUsize; EPOCHS],
    advancing: Mutex<()>,
    index: AtomicUsize,
    old_alive: AtomicBool,
    due: AtomicUsize,
}

impl TokenModel {
    fn new() -> Self {
        Self {
            generation: AtomicUsize::new(0),
            readers: std::array::from_fn(|_| AtomicUsize::new(0)),
            advancing: Mutex::new(()),
            index: AtomicUsize::new(OLD),
            old_alive: AtomicBool::new(true),
            due: AtomicUsize::new(NO_RETIRED_EPOCH),
        }
    }

    fn enter(&self) -> usize {
        enter_epoch(
            || self.generation.load(Ordering::SeqCst),
            |epoch| {
                self.readers[epoch].fetch_add(1, Ordering::SeqCst);
            },
            |epoch| self.readers[epoch].fetch_sub(1, Ordering::SeqCst),
        )
    }

    fn leave(&self, epoch: usize) {
        leave_epoch(epoch, |epoch| {
            self.readers[epoch].fetch_sub(1, Ordering::SeqCst)
        });
    }

    fn read_once(&self) {
        let epoch = self.enter();
        if self.index.load(Ordering::Acquire) == OLD {
            assert!(
                self.old_alive.load(Ordering::SeqCst),
                "a pinned reader observed a reclaimed slot"
            );
        }
        self.leave(epoch);
    }

    /// Unpublishes the old slot while pinned and tags it with `age`
    /// applied to the generation read afterwards.
    fn retire_old(&self, age: fn(usize) -> usize) {
        let epoch = self.enter();
        self.index.swap(ABSENT, Ordering::AcqRel);
        let tag = self.generation.load(Ordering::SeqCst);
        // Production queues the slot under a mutex; release-acquire on the
        // due token models that hand-off to the reclaimer.
        self.due.store(age(tag), Ordering::Release);
        self.leave(epoch);
    }

    fn try_advance(&self) {
        let Ok(_advancing) = self.advancing.try_lock() else {
            return;
        };
        let generation = self.generation.load(Ordering::SeqCst);
        let Some(advance) = plan_epoch_advance(generation, |epoch| {
            self.readers[epoch].fetch_add(0, Ordering::SeqCst) == 0
        }) else {
            return;
        };
        let _ = self.generation.compare_exchange(
            generation,
            advance.next_generation,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn reclaim_if_due(&self) {
        let due = self.due.load(Ordering::Acquire);
        if due != NO_RETIRED_EPOCH && self.generation.load(Ordering::SeqCst) >= due {
            self.old_alive.store(false, Ordering::SeqCst);
        }
    }
}

fn one_advance(tag: usize) -> usize {
    next_epoch_token(tag).expect("the model's tokens stay small")
}

#[test]
fn token_aged_reclamation_never_frees_under_a_reader() {
    model_bounded(|| {
        let state = Arc::new(TokenModel::new());
        let reader = {
            let state = Arc::clone(&state);
            thread::spawn(move || state.read_once())
        };
        let writer = {
            let state = Arc::clone(&state);
            thread::spawn(move || state.retire_old(reclaim_at))
        };
        let reclaimer = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                for _ in 0..EPOCHS {
                    state.try_advance();
                    state.reclaim_if_due();
                }
            })
        };
        reader.join().unwrap();
        writer.join().unwrap();
        reclaimer.join().unwrap();
    });
}

/// Admits a reader at the epoch after the writer's pin, as a racing rotation
/// can, and checks that `age` keeps the slot alive until that reader leaves.
fn model_reader_admitted_after_the_writers_pin(age: fn(usize) -> usize) {
    model_bounded(move || {
        let state = Arc::new(TokenModel::new());
        let reader_loaded = Arc::new(AtomicBool::new(false));
        let reclamation_attempted = Arc::new(AtomicBool::new(false));

        let writer_epoch = state.enter();
        state.try_advance();

        let reader = {
            let state = Arc::clone(&state);
            let reader_loaded = Arc::clone(&reader_loaded);
            let reclamation_attempted = Arc::clone(&reclamation_attempted);
            thread::spawn(move || {
                let epoch = state.enter();
                assert_eq!(state.index.load(Ordering::Acquire), OLD);
                reader_loaded.store(true, Ordering::Release);
                wait_until(&reclamation_attempted);
                assert!(
                    state.old_alive.load(Ordering::SeqCst),
                    "a slot was reclaimed beneath a later-epoch reader"
                );
                state.leave(epoch);
            })
        };

        wait_until(&reader_loaded);
        state.index.swap(ABSENT, Ordering::AcqRel);
        let tag = state.generation.load(Ordering::SeqCst);
        state.due.store(age(tag), Ordering::Release);
        state.leave(writer_epoch);
        for _ in 0..EPOCHS {
            state.try_advance();
        }
        state.reclaim_if_due();
        reclamation_attempted.store(true, Ordering::Release);
        reader.join().unwrap();
    });
}

#[test]
fn token_aged_reclamation_waits_for_a_reader_admitted_after_the_writers_pin() {
    model_reader_admitted_after_the_writers_pin(reclaim_at);
}

#[test]
#[should_panic(expected = "a slot was reclaimed beneath a later-epoch reader")]
fn one_advance_of_aging_reclaims_beneath_a_later_epoch_reader() {
    // Negative control: the model must catch aging too short to cover a
    // reader admitted during a racing rotation.
    model_reader_admitted_after_the_writers_pin(one_advance);
}

/// Reader shards per masked model: the writer's and the reader's.
const MODEL_SHARDS: usize = 2;

/// `ShardedCache`'s advances scan only the reader shards some thread has
/// claimed: a thread sets its shard's bit, sequentially consistently, before
/// its first pin, and an advance reads the mask after the generation with a
/// zero-delta read-modify-write.
struct MaskedTokenModel {
    generation: AtomicUsize,
    readers: [[AtomicUsize; MODEL_SHARDS]; EPOCHS],
    shards: AtomicUsize,
    index: AtomicUsize,
    old_alive: AtomicBool,
    due: AtomicUsize,
}

impl MaskedTokenModel {
    fn new() -> Self {
        Self {
            generation: AtomicUsize::new(0),
            readers: std::array::from_fn(|_| std::array::from_fn(|_| AtomicUsize::new(0))),
            shards: AtomicUsize::new(0),
            index: AtomicUsize::new(OLD),
            old_alive: AtomicBool::new(true),
            due: AtomicUsize::new(NO_RETIRED_EPOCH),
        }
    }

    fn claim(&self, shard: usize) {
        self.shards.fetch_or(1 << shard, Ordering::SeqCst);
    }

    /// The claimed shards, read as production reads them for an advance:
    /// with a zero-delta read-modify-write, which observes the latest claim
    /// where a load may not.
    fn claimed(&self) -> usize {
        self.shards.fetch_or(0, Ordering::SeqCst)
    }

    fn enter(&self, shard: usize) -> usize {
        enter_epoch(
            || self.generation.load(Ordering::SeqCst),
            |epoch| {
                self.readers[epoch][shard].fetch_add(1, Ordering::SeqCst);
            },
            |epoch| self.readers[epoch][shard].fetch_sub(1, Ordering::SeqCst),
        )
    }

    fn leave(&self, epoch: usize, shard: usize) {
        leave_epoch(epoch, |epoch| {
            self.readers[epoch][shard].fetch_sub(1, Ordering::SeqCst)
        });
    }

    /// Advances from `generation` if the claimed shards in `mask` are
    /// quiescent in the epochs the advance retires.
    fn advance(&self, generation: usize, mask: usize) {
        let Some(advance) = plan_epoch_advance(generation, |epoch| {
            (0..MODEL_SHARDS)
                .filter(|shard| mask & 1 << shard != 0)
                .all(|shard| self.readers[epoch][shard].fetch_add(0, Ordering::SeqCst) == 0)
        }) else {
            return;
        };
        let _ = self.generation.compare_exchange(
            generation,
            advance.next_generation,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn reclaim_if_due(&self) {
        let due = self.due.load(Ordering::Acquire);
        if due != NO_RETIRED_EPOCH && self.generation.load(Ordering::SeqCst) >= due {
            self.old_alive.store(false, Ordering::SeqCst);
        }
    }
}

/// A writer retires the old slot while a reader on another shard claims
/// that shard and pins; the main thread advances, reading the claimed shards
/// with `claimed`, and reclaims when due.
fn model_masked_reclamation(claimed: fn(&MaskedTokenModel) -> usize) {
    model_bounded(move || {
        let state = Arc::new(MaskedTokenModel::new());
        state.claim(0);
        let reader = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                state.claim(1);
                let epoch = state.enter(1);
                if state.index.load(Ordering::Acquire) == OLD {
                    assert!(
                        state.old_alive.load(Ordering::SeqCst),
                        "a pinned reader observed a reclaimed slot"
                    );
                }
                state.leave(epoch, 1);
            })
        };
        let writer = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                let epoch = state.enter(0);
                state.index.swap(ABSENT, Ordering::AcqRel);
                let tag = state.generation.load(Ordering::SeqCst);
                state.due.store(reclaim_at(tag), Ordering::Release);
                state.leave(epoch, 0);
            })
        };
        for _ in 0..EPOCHS {
            let generation = state.generation.load(Ordering::SeqCst);
            state.advance(generation, claimed(&state));
            state.reclaim_if_due();
        }
        reader.join().unwrap();
        writer.join().unwrap();
    });
}

#[test]
fn masked_advances_never_reclaim_under_a_reader_claiming_its_shard() {
    model_masked_reclamation(MaskedTokenModel::claimed);
}

#[test]
#[should_panic(expected = "a pinned reader observed a reclaimed slot")]
fn masked_advances_that_load_the_mask_can_skip_a_new_reader() {
    // Negative control: a plain load of the mask can miss a claim that a
    // read-modify-write observes.
    model_masked_reclamation(|state| state.shards.load(Ordering::SeqCst));
}

/// `ShardedCache`'s parked announcements, with one announcing reader. Its
/// pins reuse a parked epoch behind a light fence; an advance that the
/// parked epoch blocks evicts it behind a heavy fence.
///
/// Production's light fence is a compiler fence, and its heavy fence is
/// `membarrier`, which makes each running thread execute a full barrier at
/// some point of its program. The light fence orders with the heavy fence
/// only, not with the advance's ordinary fence, so the model does not issue
/// a fence for it. It holds `barrier` across a reuse instead, and the heavy
/// fence takes `barrier`: the reader's whole reuse then precedes the heavy
/// fence or follows it, the outcomes of a barrier before the reuse's store
/// or after its loads. A barrier between them only adds ordering.
struct AnnouncedModel {
    generation: AtomicUsize,
    /// The writer pins through a shared counter, as production threads
    /// without an announcer slot do.
    counters: [AtomicUsize; EPOCHS],
    word: AtomicUsize,
    evicted: AtomicUsize,
    index: AtomicUsize,
    old_alive: AtomicBool,
    due: AtomicUsize,
    barrier: Mutex<()>,
    light_fence: bool,
    heavy_fence: bool,
}

impl AnnouncedModel {
    fn new(word: usize, light_fence: bool, heavy_fence: bool) -> Self {
        Self {
            generation: AtomicUsize::new(0),
            counters: std::array::from_fn(|_| AtomicUsize::new(0)),
            word: AtomicUsize::new(word),
            evicted: AtomicUsize::new(0),
            index: AtomicUsize::new(OLD),
            old_alive: AtomicBool::new(true),
            due: AtomicUsize::new(NO_RETIRED_EPOCH),
            barrier: Mutex::new(()),
            light_fence,
            heavy_fence,
        }
    }

    fn pin(&self) {
        let barrier = self.light_fence.then(|| self.barrier.lock().unwrap());
        let reused = reuse_announcement(
            self.word.load(Ordering::Relaxed),
            |word| self.word.store(word, Ordering::Relaxed),
            || {},
            || self.evicted.load(Ordering::Relaxed),
            || self.generation.load(Ordering::Relaxed),
        );
        drop(barrier);
        if reused.is_none() {
            announce_afresh(
                || self.generation.load(Ordering::Relaxed),
                |word| self.word.store(word, Ordering::Release),
                || fence(Ordering::SeqCst),
                || self.evicted.load(Ordering::Relaxed),
                || self.evicted.store(0, Ordering::Relaxed),
            );
        }
    }

    fn unpin(&self) {
        let word = leave_announcement(self.word.load(Ordering::Relaxed));
        self.word.store(word, Ordering::Release);
    }

    fn enter_counter(&self) -> usize {
        enter_epoch(
            || self.generation.load(Ordering::SeqCst),
            |epoch| {
                self.counters[epoch].fetch_add(1, Ordering::SeqCst);
            },
            |epoch| self.counters[epoch].fetch_sub(1, Ordering::SeqCst),
        )
    }

    fn leave_counter(&self, epoch: usize) {
        leave_epoch(epoch, |epoch| {
            self.counters[epoch].fetch_sub(1, Ordering::SeqCst)
        });
    }

    /// As `Epochs::try_advance` for the one announcement.
    fn advance(&self) {
        let generation = self.generation.load(Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let mut evict = false;
        let Some(advance) = plan_epoch_advance(generation, |epoch| {
            if self.counters[epoch].fetch_add(0, Ordering::SeqCst) != 0 {
                return false;
            }
            match classify_announcement(
                self.word.load(Ordering::Acquire),
                || self.evicted.load(Ordering::Relaxed),
                epoch,
            ) {
                Announced::Clear => true,
                Announced::Blocks => false,
                Announced::Evict => {
                    evict = true;
                    true
                }
            }
        }) else {
            return;
        };
        if evict {
            self.evicted.store(EVICTING, Ordering::Relaxed);
            if self.heavy_fence {
                drop(self.barrier.lock().unwrap());
            }
            match eviction_outcome(self.word.load(Ordering::Acquire), epoch_slot(generation)) {
                None => return,
                Some(true) => self.evicted.store(EVICTED, Ordering::Relaxed),
                Some(false) => {}
            }
        }
        let _ = self.generation.compare_exchange(
            generation,
            advance.next_generation,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn reclaim_if_due(&self) {
        let due = self.due.load(Ordering::Acquire);
        if due != NO_RETIRED_EPOCH && self.generation.load(Ordering::SeqCst) >= due {
            self.old_alive.store(false, Ordering::SeqCst);
        }
    }
}

/// A reader pins `pins` times through its announcement, starting from
/// `word`, while a writer retires the old slot and the main thread advances
/// through a full aging and reclaims when due. Every pin that finds the old
/// slot published must find it alive.
fn model_announced_reclamation(word: usize, pins: usize, light_fence: bool, heavy_fence: bool) {
    model_bounded(move || {
        let state = Arc::new(AnnouncedModel::new(word, light_fence, heavy_fence));
        let reader = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                for _ in 0..pins {
                    state.pin();
                    if state.index.load(Ordering::Acquire) == OLD {
                        assert!(
                            state.old_alive.load(Ordering::SeqCst),
                            "a pinned reader observed a reclaimed slot"
                        );
                    }
                    state.unpin();
                }
            })
        };
        let writer = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                let epoch = state.enter_counter();
                state.index.swap(ABSENT, Ordering::AcqRel);
                let tag = state.generation.load(Ordering::SeqCst);
                state.due.store(reclaim_at(tag), Ordering::Release);
                state.leave_counter(epoch);
            })
        };
        for _ in 0..EPOCHS {
            state.advance();
            state.reclaim_if_due();
        }
        reader.join().unwrap();
        writer.join().unwrap();
    });
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
fn a_parked_announcement_reused_under_a_light_fence_survives_its_eviction() {
    model_announced_reclamation(ANNOUNCED, 1, true, true);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
fn an_announcement_made_afresh_and_then_reused_protects_both_pins() {
    model_announced_reclamation(0, 2, true, true);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "a pinned reader observed a reclaimed slot")]
fn reusing_a_parked_announcement_without_the_light_fence_can_miss_an_eviction() {
    // Negative control: the light fence orders the active mark before the
    // eviction check.
    model_announced_reclamation(ANNOUNCED, 1, false, true);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "a pinned reader observed a reclaimed slot")]
fn evicting_without_the_heavy_fence_can_reclaim_under_a_reused_announcement() {
    // Negative control: the heavy fence orders the eviction mark before the
    // advance reads the announcement again.
    model_announced_reclamation(ANNOUNCED, 1, true, false);
}

const BIASED: usize = 0;
const REVOKING: usize = 1;
const REVOKED: usize = 2;

/// A retired lane, as `RetiredQueue` in `sharded_arena`: its thread retires
/// into `ring` without `lock` while `bias` is `BIASED`, pinned through
/// `word`, and a drainer revokes the bias under `lock` before taking the
/// ring. `barrier` models the light and heavy fences as in `AnnouncedModel`:
/// the thread holds it from its pin's store to its load of `bias`, and the
/// heavy fence takes it, so it falls before the store or after the load. A
/// barrier between them only adds ordering.
struct LaneModel {
    word: AtomicUsize,
    bias: AtomicUsize,
    ring: loom::cell::UnsafeCell<usize>,
    lock: Mutex<()>,
    barrier: Mutex<()>,
    light_fence: bool,
    heavy_fence: bool,
}

#[allow(unsafe_code, reason = "the ring models a lane's unsynchronized state")]
impl LaneModel {
    fn new(light_fence: bool, heavy_fence: bool) -> Self {
        Self {
            word: AtomicUsize::new(ANNOUNCED),
            bias: AtomicUsize::new(BIASED),
            ring: loom::cell::UnsafeCell::new(0),
            lock: Mutex::new(()),
            barrier: Mutex::new(()),
            light_fence,
            heavy_fence,
        }
    }

    /// One write by the lane's thread: pin, retire into the ring, unpin.
    fn write(&self) {
        let barrier = self.light_fence.then(|| self.barrier.lock().unwrap());
        self.word
            .store(ANNOUNCED_PIN | ANNOUNCED, Ordering::Relaxed);
        let biased = self.bias.load(Ordering::Relaxed) == BIASED;
        drop(barrier);
        if biased {
            // SAFETY: the model checks that nothing else touches the ring.
            self.ring.with_mut(|ring| unsafe { *ring += 1 });
        } else {
            let _lock = self.lock.lock().unwrap();
            self.bias.store(BIASED, Ordering::Relaxed);
            // SAFETY: as above.
            self.ring.with_mut(|ring| unsafe { *ring += 1 });
        }
        self.word.store(ANNOUNCED, Ordering::Release);
    }

    /// One drain by another thread: revoke under the lock, then take the
    /// ring, unless the lane's thread is pinned.
    fn drain(&self) {
        let _lock = self.lock.lock().unwrap();
        if self.bias.load(Ordering::Relaxed) != REVOKED {
            self.bias.store(REVOKING, Ordering::Relaxed);
            if self.heavy_fence {
                drop(self.barrier.lock().unwrap());
            }
            if self.word.load(Ordering::Acquire) >= ANNOUNCED_PIN {
                return;
            }
            self.bias.store(REVOKED, Ordering::Relaxed);
        }
        // SAFETY: as in `write`.
        self.ring.with_mut(|ring| unsafe { *ring = 0 });
    }
}

/// The lane's thread writes `writes` times while the main thread drains
/// `drains` times; any access to the ring not ordered after the other's
/// fails the model.
fn model_lane(writes: usize, drains: usize, light_fence: bool, heavy_fence: bool) {
    model_bounded(move || {
        let lane = Arc::new(LaneModel::new(light_fence, heavy_fence));
        let owner = {
            let lane = Arc::clone(&lane);
            thread::spawn(move || {
                for _ in 0..writes {
                    lane.write();
                }
            })
        };
        for _ in 0..drains {
            lane.drain();
        }
        owner.join().unwrap();
    });
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
fn a_lanes_thread_and_its_drainers_never_reach_it_at_once() {
    model_lane(3, 3, true, true);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "Causality violation")]
fn draining_without_the_heavy_fence_can_race_the_lanes_thread() {
    // Negative control: the heavy fence orders the revocation before the
    // drainer reads the announcement.
    model_lane(1, 1, true, false);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "minutes unoptimized; CI runs it with --release"
)]
#[should_panic(expected = "Causality violation")]
fn retiring_without_the_light_fence_can_race_a_drainer() {
    // Negative control: the pin's fence orders its announcement before the
    // thread reads the bias.
    model_lane(1, 1, false, true);
}
