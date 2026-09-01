//! Exhaustive small-state models for the direct cache publication and QSBR protocol.

#[path = "../src/direct_epoch_protocol.rs"]
mod direct_epoch_protocol;

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

use direct_epoch_protocol::{EPOCHS, enter_epoch, leave_epoch, plan_epoch_advance};

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
