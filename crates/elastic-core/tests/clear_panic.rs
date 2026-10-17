use opthash::{ElasticHashMap, FunnelHashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Bomb {
    key: u64,
    armed: Arc<AtomicBool>,
}

impl Drop for Bomb {
    fn drop(&mut self) {
        if self.key == 7 && self.armed.swap(false, Ordering::SeqCst) {
            panic!("drop bomb");
        }
    }
}

macro_rules! clear_panic_test {
    ($name:ident, $map:ident, $must_empty:expr) => {
        #[test]
        fn $name() {
            let armed = Arc::new(AtomicBool::new(false));
            let mut map = $map::with_capacity(5000);
            for key in 0..5000 {
                map.insert(
                    key,
                    Bomb {
                        key,
                        armed: Arc::clone(&armed),
                    },
                );
            }
            armed.store(true, Ordering::SeqCst);
            assert!(catch_unwind(AssertUnwindSafe(|| map.clear())).is_err());
            if $must_empty {
                assert!(map.is_empty(), "unwinding clear must leave Funnel empty");
            }
            let keys: Vec<_> = map.keys().copied().collect();
            assert_eq!(keys.len(), map.len());
            for key in &keys {
                assert!(map.get(key).is_some(), "survivor {key} is unreachable");
            }
            if let Some(&key) = keys.first() {
                map.insert(
                    key,
                    Bomb {
                        key,
                        armed: Arc::clone(&armed),
                    },
                );
                assert_eq!(map.keys().filter(|&&k| k == key).count(), 1);
            }
            map.insert(6000, Bomb { key: 6000, armed });
            assert!(map.get(&6000).is_some());
        }
    };
}

clear_panic_test!(funnel_clear_panic_is_consistent, FunnelHashMap, true);
clear_panic_test!(elastic_clear_panic_is_consistent, ElasticHashMap, false);
