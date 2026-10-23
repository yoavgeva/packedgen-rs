//! Cost of membarrier(PRIVATE_EXPEDITED) with N other threads busy, and the
//! slowdown it inflicts on them.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

unsafe extern "C" {
    fn syscall(number: i64, ...) -> i64;
}
#[cfg(target_arch = "x86_64")]
const SYS_MEMBARRIER: i64 = 324;
#[cfg(target_arch = "aarch64")]
const SYS_MEMBARRIER: i64 = 283;
const CMD_QUERY: i64 = 0;
const CMD_PRIVATE_EXPEDITED: i64 = 1 << 3;
const CMD_REGISTER_PRIVATE_EXPEDITED: i64 = 1 << 4;

fn membarrier(cmd: i64) -> i64 {
    unsafe { syscall(SYS_MEMBARRIER, cmd, 0i64, 0i64) }
}

fn main() {
    let supported = membarrier(CMD_QUERY);
    println!("query: {supported:#x}");
    println!("register: {}", membarrier(CMD_REGISTER_PRIVATE_EXPEDITED));
    let cpus = std::thread::available_parallelism().unwrap().get();
    for busy in 0..cpus {
        for interval_us in [0u64, 10, 50] {
            let stop = Arc::new(AtomicBool::new(false));
            let counts: Vec<Arc<AtomicU64>> = (0..busy).map(|_| Arc::new(AtomicU64::new(0))).collect();
            let workers: Vec<_> = counts
                .iter()
                .map(|count| {
                    let stop = Arc::clone(&stop);
                    let count = Arc::clone(count);
                    std::thread::spawn(move || {
                        let mut local = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            for _ in 0..1000 {
                                local = local.wrapping_add(1);
                                std::hint::black_box(local);
                            }
                            count.store(local, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            std::thread::sleep(Duration::from_millis(50));
            // Baseline worker rate without barriers.
            let before: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
            std::thread::sleep(Duration::from_millis(200));
            let base_rate = (counts.iter().map(|c| c.load(Ordering::Relaxed)).sum::<u64>() - before) as f64 / 0.2;
            let before: u64 = counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
            let start = Instant::now();
            let mut calls = 0u64;
            let mut spent = Duration::ZERO;
            while start.elapsed() < Duration::from_millis(200) {
                let t = Instant::now();
                assert_eq!(membarrier(CMD_PRIVATE_EXPEDITED), 0);
                spent += t.elapsed();
                calls += 1;
                if interval_us > 0 {
                    let until = Instant::now() + Duration::from_micros(interval_us);
                    while Instant::now() < until {}
                }
            }
            let wall = start.elapsed().as_secs_f64();
            let rate = (counts.iter().map(|c| c.load(Ordering::Relaxed)).sum::<u64>() - before) as f64 / wall;
            stop.store(true, Ordering::Relaxed);
            for w in workers {
                w.join().unwrap();
            }
            println!(
                "busy={busy} interval={interval_us}us calls={calls} per_call={:.2}us others_slowdown={:.1}%",
                spent.as_secs_f64() * 1e6 / calls as f64,
                if busy > 0 { (1.0 - rate / base_rate) * 100.0 } else { 0.0 }
            );
        }
    }
}
