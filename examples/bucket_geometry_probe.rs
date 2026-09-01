//! Placement census for alternative atomic-overlay bucket geometries.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

const TARGET_NUMERATOR: usize = 23;
const TARGET_DENOMINATOR: usize = 20;

fn main() {
    let entries = std::env::args()
        .nth(1)
        .map_or(1_000_000, |value| value.parse().expect("expected entries"));
    println!(
        "entries,bucket_slots,choices,buckets,target_slots,placed,overflow,overflow_percent,average_probes,choice_1_percent,choice_2_percent,choice_3_percent,choice_4_percent,later_percent"
    );
    for bucket_slots in [4, 8, 10, 12, 16] {
        for choices in [3, 4, 6, 8] {
            census(entries, bucket_slots, choices);
        }
    }
}

fn census(entries: usize, bucket_slots: usize, choices: usize) {
    let target_slots = entries
        .saturating_mul(TARGET_NUMERATOR)
        .div_ceil(TARGET_DENOMINATOR);
    let buckets = target_slots.div_ceil(bucket_slots).max(choices);
    let mut occupied = vec![0_u8; buckets];
    let mut placement = [0_usize; 9];
    let mut probes = 0_usize;
    let mut overflow = 0_usize;
    for key in 0..entries {
        let hash = mix(key as u64 + 1);
        let candidates = bucket_choices(hash, buckets, choices);
        let mut placed = false;
        for (attempt, bucket) in candidates.into_iter().enumerate() {
            probes += 1;
            if usize::from(occupied[bucket]) < bucket_slots {
                occupied[bucket] += 1;
                placement[attempt] += 1;
                placed = true;
                break;
            }
        }
        if !placed {
            overflow += 1;
        }
    }
    let placed = entries - overflow;
    let percentage = |count: usize| count as f64 * 100.0 / entries as f64;
    let later = if choices > 4 {
        placement[4..choices].iter().sum()
    } else {
        0
    };
    println!(
        "{entries},{bucket_slots},{choices},{buckets},{},{placed},{overflow},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4}",
        buckets * bucket_slots,
        percentage(overflow),
        probes as f64 / entries as f64,
        percentage(placement[0]),
        percentage(placement[1]),
        percentage(placement[2]),
        percentage(placement[3]),
        percentage(later),
    );
}

fn bucket_choices(hash: u64, buckets: usize, choices: usize) -> Vec<usize> {
    let mut result = Vec::with_capacity(choices);
    for attempt in 0..choices {
        let variant = match attempt {
            0 => hash,
            1 => hash.rotate_right(29) ^ 0x9e37_79b9_7f4a_7c15,
            2 => hash.rotate_left(17) ^ 0xd6e8_feb8_6659_fd93,
            3 => hash.rotate_right(7) ^ 0xa076_1d64_78bd_642f,
            _ => mix(hash ^ (attempt as u64).wrapping_mul(0xe703_7ed1_a0b4_28db)),
        };
        let mut candidate = reduce(variant, buckets);
        while result.contains(&candidate) {
            candidate = if candidate + 1 == buckets {
                0
            } else {
                candidate + 1
            };
        }
        result.push(candidate);
    }
    result
}

fn reduce(hash: u64, upper: usize) -> usize {
    ((u128::from(hash) * upper as u128) >> 64) as usize
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
