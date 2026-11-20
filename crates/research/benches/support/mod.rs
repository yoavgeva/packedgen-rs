#![allow(dead_code)]

pub const LARGE_ENTRIES: usize = 1 << 18;

#[inline]
pub fn scramble(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub fn binary_key(index: u64, bytes: usize) -> Box<[u8]> {
    let mut key = vec![0_u8; bytes];
    let mut state = index;
    for chunk in key.chunks_mut(8) {
        state = scramble(state);
        let encoded = state.to_le_bytes();
        chunk.copy_from_slice(&encoded[..chunk.len()]);
    }
    key.into_boxed_slice()
}
