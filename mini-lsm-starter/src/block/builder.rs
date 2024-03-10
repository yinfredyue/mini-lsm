use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

// Without ts:
// [ key_len(2) | key | value_len(2) | value ]. Offset (u16) takes 2 bytes.
// The overhead is 2 + 2 + 2 = 6 bytes per key-value pair.
// With ts:
// [ key_len(2) | key | timestamp | value_len(2) | value ].
const OVERHEAD: usize = 6;

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn will_exceed_size_limit(&self, key: KeySlice, value: &[u8]) -> bool {
        self.data.len() + self.offsets.len() + key.raw_len() + value.len() + OVERHEAD
            >= self.block_size
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // Assumes that `add` is given keys in sorted order and we don't need
        // to perform ordering ourselves.

        // Don't check size limit for the first key-value pair
        if !self.data.is_empty() && self.will_exceed_size_limit(key, value) {
            return false;
        }

        let offset = self.data.len() as u16;
        self.offsets.push(offset);

        if self.data.is_empty() {
            KeyVec::set_from_slice(&mut self.first_key, key);
        }

        let mut ts = key.ts().to_be_bytes().to_vec();
        let mut key_len = (key.key_len() as u16).to_be_bytes().to_vec();
        let mut key = key.key_ref().to_vec();
        let mut value_len = (value.len() as u16).to_be_bytes().to_vec();
        let mut value = value.to_vec();
        self.data.append(&mut key_len);
        self.data.append(&mut key);
        self.data.append(&mut ts);
        self.data.append(&mut value_len);
        self.data.append(&mut value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
