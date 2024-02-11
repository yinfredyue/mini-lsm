#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn decode_entry(data: Vec<u8>, start_offset: usize) -> (KeyVec, (usize, usize)) {
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let key = KeyVec::from_vec(data[2..(key_len + 2)].to_vec());
    let value_range = (start_offset + key_len + 4, start_offset + data.len());

    (key, value_range)
}

impl BlockIterator {
    // The returned iterator is not fully initialized. You should initialize
    // `key`, `value_range`, and `idx` youself.
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };

        let first_entry_len = iter.first_entry_len();
        let (first_key, _) = decode_entry(iter.block.data[..first_entry_len].to_vec(), 0);
        iter.first_key = first_key;

        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }

        if self.idx == self.block.offsets.len() - 1 {
            self.key = KeyVec::new();
            return;
        }

        self.seek_to_idx(self.idx + 1)
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // TODO: Use binary search
        if self.key.as_key_slice().ge(&key) {
            self.seek_to_first()
        }

        let mut found = false;
        for idx in 0..self.block.offsets.len() {
            self.seek_to_idx(idx);

            if self.key.as_key_slice().ge(&key) {
                found = true;
                break;
            }
        }

        if !found {
            self.next()
        }
    }

    fn seek_to_idx(&mut self, idx: usize) {
        assert!(idx < self.block.offsets.len());

        let entry_start_offset = self.block.offsets[idx] as usize;
        let entry_end_offset = if idx == self.block.offsets.len() - 1 {
            self.block.data.len()
        } else {
            self.block.offsets[idx + 1] as usize
        };
        let (key, value_range) = decode_entry(
            self.block.data[entry_start_offset..entry_end_offset].to_vec(),
            entry_start_offset,
        );

        self.idx = idx;
        self.key = key;
        self.value_range = value_range;
    }

    fn first_entry_len(&self) -> usize {
        let num_elements = self.block.offsets.len();

        assert!(num_elements > 0);
        assert!(self.block.offsets[0] == 0);

        if num_elements == 1 {
            self.block.data.len()
        } else {
            self.block.offsets[1] as usize
        }
    }
}
