#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iter = Self {
            table: table.clone(),
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block(0)?),
            blk_idx: 0,
        };
        iter.seek_to_first()?;
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.blk_idx != 0 {
            let blk = self.table.read_block(0)?;
            let blk_iter = BlockIterator::create_and_seek_to_first(blk);
            self.blk_idx = 0;
            self.blk_iter = blk_iter;
        } else {
            self.blk_iter.seek_to_first();
        }

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self::create_and_seek_to_first(table)?;
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // `partition_point` returns [0, len-1]. It assumes that the predicate
        // evaluates to true on the prefix, and evaluates to false on the suffix.
        // [ true, true, ..., true, false, false, ..., false ].
        // It returns the index to the first element that evaluates to false.
        //
        // When predicat is "first_key <= key", the returned `idx` is the first
        // element s.t. "first_key > key". So, we should start searching at
        // max(0, `idx-1`).
        let idx = self
            .table
            .block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key);
        let idx = if idx > 0 { idx - 1 } else { idx };

        // May exist in idx, idx+1, ...
        self.blk_idx = idx;
        self.blk_iter =
            BlockIterator::create_and_seek_to_key(self.table.read_block(self.blk_idx)?, key);

        while !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.block_meta.len() {
            self.blk_idx += 1;
            self.blk_iter =
                BlockIterator::create_and_seek_to_key(self.table.read_block(self.blk_idx)?, key);
        }

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        while !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.blk_idx += 1;
            let blk = self.table.read_block(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(blk);
        }

        Ok(())
    }
}
