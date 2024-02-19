use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        assert!(!sstables.is_empty());
        let current = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        assert!(current.is_valid());
        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut idx = {
            // `idx` is the first SST s.t. first_key > key.
            let idx = sstables.partition_point(|sst| sst.first_key().as_key_slice() <= key);
            if idx == 0 {
                idx
            } else {
                idx - 1
            }
        };

        let mut current = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;
        while !current.is_valid() && idx + 1 < sstables.len() {
            idx += 1;
            current = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;
        }

        Ok(Self {
            current: Some(current),
            next_sst_idx: idx + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map_or(false, |iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let mut current = self.current.take().unwrap();

        current.next()?;

        while !current.is_valid() && self.next_sst_idx < self.sstables.len() {
            current = SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?;
            self.next_sst_idx += 1;
        }

        self.current = Some(current);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
