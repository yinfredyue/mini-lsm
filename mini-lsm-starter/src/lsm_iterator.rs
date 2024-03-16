use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
/// TwoMergeIterator<
///   TwoMergeIterator<
///     MergeIterator<MemTableIterator>,  <- memtables
///     MergeIterator<SsTableIterator>    <- L0 SSTs
///   >,
///   MergeIterator<SstConcatIterator>    <- L1, L2, ..., Ln SSTs
/// >
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,

    // W1D5: SsTableIterator doesn't support upper bound. But LsmStorage
    // supports scan(lower, upper). Thus, LsmIterator has to handle the upper
    // bound.
    upper: Bound<Bytes>,
    prev_key: Option<Vec<u8>>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            upper,
            prev_key: None,
        };

        iter.next()?;
        Ok(iter)
    }

    // Returns if the iterator has crossed `upper` and thus is not valid
    fn crossed_upper(&self) -> bool {
        match &self.upper {
            Bound::Included(upper) => self.key() > upper,
            Bound::Excluded(upper) => self.key() >= upper,
            Bound::Unbounded => false,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.crossed_upper()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        while self.inner.is_valid() {
            // Skip if:
            // 1. Same key as before (we have returned the latest version)
            // 2. Value is empty (has been deleted)
            // 3. The key was written after the transaction is created
            let (should_skip, should_update_prev_key) =
                if self.key() == self.prev_key.as_ref().unwrap_or(&vec![]) {
                    (true, false)
                } else if self.value().is_empty() {
                    (true, true)
                } else {
                    (false, true)
                };

            if should_update_prev_key {
                self.prev_key = Some(self.key().to_vec());
            }

            if should_skip {
                self.inner.next()?
            } else {
                break;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
///
/// Error implies invalidness. But not the other way around.
/// Case 1: The iterator becomes invalid even if `next` has never returned an error.
/// In this case, next() should do nothing and keeps returning ok.
/// Case 2: After `next` has returned an error, the iterator becomes invalid and all
/// subsequent `next` should return error.
///
/// Examples:
/// Case 1. After `next` returns ok, the underlying iterator becomes invalid. The
/// fused-iterator becomes invalid, even though no error has been returned.
/// Case 2. The underlying iterator returns an error when calling `next`.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored {
            panic!("Has errored")
        } else {
            self.iter.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.has_errored {
            panic!("Has errored")
        } else {
            self.iter.value()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow!("next() called on errored FusedIterator"));
        }

        if !self.is_valid() {
            return Ok(());
        }

        let res = self.iter.next();
        if res.is_err() {
            self.has_errored = true;
        }

        res
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
