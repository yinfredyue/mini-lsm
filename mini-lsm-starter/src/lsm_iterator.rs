#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{anyhow, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner) -> Result<Self> {
        while iter.is_valid() && iter.value().is_empty() {
            // A deletion
            println!("{:?} was deleted, continue", iter.key());
            iter.next()?;
        }

        Ok(Self { inner: iter })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.inner.is_valid() {
            return Err(anyhow!("next() called on invalid LsmIterator"));
        }

        self.inner.next()?;

        while self.inner.is_valid() && self.value().is_empty() {
            // A deletion
            println!("{:?} was deleted, continue", self.key());
            self.inner.next()?;
        }

        Ok(())
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

        return res;
    }
}
