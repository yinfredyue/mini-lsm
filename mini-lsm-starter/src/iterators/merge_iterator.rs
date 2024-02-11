use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    // Invariant: iters and current only contain valid iterators.
    // This is necessary HeapWrapper::partial_cmp assumes the iter is valid - it
    // calls key() without checking is_valid()
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters_heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                iters_heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = iters_heap.pop();
        Self {
            iters: iters_heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(anyhow::anyhow!("next() called on invalid MergeIterator"));
        }

        // A memtable is not aware of deletion - it only knows about get() and put().
        // Similar, a merge_iterator just merges a few MemtableIterators.
        // Deletion is handled at the LSM level.
        let HeapWrapper(prev_idx, mut prev_iter) = self.current.take().unwrap();

        // Call next() on any iterator that points to the current_key.
        // If an iterator is no longer valid after calling next(), discard it.
        while let Some(mut iter) = self.iters.pop() {
            if iter.1.key().eq(&prev_iter.key()) {
                iter.1.next()?;
                if iter.1.is_valid() {
                    self.iters.push(iter);
                }
            } else {
                self.iters.push(iter);
                break;
            }
        }

        assert!(prev_iter.is_valid());
        prev_iter.next()?;
        if prev_iter.is_valid() {
            self.iters.push(HeapWrapper(prev_idx, prev_iter))
        }

        self.current = self.iters.pop();

        Ok(())
    }
}
