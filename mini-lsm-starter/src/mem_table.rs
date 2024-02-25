#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        // Assume the WAL is empty.
        let mut memtable = Self::create(id);
        memtable.wal = match Wal::create(path) {
            Ok(wal) => Some(wal),
            Err(err) => return Err(err),
        };

        Ok(memtable)
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skiplist: SkipMap<Bytes, Bytes> = SkipMap::new();
        let wal = Wal::recover(path, &skiplist)?;

        Ok(Self {
            map: Arc::new(skiplist),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|entry| Bytes::clone(entry.value()))
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let total_size = key.len() + value.len();
        self.approximate_size
            .clone()
            .fetch_add(total_size, std::sync::atomic::Ordering::SeqCst);

        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let lower = map_bound(lower);
        let upper = map_bound(upper);

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),

            // HACK: MemTableIterator checks item.0.is_empty() to see if the
            // iterator is valid. next() returns error if the iterator is
            // invalid before move. To ensure the immediate next() call
            // runs, item.0 has to be nonempty.
            item: (Bytes::copy_from_slice("INIT".as_bytes()), Bytes::new()),

            // `map.range(&self, range: RangeBounds<Q>)`
            // The RangeBounds<T> trait is implemented for (Bound<Byte>, Bound<Byte>)
            iter_builder: |map: &Arc<SkipMap<Bytes, Bytes>>| map.range((lower, upper)),
        }
        .build();

        iter.next().unwrap();

        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(KeySlice::from_slice(entry.key()), entry.value());
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    /// When item.0 is empty, the iterator is invalid
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.borrow_item().0)
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        // Returns error when:
        // - Called when is_valid() returns true
        // - Error when trying to move error
        // Won't return error when:
        // - After moving, is_valid() returns true
        if !self.is_valid() {
            return Err(anyhow!("next() called on invalid MemTableIterator"));
        }

        let new_item = match self.with_iter_mut(|iter| iter.next()) {
            None => (Bytes::new(), Bytes::new()),
            Some(entry) => (entry.key().clone(), entry.value().clone()),
        };

        self.with_item_mut(|item| *item = new_item);
        Ok(())
    }
}
impl std::fmt::Debug for MemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MemTable[id={:?}, map=[min_key={:?}, max_key={:?}, num_keys={:?}]]",
            self.id,
            self.map.front(),
            self.map.back(),
            self.map.len(),
        )
    }
}
