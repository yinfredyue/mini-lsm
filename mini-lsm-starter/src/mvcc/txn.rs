use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound_u8_to_bytes,
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// read set, and write set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("get() on committed txn"));
        }

        self.add_to_readset(key);

        if let Some(entry) = self.local_storage.get(&Bytes::copy_from_slice(key)) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("scan() on committed txn"));
        }

        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        // initialize TxnLocalIterator to point to a valid item, if any
        let txn_local_iter = {
            let lower = map_bound_u8_to_bytes(lower);
            let upper = map_bound_u8_to_bytes(upper);

            let (item, should_advance) = {
                let mut range = self.local_storage.range((lower.clone(), upper.clone()));
                if let Some(entry) = range.next() {
                    ((entry.key().clone(), entry.value().clone()), true)
                } else {
                    ((Bytes::new(), Bytes::new()), false)
                }
            };

            let iter = TxnLocalIteratorBuilder {
                map: self.local_storage.clone(),
                item,
                iter_builder: |map: &Arc<SkipMap<Bytes, Bytes>>| {
                    let mut iter = map.range((lower, upper));
                    if should_advance {
                        iter.next();
                    }
                    iter
                },
            }
            .build();

            iter
        };

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(txn_local_iter, lsm_iter)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("put() on committed txn");
        }

        self.add_to_writeset(key);

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("delete() on committed txn");
        }

        self.add_to_writeset(key);

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("commit() on committed txn"));
        }

        // Detect conflcits in optimistic concurrency control
        let mvcc = self.inner.mvcc();
        let _commit_lock = mvcc.commit_lock.lock();
        let commit_ts = mvcc.latest_commit_ts() + 1;
        let mut committed_txns = mvcc.committed_txns.lock();
        let mut conflict_detected = false;

        println!(
            "Committing txn: read_ts: {}, commit_ts: {}",
            self.read_ts, commit_ts
        );

        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let key_hashes = key_hashes.lock();

            // Only check write txns - read-only txns can always commit
            if !key_hashes.1.is_empty() {
                // Check if the readset overlaps with writeset of other txns
                let overlapping_txns = committed_txns
                    .range((Bound::Excluded(self.read_ts), Bound::Excluded(commit_ts)));

                for (_ts, txn) in overlapping_txns {
                    if !txn.key_hashes.is_disjoint(&key_hashes.0) {
                        conflict_detected = true;
                        break;
                    }
                }
            }
        }

        if conflict_detected {
            println!("Conflicts detected, cannot commit");
            return Err(anyhow!("Conlicts detected, cannot commit"));
        }

        self.committed
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let records: Vec<WriteBatchRecord<Bytes>> = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect();

        self.inner.write_batch(&records[..])?;

        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let key_hashes = key_hashes.lock();

            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: key_hashes.1.clone(),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );
        }

        println!("Committed successfully, commit_ts = {}", commit_ts);

        Ok(())
    }

    fn add_to_readset(&self, key: &[u8]) {
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut hashes = key_hashes.lock();
            hashes.0.insert(farmhash::hash32(key));
        }
    }

    fn add_to_writeset(&self, key: &[u8]) {
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut hashes = key_hashes.lock();
            hashes.1.insert(farmhash::hash32(key));
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts = self.inner.mvcc().ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|item| item.1.as_ref())
    }

    fn key(&self) -> &[u8] {
        self.with_item(|item| item.0.as_ref())
    }

    fn is_valid(&self) -> bool {
        println!(
            "TxnLocalIterator is_valid: {}",
            self.with_item(|item| !item.0.is_empty())
        );

        self.with_item(|item| !item.0.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        let new_item = self.with_iter_mut(|iter| {
            if let Some(entry) = iter.next() {
                (entry.key().clone(), entry.value().clone())
            } else {
                (Bytes::new(), Bytes::new())
            }
        });
        println!("TxnLocalIterator next() -> {:?}", new_item);
        self.with_item_mut(|item| *item = new_item);

        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let txn_iter = Self { _txn: txn, iter };
        Ok(txn_iter)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        while self.is_valid() {
            if self.is_valid() {
                // Record key into readset
                self._txn.add_to_readset(self.iter.key());
                println!("scan(): add {:?} to readset", self.iter.key());
            }

            self.iter.next()?;

            if self.is_valid() && !self.value().is_empty() {
                break;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        println!("TxnIterator: num_active_iterators");
        self.iter.num_active_iterators()
    }
}
