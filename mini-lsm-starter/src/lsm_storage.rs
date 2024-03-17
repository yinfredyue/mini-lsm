#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{
    map_bound_u8_to_bytes, map_lower_bound_u8_to_keyslice, map_upper_bound_u8_to_keyslice,
    MemTable, MemTableIterator,
};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

type AllMergedIterator = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

/// Represents the state of the storage engine.
#[derive(Clone, Debug)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    /// It's an ordered list of (id, sst_ids), representing levels from top to
    /// bottom. The first element is usually L0. When using tiered compaction,
    /// the first element is the first sorted run.
    /// Note that there's no ordering guarantee on levels' ids.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        println!("close");

        if !self.inner.clone().options.enable_wal {
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }

            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        self.flush_notifier.send(())?;
        println!("Waiting for flush thread...");
        if let Some(join_hanlde) = self.flush_thread.lock().take() {
            join_hanlde
                .join()
                .map_err(|_| anyhow!("Failed to join flush thread"))?
        }

        self.compaction_notifier.send(())?;
        println!("Waiting for compaction thread...");
        if let Some(join_hanlde) = self.compaction_thread.lock().take() {
            join_hanlde
                .join()
                .map_err(|_| anyhow!("Failed to join flush thread"))?
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        println!("open({:?})", path.as_ref());

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        std::fs::create_dir_all(path.as_ref())?;

        let manifest_path = path.as_ref().join("manifest");
        let (manifest, records) = if !Path::exists(&manifest_path) {
            (Manifest::create(manifest_path)?, vec![])
        } else {
            Manifest::recover(manifest_path)?
        };

        let mut state = LsmStorageState::create(&options);
        if options.enable_wal {
            // Ideally we should do this in LsmStorageState::create, but that
            // requires adding a `path` argument to LsmStorageState::create
            assert_eq!(state.memtable.id(), 0);
            let wal_path = Self::path_of_wal_static(path.as_ref(), 0);
            let memtable = if Path::exists(&wal_path) {
                MemTable::recover_from_wal(0, wal_path)?
            } else {
                MemTable::create_with_wal(0, wal_path)?
            };
            state.memtable = Arc::new(memtable);
        }

        // Rebuild LSM structure from manifest and wal
        println!("Rebuilding from manifest and WAL...");
        for record in records {
            let create_dummy_sst = |sst_id| {
                let empty = KeyBytes::default();
                Arc::new(SsTable::create_meta_only(sst_id, 0, empty.clone(), empty))
            };

            match record {
                ManifestRecord::Flush(sst_id) => {
                    println!("Flush({})", sst_id);

                    assert_eq!(state.imm_memtables.pop().unwrap().id(), sst_id);

                    if compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, sst_id);
                    } else {
                        state.levels.insert(0, (sst_id, vec![sst_id]));
                    }

                    state.sstables.insert(sst_id, create_dummy_sst(sst_id));
                }
                ManifestRecord::NewMemtable(sst_id) => {
                    println!("NewMemTable({})", sst_id);

                    state.imm_memtables.insert(0, state.memtable.clone());
                    let memtable = if options.enable_wal {
                        let wal_path = Self::path_of_wal_static(path.as_ref(), sst_id);
                        MemTable::recover_from_wal(sst_id, wal_path).unwrap()
                    } else {
                        MemTable::create(sst_id)
                    };
                    state.memtable = Arc::new(memtable);
                }
                ManifestRecord::Compaction(task, output_ssts) => {
                    println!("Compaction({:?}, output_ssts={:?})", task, output_ssts);

                    for &sst_id in output_ssts.iter() {
                        state.sstables.insert(sst_id, create_dummy_sst(sst_id));
                    }

                    let to_remove;
                    (state, to_remove) =
                        compaction_controller.apply_compaction_result(&state, &task, &output_ssts);

                    for sst_id in to_remove {
                        state.sstables.remove(&sst_id);
                    }
                }
            }
        }

        // Read back data from ssts on disk
        state.sstables = state
            .sstables
            .iter()
            .map(|(&sst_id, _)| {
                let sst_path = Self::path_of_sst_static(&path, sst_id);
                let sst_file = FileObject::open(sst_path.as_path()).unwrap();
                let sst = SsTable::open(sst_id, None, sst_file).unwrap();
                (sst_id, Arc::new(sst))
            })
            .collect();

        println!("DEBUG: Restored from manifest: {:?}", state);
        for (_, sst) in state.sstables.iter() {
            println!("{:?}", sst);
            let mut iter = SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap();
            while iter.is_valid() {
                let key = iter.key().for_testing_key_ref().to_vec();
                let value = iter.value().to_vec();
                println!(
                    "key={:?}, value={:?}",
                    String::from_utf8(key).unwrap(),
                    String::from_utf8(value).unwrap()
                );
                iter.next().unwrap();
            }
        }

        // Restore existing max sst_id
        let existing_max_sst_id = vec![
            state.memtable.id(),
            state
                .imm_memtables
                .iter()
                .map(|tbl| tbl.id())
                .max()
                .unwrap_or(0),
            state.sstables.keys().copied().max().unwrap_or(0),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);

        // Restore existing max_ts
        let existing_max_ts = {
            // `state.memtable` is restored from WAL if `options.enable_wal`
            let max_ts_from_memtables = {
                let mut max_ts = TS_DEFAULT;

                let mut all_memtables = state.imm_memtables.clone();
                all_memtables.insert(0, state.memtable.clone());
                for tbl in all_memtables {
                    let mut iter = tbl.scan(Bound::Unbounded, Bound::Unbounded);
                    while iter.is_valid() {
                        max_ts = max_ts.max(iter.key().ts());
                        iter.next()?;
                    }
                }

                max_ts
            };

            let max_ts_from_ssts = state
                .sstables
                .values()
                .map(|sst| sst.max_ts())
                .max()
                .unwrap_or(TS_DEFAULT);

            println!(
                "max_ts_from_memtables: {}, max_ts_from_ssts: {}",
                max_ts_from_memtables, max_ts_from_ssts
            );
            max_ts_from_memtables.max(max_ts_from_ssts)
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.as_ref().to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(existing_max_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(existing_max_ts + 1)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        println!("get({:?})", key);
        let txn = self.new_txn()?;
        txn.get(key)
    }

    pub fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        println!("get_with_ts({:?}, {:?})", key, read_ts);
        let lower = Bound::Included(KeySlice::from_slice(key, read_ts));
        let upper = Bound::Included(KeySlice::from_slice(key, TS_RANGE_END));
        let iter = self.get_scan_iter(lower, upper)?;

        if !iter.is_valid() || iter.key().key_ref() != key || iter.value().is_empty() {
            return Ok(None);
        }

        Ok(Some(Bytes::copy_from_slice(iter.value())))
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    fn may_freeze(&self, memtable: Arc<MemTable>) -> Result<()> {
        // We write-lock self.state after entering self.force_freeze_memtable,
        // so we need to prevent two threads execute it concurrently.
        // It's important to check size again after acquiring self.state_lock.
        if memtable.approximate_size() > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();

            if memtable.approximate_size() > self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?
            }
        }

        Ok(())
    }

    pub fn write_core(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mvcc = self.mvcc.as_ref().unwrap();
        let _mvcc_lock_guard = mvcc.write_lock.lock();
        let ts = mvcc.latest_commit_ts() + 1;
        println!(
            "Write({}, {}, ts={})",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
            ts
        );

        let state = self.state.read();
        let memtable = state.memtable.clone();
        memtable.put(KeySlice::from_slice(key, ts), value)?;
        drop(state);

        mvcc.update_commit_ts(ts);
        self.may_freeze(memtable)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_core(key, value)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_core(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        std::fs::File::open(&self.path)?
            .sync_all()
            .map_err(anyhow::Error::from)
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let sst_id = self.next_sst_id();
        let wal_path = self.path_of_wal(sst_id);
        let new_memtable = MemTable::create_with_wal(sst_id, wal_path)?;
        if let Some(manifest) = &self.manifest {
            manifest.add_record(_state_lock_observer, ManifestRecord::NewMemtable(sst_id))?;
        };

        {
            // You cannot modify Arc<T>, unless there's interior mutability,
            // like Arc<Mutex<T>> and Arc<RwLock<T>>.
            // Because `state` is Arc<RwLock<Arc<LsmStorageState>>>, we cannot
            // modify the innermost LsmStorageState. We can only modify the
            // Arc<LsmStorageState> wrapped by the RwLock.
            let mut state = self.state.write();

            let mut new_state = state.as_ref().clone();
            new_state
                .imm_memtables
                .insert(0, new_state.memtable.clone());
            new_state.memtable = Arc::new(new_memtable);

            *state = Arc::new(new_state);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // 1. Hold self.state_lock s.t. force_flush_xxx and force_freeze_xxx
        // cannot execute concurrently.
        // 2. Read-lock self.state when deciding which memtable to flush, and
        // release after that.
        // 3. Doesn't hold any lock when flushing!
        // 4. After done flushing, acquire write lock to update self.imm_memtables
        // , self.l0_sstables, and self.sstables
        //
        // Q: Is it ok to read-lock, read self.imm_memtables.last(), release
        // lock, and self.imm_memtables.pop()?
        // A: Self.imm_memtables are only written by force_flush_xx and
        // force_freeze_xx. Because we acquired self.state_lock, there's no
        // concurrently writes. Also, we acquire read-lock and write-lock
        // appropriately, so this is safe.
        let _state_lock = self.state_lock.lock();

        let memtable_to_flush;
        {
            let guard = self.state.read();

            match guard.imm_memtables.last() {
                Some(memtable) => {
                    memtable_to_flush = memtable.clone();
                }
                None => return Ok(()),
            }
        }

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sst_builder)?;
        let sst_id = memtable_to_flush.id();
        let sstable = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        // Sync directory
        // Write and sync manifest
        self.sync_dir()?;
        if let Some(manifest) = &self.manifest {
            manifest.add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        };

        {
            let mut guard = self.state.write();

            let mut new_state = guard.as_ref().clone();
            new_state.imm_memtables.pop().unwrap();
            if self.compaction_controller.flush_to_l0() {
                // Flush to L0
                new_state.l0_sstables.insert(0, sst_id);
            } else {
                // Insert a new sorted run
                new_state.levels.insert(0, (sst_id, vec![sst_id]));
            }
            new_state.sstables.insert(sst_id, Arc::new(sstable));
            *guard = Arc::new(new_state);
        }

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc.as_ref().unwrap().new_txn(self.clone(), false))
    }

    fn get_memtable_iter(
        &self,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> MergeIterator<MemTableIterator> {
        let state = self.state.read();
        let mut memtables = state.imm_memtables.clone();
        memtables.insert(0, state.memtable.clone());
        let memtable_iters: Vec<Box<MemTableIterator>> = memtables
            .iter()
            .map(|t| Box::new(t.scan(lower, upper)))
            .collect();

        println!("memtables: {:?}", memtables);

        MergeIterator::create(memtable_iters)
    }

    // SsTableIterator doesn't support `upper`. The `upper` is used for filtering.
    fn get_l0_ssts(
        &self,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let state = self.state.read();

        let sstables: Vec<Arc<SsTable>> = state
            .l0_sstables
            .iter()
            .map(|idx| {
                state
                    .sstables
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("sstable not found"))
            })
            .collect::<Result<_>>()?;

        let sstables = sstables
            .into_iter()
            .filter(|sst| match lower {
                Bound::Included(lower) => lower <= sst.last_key().as_key_slice(),
                Bound::Excluded(lower) => lower < sst.last_key().as_key_slice(),
                Bound::Unbounded => true,
            })
            .filter(|sst| match upper {
                Bound::Included(upper) => upper >= sst.first_key().as_key_slice(),
                Bound::Excluded(upper) => upper > sst.first_key().as_key_slice(),
                Bound::Unbounded => true,
            })
            .collect::<Vec<_>>();

        println!("L0 ssts: {:?}", sstables);

        let sstable_iters: Vec<SsTableIterator> = sstables
            .into_iter()
            .map(|table| match lower {
                Bound::Included(lower) => SsTableIterator::create_and_seek_to_key(table, lower),
                Bound::Excluded(lower) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table, lower)?;
                    if iter.is_valid() && iter.key() == lower {
                        iter.next()?;
                    }
                    Ok(iter)
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table),
            })
            .collect::<Result<_>>()?;

        let sstable_iters: Vec<Box<SsTableIterator>> =
            sstable_iters.into_iter().map(Box::new).collect();

        println!("l0 sstable_iters: {:?}", sstable_iters);

        Ok(MergeIterator::create(sstable_iters))
    }

    fn get_ln_iter(&self, n: usize, lower: Bound<KeySlice>) -> Result<SstConcatIterator> {
        let state = self.state.read();

        let ln_ssts = state.levels[n - 1]
            .1
            .iter()
            .map(|id| state.sstables.get(id).unwrap())
            .cloned()
            .collect::<Vec<_>>();

        match lower {
            Bound::Included(lower) => SstConcatIterator::create_and_seek_to_key(ln_ssts, lower),
            Bound::Excluded(lower) => {
                let mut iter = SstConcatIterator::create_and_seek_to_key(ln_ssts, lower)?;
                if iter.is_valid() && iter.key() == lower {
                    iter.next()?;
                }
                Ok(iter)
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ln_ssts),
        }
    }

    fn get_lower_sst_iter(
        &self,
        lower: Bound<KeySlice>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let state = self.state.read();

        let lower_sst_iter: Vec<Box<SstConcatIterator>> = (1..=state.levels.len())
            .map(|level| self.get_ln_iter(level, lower))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(Box::new)
            .collect();

        Ok(MergeIterator::create(lower_sst_iter))
    }

    fn get_scan_iter(
        &self,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<AllMergedIterator> {
        let memtable_iter = self.get_memtable_iter(lower, upper);
        let l0_iter = self.get_l0_ssts(lower, upper)?;
        let lower_sst_iter = self.get_lower_sst_iter(lower)?;

        let merged = TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, l0_iter)?,
            lower_sst_iter,
        )?;

        Ok(merged)
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        println!(
            "scan_with_ts(lower={:?}, upper={:?}, ts={})",
            lower, upper, read_ts
        );
        let iter = self.get_scan_iter(
            map_lower_bound_u8_to_keyslice(lower),
            map_upper_bound_u8_to_keyslice(upper),
        )?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound_u8_to_bytes(upper),
            read_ts,
        )?))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper)
    }
}
