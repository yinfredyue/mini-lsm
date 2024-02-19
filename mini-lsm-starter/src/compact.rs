#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let guard = self.state.read();

        match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(_) => todo!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let sstables = [l0_sstables.clone(), l1_sstables.clone()]
                    .concat()
                    .into_iter()
                    .map(|id| guard.sstables.get(&id).ok_or(anyhow!("not found")))
                    .map(|res| res.cloned())
                    .collect::<Result<Vec<_>>>()?;

                let sstable_iters = sstables
                    .into_iter()
                    .map(|table| SsTableIterator::create_and_seek_to_first(table))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(Box::new)
                    .collect();

                let mut sorted_run = Vec::new();

                // Build a new SSTable from `builder` and add to `new_l1_sstables`
                let mut build_sst = |builder: SsTableBuilder| -> Result<()> {
                    let sst_id = self.next_sst_id();
                    let sst = builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;
                    sorted_run.push(Arc::new(sst));
                    Ok(())
                };

                {
                    let mut builder = SsTableBuilder::new(self.options.block_size);

                    // Iterate over merged iterator
                    let mut merged_iter = MergeIterator::create(sstable_iters);
                    while merged_iter.is_valid() {
                        if !merged_iter.value().is_empty() {
                            builder.add(merged_iter.key(), merged_iter.value());

                            if builder.estimated_size() > self.options.target_sst_size {
                                let full_builder = std::mem::replace(
                                    &mut builder,
                                    SsTableBuilder::new(self.options.block_size),
                                );
                                build_sst(full_builder)?;
                            }
                        }

                        merged_iter.next()?;
                    }

                    // Add any remaining data
                    build_sst(builder)?;
                }

                Ok(sorted_run)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (task, l0_ssts_to_compact, l1_ssts_to_compact) = {
            let guard = self.state.read();

            println!(
                "Before compaction: l0: {:?},  l1: {:?}, sstables: {:?}",
                guard.l0_sstables, guard.levels[0], guard.sstables
            );

            assert_eq!(guard.levels.len(), 1);
            assert_eq!(guard.levels[0].0, 1);

            let task = CompactionTask::ForceFullCompaction {
                l0_sstables: guard.l0_sstables.clone(),
                l1_sstables: guard.levels[0].1.clone(),
            };

            (task, guard.l0_sstables.clone(), guard.levels[0].1.clone())
        };

        let sorted_run = self.compact(&task)?;
        println!("Sorted run: {:?}", sorted_run);

        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut new_state = guard.as_ref().clone();

            // Delete old l0
            // It's possible that new L0 SSTs are flushed concurrently. Thus,
            // we cannot simply delete all l0_sstables.
            for _ in 0..l0_ssts_to_compact.len() {
                let l1_sst_id = new_state.l0_sstables.pop().unwrap();
                new_state.sstables.remove(&l1_sst_id);
            }
            // Delete old l1
            for old_l1_sst_id in &guard.l0_sstables {
                new_state.sstables.remove(old_l1_sst_id);
            }
            // Add new l1
            for sst in sorted_run.iter() {
                new_state.sstables.insert(sst.sst_id(), sst.clone());
            }
            new_state.levels.clear();
            let new_l1 = sorted_run
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<_>>();
            new_state.levels.push((1, new_l1));

            *guard = Arc::new(new_state);

            println!(
                "After compaction: l0: {:?}, l1: {:?}, sstables: {:?}",
                guard.l0_sstables, guard.levels[0], guard.sstables
            );
        }

        // Remove compacted files
        for id in [l0_ssts_to_compact, l1_ssts_to_compact].concat() {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() + 1 < self.options.num_memtable_limit {
            return Ok(());
        }

        self.force_flush_next_imm_memtable()
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
