use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        println!("Options: {:?}", options);
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        for upper_level in 0..self.options.max_levels {
            let upper_level_sst_ids = Self::get_level_sst_ids(snapshot, upper_level);
            let lower_level = upper_level + 1;
            let lower_level_sst_ids = Self::get_level_sst_ids(snapshot, lower_level);

            if self.should_compact(
                upper_level,
                upper_level_sst_ids.len(),
                lower_level_sst_ids.len(),
            ) {
                println!(
                    "compaction triggered at level {} and {}",
                    upper_level, lower_level
                );

                let upper_level = if upper_level == 0 {
                    None
                } else {
                    Some(upper_level)
                };
                return Some(SimpleLeveledCompactionTask {
                    upper_level,
                    upper_level_sst_ids: upper_level_sst_ids.clone(),
                    lower_level,
                    lower_level_sst_ids: lower_level_sst_ids.clone(),
                    is_lower_level_bottom_level: Self::is_bottom_level(snapshot, lower_level),
                });
            }
        }

        None
    }

    // `level = n` -> Ln
    fn get_level_sst_ids<'a>(snapshot: &'a LsmStorageState, level: usize) -> &'a Vec<usize> {
        if level == 0 {
            return &snapshot.l0_sstables;
        }

        let level_0_based = level - 1;
        assert!(snapshot.levels[level_0_based].0 == level);

        &snapshot.levels[level_0_based].1
    }

    fn is_bottom_level(snapshot: &LsmStorageState, level: usize) -> bool {
        assert!(level >= 1);
        level == snapshot.levels.len()
    }

    // For L0, consider l0_file_num and size_ratio
    // For L1..Ln, consider size_ratio
    fn should_compact(
        &self,
        upper_level: usize,
        upper_level_size: usize,
        lower_level_size: usize,
    ) -> bool {
        let size_ratio = if upper_level_size == 0 {
            self.options.size_ratio_percent
        } else {
            lower_level_size / upper_level_size
        };

        let size_ratio_trigger = size_ratio * 100 < self.options.size_ratio_percent;

        if upper_level == 0 {
            return upper_level_size >= self.options.level0_file_num_compaction_trigger
                && size_ratio_trigger;
        }

        size_ratio_trigger
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let mut ssts_to_remove = Vec::new();

        match task.upper_level {
            None => {
                // L0 compaction:
                // Delete compacted L0 tables
                // Delete compacted L1 tables
                // Add new L1 tables

                for compacted_l0_sst in task.upper_level_sst_ids.iter().rev() {
                    let popped = new_state.l0_sstables.pop().unwrap();
                    ssts_to_remove.push(popped);
                    assert_eq!(popped, *compacted_l0_sst);
                }

                ssts_to_remove.append(&mut new_state.levels[0].1);
                new_state.levels[0].1 = output.to_owned();
            }
            Some(n) => {
                // Ln compaction, where n = upper_level
                // Delete compacted Ln tabls
                // Delete compacted Ln+1 tables
                // Add new Ln+1 tables
                for compacted_ln_sst in task.upper_level_sst_ids.iter().rev() {
                    let popped = new_state.levels[n - 1].1.pop().unwrap();
                    ssts_to_remove.push(popped);
                    assert_eq!(popped, *compacted_ln_sst);
                }

                ssts_to_remove.append(&mut new_state.levels[n].1);
                new_state.levels[n].1 = output.to_owned();
            }
        };

        (new_state, ssts_to_remove)
    }
}
