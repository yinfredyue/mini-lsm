#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        println!("{:?}", options);
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let target_sizes = self.target_sizes(snapshot);
        println!(
            "target_sizes: {:?}",
            target_sizes
                .iter()
                .map(|x| x / usize::pow(2, 20))
                .collect::<Vec<_>>()
        );

        let num_levels = snapshot.levels.len();

        // L0 compaction, by number of files
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let lower_level_idx = target_sizes.iter().position(|size| *size > 0).unwrap();
            let lower_level = snapshot.levels[lower_level_idx].0;
            let lower_level_sst_ids = snapshot.levels[lower_level_idx].1.clone();
            let is_lower_level_bottom_level = lower_level_idx + 1 == num_levels;

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            });
        }

        // Lower level compaction, by priority = current_size / target_size
        let upper_level_idx = self.highest_priority_level_to_compact(snapshot);

        match upper_level_idx {
            None => None,
            Some(upper_level_idx) => {
                // Select SSTs to compact
                // Oldest SST from the upper level, and all overlapping SSTs from the lower level
                let (upper_level, upper_level_sst_ids) = &snapshot.levels[upper_level_idx];
                let oldest_upper_level_sst_id = upper_level_sst_ids.iter().min().unwrap();
                let oldest_upper_level_sst = snapshot
                    .sstables
                    .get(oldest_upper_level_sst_id)
                    .unwrap()
                    .clone();

                assert!(upper_level_idx != num_levels);
                let (lower_level, lower_level_sst_ids) =
                    snapshot.levels[upper_level_idx + 1].clone();

                let overlapping_lower_level_sst_ids: Vec<usize> = lower_level_sst_ids
                    .into_iter()
                    .filter(|sst_id| {
                        let sst = snapshot.sstables.get(sst_id).unwrap().clone();

                        (sst.last_key() >= oldest_upper_level_sst.first_key())
                            && (sst.first_key() <= oldest_upper_level_sst.last_key())
                    })
                    .collect();

                println!(
                    "upper_level: {:?}, oldest_upper_level_sst: {:?}, overlapping_lower_sst_ids: {:?}",
                    upper_level, oldest_upper_level_sst_id, overlapping_lower_level_sst_ids
                );

                let is_lower_level_bottom_level = upper_level + 1 == num_levels - 1;

                Some(LeveledCompactionTask {
                    upper_level: Some(*upper_level),
                    upper_level_sst_ids: vec![*oldest_upper_level_sst_id],
                    lower_level,
                    lower_level_sst_ids: overlapping_lower_level_sst_ids,
                    is_lower_level_bottom_level,
                })
            }
        }
    }

    fn last_level_size(snapshot: &LsmStorageState) -> usize {
        let last_level = &snapshot.levels.last().unwrap().1;

        last_level
            .iter()
            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size() as usize)
            .sum()
    }

    // In bytes, not mb
    fn target_sizes(&self, snapshot: &LsmStorageState) -> Vec<usize> {
        let num_levels = snapshot.levels.len();
        let base_level_size = self.options.base_level_size_mb * usize::pow(2, 20);
        let last_level_size = Self::last_level_size(snapshot);
        println!("Last_level_size: {:?}", last_level_size / usize::pow(2, 20));

        // last_level_size <= base_level_size
        // Don't create new levels, just add to the bottom level
        if last_level_size <= base_level_size {
            let mut res = vec![0; num_levels];
            res[num_levels - 1] = base_level_size;
            return res;
        }

        // last_level_size > base_level_size
        let mut size_below_base_added = false;
        let mut target_sizes = (1..=snapshot.levels.len())
            .rev()
            .map(|level| {
                let exponent = (num_levels - level) as u32;
                let multipler = self.options.level_size_multiplier.pow(exponent);
                let size = last_level_size / multipler;

                if size > base_level_size {
                    size
                } else if !size_below_base_added {
                    size_below_base_added = true;
                    size
                } else {
                    0
                }
            })
            .collect::<Vec<_>>();

        target_sizes.reverse();
        target_sizes
    }

    fn highest_priority_level_to_compact(&self, snapshot: &LsmStorageState) -> Option<usize> {
        let target_sizes = self.target_sizes(snapshot);
        let current_sizes: Vec<usize> = snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                sst_ids
                    .iter()
                    .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size() as usize)
                    .sum()
            })
            .collect();
        let priorities: Vec<f32> = target_sizes
            .iter()
            .zip(current_sizes.iter())
            .map(|(target, current)| {
                if *target == 0 {
                    0. // To avoid NaN
                } else {
                    *current as f32 / (*target as f32)
                }
            })
            .collect();
        println!("Priorities: {:?}", priorities);

        let highest_priority_idx = priorities
            .iter()
            .enumerate()
            .max_by(|(_, p1), (_, p2)| p1.total_cmp(p2))
            .map(|(idx, _)| idx);

        highest_priority_idx.filter(|&idx| priorities[idx] > 1.)
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let mut ssts_to_remove;

        println!("Compaction task: {:?}, snapshot: {:?}", task, snapshot);

        // Remove task.upper_level_sst_ids, task.lower_level_sst_ids
        // Add output
        match task.upper_level {
            None => {
                let (lower_level_idx, (_, lower_level_sst_ids)) =
                    Self::get_level(snapshot, task.lower_level);
                assert_eq!(&task.lower_level_sst_ids, lower_level_sst_ids);

                for sst_id in task.upper_level_sst_ids.iter().rev() {
                    let removed_sst_id = new_state.l0_sstables.pop().unwrap();
                    assert_eq!(removed_sst_id, *sst_id);
                }
                new_state.levels[lower_level_idx].1.clear();
                new_state.levels[lower_level_idx].1 = output.to_vec();

                ssts_to_remove = task.upper_level_sst_ids.clone();
                ssts_to_remove.append(&mut task.lower_level_sst_ids.clone());
            }
            Some(upper_level) => {
                let (upper_level_idx, _) = Self::get_level(snapshot, upper_level);
                let (lower_level_idx, (_, lower_level_sst_ids)) =
                    Self::get_level(snapshot, task.lower_level);

                assert_eq!(task.upper_level_sst_ids.len(), 1);

                new_state.levels[upper_level_idx]
                    .1
                    .retain(|x| *x != task.upper_level_sst_ids[0]);

                {
                    let new_lower_level = &mut new_state.levels[lower_level_idx].1;
                    new_lower_level.clear();

                    let get_sst = |sst_id| {
                        snapshot
                            .sstables
                            .get(sst_id)
                            .unwrap_or_else(|| panic!("{:?} not found", sst_id))
                            .clone()
                    };

                    let (output_min_key, output_max_key) = {
                        let upper_level_sst = get_sst(&task.upper_level_sst_ids[0]).clone();
                        let mut min_key = upper_level_sst.first_key().clone();
                        let mut max_key = upper_level_sst.last_key().clone();

                        if !task.lower_level_sst_ids.is_empty() {
                            let lower_level_first_sst =
                                get_sst(&task.lower_level_sst_ids[0]).clone();
                            let lower_level_last_sst = get_sst(
                                &task.lower_level_sst_ids[task.lower_level_sst_ids.len() - 1],
                            )
                            .clone();

                            let lower_level_min_key = lower_level_first_sst.first_key().clone();
                            let lower_level_max_key = lower_level_last_sst.last_key().clone();

                            min_key = min_key.min(lower_level_min_key);
                            max_key = max_key.max(lower_level_max_key);
                        }

                        (min_key, max_key)
                    };

                    let mut idx = 0;

                    // Non-overlapping prefix
                    while idx < lower_level_sst_ids.len()
                        && get_sst(&lower_level_sst_ids[idx]).last_key() < &output_min_key
                    {
                        new_lower_level.push(lower_level_sst_ids[idx]);
                        idx += 1;
                    }

                    // Overlapping
                    new_lower_level.extend_from_slice(output);
                    while idx < lower_level_sst_ids.len()
                        && get_sst(&lower_level_sst_ids[idx]).first_key() <= &output_max_key
                    {
                        idx += 1;
                    }

                    // Non-overlapping suffix
                    new_lower_level.extend_from_slice(&lower_level_sst_ids[idx..]);
                }

                ssts_to_remove = vec![task.upper_level_sst_ids[0]];
                ssts_to_remove.append(&mut task.lower_level_sst_ids.clone());
            }
        };

        (new_state, ssts_to_remove)
    }

    fn get_level(snapshot: &LsmStorageState, level_id: usize) -> (usize, &(usize, Vec<usize>)) {
        let pos = snapshot
            .levels
            .iter()
            .position(|level| level.0 == level_id)
            .unwrap();

        (pos, &snapshot.levels[pos])
    }
}
