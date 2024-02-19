use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        println!("TiedCompactionOptions: {:?}", options);
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // Each tier is a sorted run.
        // Trigger compaction only when the number of tiers > num_tiers
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Space amplification ratio
        {
            let all_levels_except_last_size: usize = snapshot.levels[..snapshot.levels.len() - 1]
                .iter()
                .map(|(level, _)| Self::level_size(snapshot, *level))
                .sum();
            let last_level_size = Self::level_size(snapshot, snapshot.levels.last().unwrap().0);

            let space_amp_ratio = all_levels_except_last_size as f32 / last_level_size as f32;
            if space_amp_ratio * 100. >= self.options.max_size_amplification_percent as f32 {
                // full compaction
                println!(
                    "compaction triggered by space amplification ratio: {}",
                    space_amp_ratio
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }
        }

        // Size ratio
        {
            let mut all_previous_levels_size = 0;
            for (idx, (level, _)) in snapshot.levels.iter().enumerate() {
                let current_level_size = Self::level_size(snapshot, *level);

                let size_ratio = all_previous_levels_size as f32 / current_level_size as f32 * 100.;

                if idx + 1 >= self.options.min_merge_width
                    && size_ratio >= 100. + self.options.size_ratio as f32
                {
                    println!("compaction triggered by size ratio: {}", size_ratio);

                    return Some(TieredCompactionTask {
                        tiers: snapshot.levels[0..=idx].to_vec(),
                        bottom_tier_included: true,
                    });
                }

                all_previous_levels_size += current_level_size;
            }
        }

        // Reduce sorted run
        Some(TieredCompactionTask {
            tiers: snapshot.levels[..2].to_vec(),
            bottom_tier_included: true,
        })
    }

    fn level_size(snapshot: &LsmStorageState, level: usize) -> usize {
        assert!(level >= 1);

        for (curr_level, ssts) in snapshot.levels.iter() {
            if *curr_level == level {
                return ssts.len();
            }
        }

        panic!(
            "Level not found? level={}, levels={:?}",
            level, snapshot.levels
        );
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(task.bottom_tier_included);

        let mut new_state = snapshot.clone();

        let mut ssts_to_remove = Vec::new();

        // All tiers are merged to create a new sorted run in a new level.
        // Delete all old tables
        // Add new tables to the last level

        let mut tiers_by_level: HashMap<usize, Vec<usize>> = HashMap::new();
        for (level, ssts) in task.tiers.iter() {
            tiers_by_level.insert(*level, ssts.clone());
        }

        for (level, ssts) in new_state.levels.iter_mut() {
            if let Some(tier) = tiers_by_level.get(level) {
                for sst_to_remove in tier.iter().rev() {
                    let popped = ssts.pop().unwrap();
                    assert_eq!(popped, *sst_to_remove);
                    ssts_to_remove.push(*sst_to_remove);
                }
            }
        }

        // insert at the front
        let next_level = tiers_by_level.keys().max().unwrap() + 1;
        new_state
            .levels
            .insert(0, (next_level + 1, output.to_owned()));

        // Delete empty levels
        new_state.levels = new_state
            .levels
            .into_iter()
            .filter(|(_, ssts)| !ssts.is_empty())
            .collect();

        println!("new_state.levels: {:?}", new_state.levels);

        (new_state, ssts_to_remove)
    }
}
