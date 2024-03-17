use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|x| *x += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let entry = self.readers.entry(ts).and_modify(|x| *x -= 1);

        match entry {
            std::collections::btree_map::Entry::Vacant(_) => panic!("impossible"),
            std::collections::btree_map::Entry::Occupied(v) => {
                if *v.get() == 0 {
                    v.remove_entry();
                }
            }
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(key, _)| *key)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
