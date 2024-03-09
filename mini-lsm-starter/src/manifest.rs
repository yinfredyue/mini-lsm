#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        println!("Manifest::create({:?})", path.as_ref());

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let file = Arc::new(Mutex::new(file));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        println!("Manifest::recover({:?})", path.as_ref());

        let mut file = OpenOptions::new().read(true).append(true).open(path)?;

        // Must use `read_to_end` instead of `read`, because `read` only read
        // the size of `buf`!
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let records = serde_json::Deserializer::from_slice(&buf)
            .into_iter::<ManifestRecord>()
            .collect::<Result<Vec<ManifestRecord>, _>>()
            .map_err(anyhow::Error::from)?;

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();

        let buf = serde_json::to_vec(&record)?;
        let num_bytes_written = file.write(&buf)?;
        assert_eq!(num_bytes_written, buf.len());

        file.sync_all().map_err(anyhow::Error::from)
    }
}
