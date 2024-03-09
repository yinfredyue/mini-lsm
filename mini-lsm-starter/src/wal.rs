#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    // Format: [ key_len (2) | key | value_len (2) | value ]
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)?;
        let bufwriter = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(bufwriter)),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let writer = BufWriter::new(File::open(path.as_ref())?);
        let wal = Self {
            file: Arc::new(Mutex::new(writer)),
        };

        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;

        {
            let mut curr = 0;
            loop {
                if curr >= buf.len() {
                    break;
                }

                let key_len = u16::from_be_bytes([buf[curr], buf[curr + 1]]) as usize;
                curr += 2;

                let key = Bytes::copy_from_slice(&buf[curr..curr + key_len]);
                curr += key_len;

                let value_len = u16::from_be_bytes([buf[curr], buf[curr + 1]]) as usize;
                curr += 2;

                let value = Bytes::copy_from_slice(&buf[curr..curr + value_len]);
                curr += value_len;

                println!(
                    "key_len: {}, key: {}, value_len: {}, value: {}",
                    key_len,
                    String::from_utf8(key.to_vec()).unwrap(),
                    value_len,
                    String::from_utf8(value.to_vec()).unwrap(),
                );

                skiplist.insert(key, value);
            }
        }

        Ok(wal)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_len = (key.len() as u16).to_be_bytes();
        let value_len = (value.len() as u16).to_be_bytes();

        let mut bufwriter = self.file.lock();
        assert_eq!(bufwriter.write(&key_len)?, 2);
        assert_eq!(bufwriter.write(key)?, key.len());
        assert_eq!(bufwriter.write(&value_len)?, 2);
        assert_eq!(bufwriter.write(value)?, value.len());

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file
            .lock()
            .get_mut()
            .sync_all()
            .map_err(anyhow::Error::from)
    }
}
