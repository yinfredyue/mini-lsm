use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    // Without ts, format: [ key_len (2) | key | value_len (2) | value ]
    // With ts, format: [ key_len (2) | key | ts | value_len (2) | value ]
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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let writer = BufWriter::new(File::open(path.as_ref())?);
        let wal = Self {
            file: Arc::new(Mutex::new(writer)),
        };

        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;
        let mut buf = &buf[..];

        while buf.has_remaining() {
            let key_len = buf.get_u16() as usize;

            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);

            let ts = buf.get_u64();

            let value_len = buf.get_u16() as usize;

            let value = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);

            skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }

        Ok(wal)
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let key_len = (key.key_len() as u16).to_be_bytes();
        let value_len = (value.len() as u16).to_be_bytes();

        let mut bufwriter = self.file.lock();
        assert_eq!(bufwriter.write(&key_len)?, std::mem::size_of::<u16>());
        assert_eq!(bufwriter.write(key.key_ref())?, key.key_len());
        assert_eq!(
            bufwriter.write(key.ts().to_be_bytes().as_slice())?,
            std::mem::size_of::<u64>()
        );
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
