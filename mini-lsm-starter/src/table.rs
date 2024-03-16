#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

// With ts:
// | first_key_len | last_key_len | offset | first_key_ts | first_key | last_key_ts | last_key
impl BlockMeta {
    fn encode_one(block_meta: &BlockMeta) -> Vec<u8> {
        let mut buf = Vec::new();

        let first_key_len = block_meta.first_key.key_len() as u16;
        buf.append(&mut first_key_len.to_be_bytes().to_vec());

        let last_key_len = block_meta.last_key.key_len() as u16;
        buf.append(&mut last_key_len.to_be_bytes().to_vec());

        buf.append(&mut (block_meta.offset as u32).to_be_bytes().to_vec());

        buf.append(&mut block_meta.first_key.ts().to_be_bytes().to_vec());
        buf.append(&mut block_meta.first_key.key_ref().to_vec());

        buf.append(&mut block_meta.last_key.ts().to_be_bytes().to_vec());
        buf.append(&mut block_meta.last_key.key_ref().to_vec());

        buf
    }

    fn decode_one(buf: &mut impl Buf) -> BlockMeta {
        let first_key_len = buf.get_u16();
        let last_key_len = buf.get_u16();

        let offset = buf.get_u32() as usize;

        let first_key_ts = buf.get_u64();
        let first_key =
            KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len as usize), first_key_ts);

        let last_key_ts = buf.get_u64();
        let last_key =
            KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len as usize), last_key_ts);

        BlockMeta {
            offset,
            first_key,
            last_key,
        }
    }

    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    ///
    /// Format:
    /// | num_metas | meta | meta | ... | meta |
    /// For each meta block:
    /// | first_key_len | last_key_len | offset (u32) | first_key | last_key |
    /// All numbers are u16 except for offset
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let num_metas = block_meta.len() as u16;
        buf.append(&mut num_metas.to_be_bytes().to_vec());

        for meta in block_meta {
            buf.append(&mut Self::encode_one(meta));
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = Vec::new();

        let num_metas = buf.get_u16();
        println!("num_metas: {:?}", num_metas);
        for _ in 0..num_metas {
            let meta = Self::decode_one(&mut buf);
            metas.push(meta);
        }

        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    /// -------------------------------------------------------------------------------------------
    /// |         Block Section         |          Meta Section         |          Extra          |
    /// -------------------------------------------------------------------------------------------
    /// | data block | ... | data block |            metadata           | meta block offset (u32) |
    /// -------------------------------------------------------------------------------------------
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let filesize = file.1;

        // read meta block offset
        let block_meta_offset_bytes: [u8; 4] = file.read(filesize - 4, 4)?.as_slice().try_into()?;
        let block_meta_offset = u32::from_be_bytes(block_meta_offset_bytes) as u64;

        // read max ts
        let max_ts_bytes: [u8; 8] = file.read(filesize - 4 - 8, 8)?.as_slice().try_into()?;
        let max_ts = u64::from_be_bytes(max_ts_bytes);

        // read meta blocks
        let block_meta_num_bytes = filesize - block_meta_offset - 4;
        let block_meta = BlockMeta::decode_block_meta(
            &(file.read(block_meta_offset, block_meta_num_bytes)?)[..],
        );

        let first_key = block_meta[0].first_key.clone();
        let last_key = block_meta[block_meta.len() - 1].last_key.clone();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let data_start = self.block_meta[block_idx].offset;
        let data_end = if block_idx == self.block_meta.len() - 1 {
            self.block_meta_offset
        } else {
            self.block_meta[block_idx + 1].offset
        };

        let block = self
            .file
            .read(data_start as u64, (data_end - data_start) as u64)?;

        Ok(Arc::new(Block::decode(&block[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match &self.block_cache {
            None => self.read_block(block_idx),
            Some(cache) => {
                cache
                    .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                    .map_err(|err| anyhow::Error::msg(err.to_string())) // This is kinda hacky
            }
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // `partition_point` returns [0, len]. It assumes that the predicate
        // evaluates to true on the prefix, and evaluates to false on the suffix.
        // [ true, true, ..., true, false, false, ..., false ].
        // It returns the index to the first element that evaluates to false.
        //
        // When predicat is "first_key <= key", the returned `idx` is the first
        // element s.t. "first_key > key". So, we should start searching at
        // max(0, `idx-1`).
        let idx = self
            .block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key);

        if idx == 0 {
            idx
        } else {
            idx - 1
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

impl std::fmt::Debug for SsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SsTable[{}, first={:?}, last={:?}]",
            self.id, self.first_key, self.last_key
        )
    }
}
