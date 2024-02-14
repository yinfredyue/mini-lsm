use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::key::KeyBytes;
use crate::table::FileObject;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    // Append data in self.builder to self.data and self.meta
    fn build_block(&mut self) {
        let full_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        // update data, meta
        let block = full_builder.build();

        let block_first_key =
            KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.first_key)));
        let block_last_key = KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.last_key)));
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: block_first_key,
            last_key: block_last_key,
        };

        self.data.append(&mut block.encode().to_vec());
        self.meta.push(block_meta)
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let added = self.builder.add(key, value);
        if !added {
            self.build_block();
            assert!(self.builder.add(key, value));
        }

        if self.first_key.is_empty() {
            self.first_key = key.into_inner().to_vec();
        }
        self.last_key = key.into_inner().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.build_block();

        let mut data_section = self.data;
        let mut meta_section = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut meta_section);
        let block_meta_offset = data_section.len();

        let mut res = Vec::new();
        res.append(&mut data_section);
        res.append(&mut meta_section);
        res.append(&mut (block_meta_offset as u32).to_be_bytes().to_vec());

        let file = FileObject::create(path.as_ref(), res)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
