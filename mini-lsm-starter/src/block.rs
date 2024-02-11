mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_elements: Bytes = (self.offsets.len() as u16).to_be_bytes().to_vec().into();
        let data: Bytes = self.data.clone().into();
        let offsets: Bytes = self
            .offsets
            .clone()
            .to_vec()
            .iter()
            .flat_map(|x| x.to_be_bytes())
            .collect();

        [data, offsets, num_elements].concat().into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(raw_data: &[u8]) -> Self {
        let len = raw_data.len();

        let num_elements = u16::from_be_bytes([raw_data[len - 2], raw_data[len - 1]]) as usize;

        let offsets_start = len - 2 - 2 * num_elements;
        assert!((len - 2 - offsets_start) % 2 == 0);
        let mut offsets = Vec::with_capacity(num_elements * 2);
        for chunk in raw_data[offsets_start..(len - 2)].chunks_exact(2) {
            offsets.push(u16::from_be_bytes([chunk[0], chunk[1]]));
        }

        let data = raw_data[..offsets_start].to_vec();

        Block { data, offsets }
    }
}
