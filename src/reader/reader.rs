use std::fs::File;

use super::*;
use crate::reader::collector::{Collector, CollectorCriteria};

pub struct Reader<D: Read + Seek> {
    header: Header,
    data: D,
    collector: Collector,
}

impl<D: Read + Seek> Reader<D> {
    pub fn shuffle(&mut self) {
        self.collector.shuffle = true;
    }

    pub fn by_size(&mut self, size: usize) {
        self.collector.criteria = CollectorCriteria::Size(size);
    }

    pub fn by_count(&mut self, count: usize) {
        self.collector.criteria = CollectorCriteria::Count(count);
    }

    pub fn shard(&mut self, rank: u16, world_size: u16) -> Result<()> {
        ensure!(
            rank < world_size,
            "rank must be less than world_size, got rank: {}, world_size: {}",
            rank,
            world_size
        );
        ensure!(
            world_size > 0,
            "world_size must be greater than 0, got world_size: {}",
            world_size
        );
        self.collector.shard = Some((rank, world_size));
        Ok(())
    }

    pub fn as_iter(&mut self) -> impl Iterator<Item = (String, Vec<u8>)> + '_ {
        self.collector.as_iter(&self.header, &mut self.data)
    }
}

impl Reader<File> {
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path).map_err(|e| eyre!(e)).wrap_err("Failed to open file")?;
        let header = Header::read(path)?;
        let collector = Collector::default();
        Ok(Self {
            header,
            data: file,
            collector,
        })
    }
}
