use std::cmp::min;

use color_eyre::eyre::{ensure, eyre, Result};
use either::Either;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use thiserror::Error;

use crate::reader::block::Block;
use crate::reader::bounded::BoundedIter;
use crate::reader::datasource::DataSource;
use crate::reader::readers::RcHeader;
use crate::reader::Sample;

#[derive(Clone, Copy, Debug)]
pub(crate) enum CollectorCriteria {
    Size(usize),
    Count(usize),
}

impl Default for CollectorCriteria {
    fn default() -> Self {
        Self::Count(1)
    }
}

impl CollectorCriteria {
    fn size_collect_block(header: RcHeader, block_size: usize, start: usize) -> Result<Block> {
        let mut size = 0;
        let range_size = header
            .get_range(start..header.len())
            .ok_or(eyre!("Index out of bounds"))?
            .iter()
            .take_while(|(_, entry)| {
                size += entry.length();
                size <= block_size
            })
            .count()
            .max(1);
        Ok(Block::from_range(header, start..start + range_size))
    }

    fn count_collect_block(header: RcHeader, num_entries: usize, start: usize) -> Block {
        let end = min(start + num_entries, header.len());
        Block::from_range(header, start..end)
    }

    fn collect(&self, header: RcHeader, start: usize) -> Result<Block> {
        match self {
            CollectorCriteria::Size(n) => Self::size_collect_block(header, *n, start),
            CollectorCriteria::Count(n) => Ok(Self::count_collect_block(header, *n, start)),
        }
    }
}

#[derive(Error, Debug)]
pub enum CollectorError {
    #[error("Rank must be less than world_size, got rank: {0}, world_size: {1}")]
    InvalidRank(u16, u16),
    #[error("World_size must be greater than 0, got world_size: {0}")]
    InvalidWorldSize(u16),
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Collector {
    criteria: CollectorCriteria,
    shuffle: Option<u64>,
    shard: Option<(u16, u16)>,
    buffer_size: Option<u32>,
}

impl Collector {
    pub(crate) fn by_size(&mut self, size: usize) -> &mut Self {
        self.criteria = CollectorCriteria::Size(size);
        self
    }

    pub(crate) fn by_count(&mut self, count: usize) -> &mut Self {
        self.criteria = CollectorCriteria::Count(count);
        self
    }

    pub(crate) fn with_shuffling(&mut self, seed: Option<u64>) -> &mut Self {
        self.shuffle = seed;
        self
    }

    pub(crate) fn with_sharding(&mut self, rank: u16, world_size: u16) -> Result<&mut Self> {
        ensure!(rank < world_size, CollectorError::InvalidRank(rank, world_size));
        ensure!(world_size > 0, CollectorError::InvalidWorldSize(world_size));
        self.shard = Some((rank, world_size));
        Ok(self)
    }

    pub(crate) fn with_buffering(&mut self, buffer_size: u32) -> &mut Self {
        self.buffer_size = Some(buffer_size);
        self
    }

    fn collect(&self, header: RcHeader) -> Result<Vec<Block>> {
        let entries = header.entries();
        let mut blocks = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let block = self.criteria.collect(header.clone(), start)?;
            start += block.len();
            blocks.push(block);
        }
        Ok(blocks)
    }

    fn iter_blocks(&self, header: RcHeader) -> Result<impl Iterator<Item = Block>> {
        let mut blocks = self.collect(header.clone())?;
        if let Some(seed) = self.shuffle {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            blocks.shuffle(&mut rng);
        }
        let iter = blocks.into_iter();
        match self.shard {
            Some((rank, world_size)) => Ok(Either::Left(
                iter.enumerate()
                    .filter(move |(i, _)| *i as u16 % world_size == rank)
                    .map(|(_, block)| block),
            )),
            None => Ok(Either::Right(iter)),
        }
    }

    fn add_buffering<I>(&self, data: DataSource, block_iter: I) -> impl Iterator<Item = Result<Block>>
    where
        I: Iterator<Item = Block>,
    {
        let ds = data.into_async().unwrap();
        let futures = block_iter.map(|block| block.read_async(ds.clone())).collect();
        BoundedIter::from_vec(futures, self.buffer_size.unwrap() as usize)
    }

    pub(crate) fn iter(&self, header: RcHeader, data: DataSource) -> impl Iterator<Item = Sample> {
        match self.buffer_size {
            Some(_) => Either::Left(
                self.add_buffering(data, self.iter_blocks(header).unwrap())
                    .flat_map(|block| block.unwrap().to_vec().unwrap().into_iter()),
            ),
            None => Either::Right(self.iter_blocks(header).unwrap().flat_map(move |block| {
                let data = data.clone().into_sync().unwrap();
                block.read(data).unwrap().to_vec().unwrap().into_iter()
            })),
        }
    }
}
