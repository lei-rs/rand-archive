use std::cmp::min;

use either::Either;

use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) enum CollectorCriteria {
    Size(usize),
    Count(usize),
}

impl Default for CollectorCriteria {
    fn default() -> Self {
        Self::Size(100 * 1024)
    }
}

impl CollectorCriteria {
    fn size_collect_block(header: Rc<Header>, block_size: usize, start: usize) -> Result<Block> {
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

    fn count_collect_block(header: Rc<Header>, num_entries: usize, start: usize) -> Result<Block> {
        let end = min(start + num_entries, header.len());
        Ok(Block::from_range(header, start..end))
    }

    fn collect(&self, header: Rc<Header>, start: usize) -> Result<Block> {
        match self {
            CollectorCriteria::Size(n) => Self::size_collect_block(header, *n, start),
            CollectorCriteria::Count(n) => Self::count_collect_block(header, *n, start),
        }
    }
}

#[derive(Error, Debug)]
pub enum ShardingError {
    #[error("rank must be less than world_size, got rank: {0}, world_size: {1}")]
    InvalidRank(u16, u16),
    #[error("world_size must be greater than 0, got world_size: {0}")]
    InvalidWorldSize(u16),
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Collector {
    criteria: CollectorCriteria,
    shuffle: bool,
    shard: Option<(u16, u16)>,
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

    pub(crate) fn with_shuffling(&mut self) -> &mut Self {
        self.shuffle = true;
        self
    }

    pub(crate) fn with_sharding(&mut self, rank: u16, world_size: u16) -> Result<&mut Self> {
        ensure!(rank < world_size, ShardingError::InvalidRank(rank, world_size));
        ensure!(world_size > 0, ShardingError::InvalidWorldSize(world_size));
        self.shard = Some((rank, world_size));
        Ok(self)
    }

    fn collect(&self, header: Rc<Header>) -> Result<Vec<Block>> {
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

    fn iter_blocks(&self, header: Rc<Header>) -> Result<impl Iterator<Item = Block>> {
        let mut blocks = self.collect(header.clone())?;
        if self.shuffle {
            blocks.shuffle(&mut rand::thread_rng());
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

    pub(crate) fn iter<D>(&self, header: Rc<Header>, data: Rc<RefCell<D>>) -> Result<impl Iterator<Item = Sample>>
    where
        D: DataSource + ?Sized,
    {
        Ok(self.iter_blocks(header)?.flat_map(move |block| {
            let data = &mut *data.borrow_mut();
            block.to_vec(block.read(data).unwrap()).unwrap().into_iter()
        }))
    }
}
