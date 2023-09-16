use std::cell::RefCell;
use std::cmp::min;

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
            .get_range(start..header.num_entries())
            .ok_or(eyre!("Index out of bounds"))?
            .iter()
            .take_while(|(_, entry)| {
                size += entry.length;
                size <= block_size
            })
            .count().max(1);
        ensure!(range_size > 0, "Block size too small");
        Ok(Block::from_range(
            header,
            start..start + range_size,
        ))
    }

    fn count_collect_block(header: Rc<Header>, num_entries: usize, start: usize) -> Result<Block> {
        let end = min(start + num_entries, header.num_entries());
        Ok(Block::from_range(
            header,
            start..end,
        ))
    }

    fn collect(&self, header: Rc<Header>, start: usize) -> Result<Block> {
        match self {
            CollectorCriteria::Size(n) => Self::size_collect_block(header, *n, start),
            CollectorCriteria::Count(n) => Self::count_collect_block(header, *n, start),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Collector {
    pub(crate) criteria: CollectorCriteria,
    pub(crate) shuffle: bool,
    pub(crate) shard: Option<(u16, u16)>,
}

impl Collector {
    fn collect(&self, header: Rc<Header>) -> Result<Vec<Block>> {
        let entries = header.entries();
        let mut blocks = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let block = self.criteria.collect(header.clone(), start)?;
            start += block.num_entries();
            blocks.push(block);
        }
        Ok(blocks)
    }

    pub(crate) fn to_iter<D>(self, header: &Rc<Header>, data: &Rc<RefCell<D>>) -> impl Iterator<Item = (String, Vec<u8>)>
    where
        D: Read + Seek,
    {
        let data = data.clone();
        let blocks = Rc::new(self.collect(header.clone()).unwrap());
        let mut indices = (0..blocks.len()).collect::<Vec<_>>();
        if self.shuffle {
            indices.shuffle(&mut rand::thread_rng());
        }
        let indices = match self.shard {
            Some((rank, world_size)) => indices
                .into_iter()
                .filter(move |&i| i as u16 % world_size == rank)
                .collect(),
            None => indices,
        };
        indices.into_iter().flat_map(move |i| {
            let blocks = blocks.clone();
            let block = blocks.get(i).unwrap();
            let data = &mut *data.borrow_mut();
            block
                .to_vec(block.read(data).unwrap())
                .into_iter()
        })
    }
}
