use std::cmp::min;

use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) enum CollectorCriteria {
    Size(usize),
    Count(usize),
}

impl Default for CollectorCriteria {
    fn default() -> Self {
        Self::Size(4 * 1024)
    }
}

impl CollectorCriteria {
    fn size_collect_block(header: &Header, block_size: usize, start: usize) -> Result<Block> {
        let first = header.get_index(start).ok_or(eyre!("Index out of bounds"))?.1;
        let mut size = 0;
        let end = header
            .entries()
            .get_range(start..start + header.num_entries())
            .ok_or(eyre!("Index out of bounds"))?
            .iter()
            .take_while(|(key, entry)| {
                size += entry.length;
                size <= block_size
            })
            .count();
        ensure!(end > 0, "Block size too small");
        Ok(Block::from_slice(
            first.start_idx + header.raw_size(),
            header.entries().get_range(start..start + end).unwrap(),
        ))
    }

    fn count_collect_block(header: &Header, num_entries: usize, start: usize) -> Result<Block> {
        let first = header.get_index(start).ok_or(eyre!("Index out of bounds"))?.1;
        let end = min(start + num_entries, header.num_entries());
        Ok(Block::from_slice(
            first.start_idx + header.raw_size(),
            header.entries().get_range(start..end).unwrap(),
        ))
    }

    fn collect<'a>(&self, header: &'a Header, start: usize) -> Result<Block<'a>> {
        match self {
            CollectorCriteria::Size(n) => Self::size_collect_block(header, *n, start),
            CollectorCriteria::Count(n) => Self::count_collect_block(header, *n, start),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Collector {
    pub(crate) criteria: CollectorCriteria,
    pub(crate) shuffle: bool,
    pub(crate) shard: Option<(u16, u16)>,
}

impl Default for Collector {
    fn default() -> Self {
        Self {
            criteria: CollectorCriteria::default(),
            shuffle: false,
            shard: None,
        }
    }
}

impl Collector {
    fn collect<'a>(&'a self, header: &'a Header) -> Result<Vec<Block>> {
        let entries = header.entries();
        let mut blocks = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let block = self.criteria.collect(header, start)?;
            start += block.num_entries();
            blocks.push(block);
        }
        Ok(blocks)
    }

    pub(crate) fn as_iter<'a, D>(
        &'a self,
        header: &'a Header,
        data: &'a mut D,
    ) -> impl Iterator<Item = (String, Vec<u8>)> + '_
    where
        D: Read + Seek + 'a,
    {
        let blocks = self.collect(header).unwrap();
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
            let block = &blocks[i];
            block.to_vec(block.read(data).unwrap()).into_iter()
        })
    }
}
