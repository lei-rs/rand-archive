use super::*;

pub(crate) struct Block {
    header: Rc<Header>,
    range: Range<usize>,
}

impl Block {
    pub(crate) fn from_range(header: Rc<Header>, range: Range<usize>) -> Self {
        let header = header.clone();
        Self { header, range }
    }

    pub(crate) fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    pub(crate) fn read<D>(&self, data_source: &mut D) -> Result<Bytes>
    where
        D: DataSource + ?Sized,
    {
        let byte_range = self.header.byte_range_of(&self.range).ok_or(eyre!("Invalid range"))?;
        data_source.get_range(byte_range)
    }

    pub(crate) fn to_vec(&self, mut data: Bytes) -> Result<Vec<Sample>> {
        Ok(self
            .header
            .get_range(self.range.clone())
            .ok_or(eyre!("Invalid range"))?
            .iter()
            .map(|(key, entry)| (key.to_owned(), data.split_to(entry.length())))
            .collect())
    }
}
