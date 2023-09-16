use std::ops::Range;

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

    pub(crate) fn num_entries(&self) -> usize {
        self.range.end - self.range.start
    }

    pub(crate) fn len_bytes(&self) -> usize {
        self.header.entry_end(self.range.end - 1).unwrap()
            - self.header.entry_start(self.range.start).unwrap()
    }

    pub(crate) fn range_bytes(&self) -> Range<usize> {
        self.header.byte_start(self.range.start).unwrap()
            ..self.header.byte_end(self.range.end - 1).unwrap()
    }

    pub(crate) fn range_in_buffer(&self, entry: &EntryMetadata) -> Range<usize> {
        let offset = self.header.entry_start(self.range.start).unwrap();
        entry.start_idx - offset..entry.end_idx() - offset
    }

    pub(crate) fn read<D: Read + Seek>(&self, data_source: &mut D) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; self.len_bytes()];
        data_source.seek(SeekFrom::Start(self.range_bytes().start as u64))?;
        data_source
            .read_exact(buf.as_mut())
            .map_err(|e| eyre!(e))
            .wrap_err("Failed to read block")?;
        Ok(buf)
    }

    pub(crate) fn to_vec(&self, data: Vec<u8>) -> Vec<(String, Vec<u8>)> {
        self.header
            .get_range(self.range.clone())
            .unwrap()
            .iter()
            .map(|(key, entry)| {
                let range = self.range_in_buffer(entry);
                (key.to_owned(), data[range].to_vec())
            })
            .collect()
    }
}
