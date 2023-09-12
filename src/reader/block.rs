use indexmap::map::Slice;

use super::*;

macro_rules! vec_raw {
    ($ptr:expr, $length:expr) => {
        unsafe { Vec::from_raw_parts($ptr, $length, $length) }
    };
}

pub struct Block<'a> {
    start_idx: usize,
    length: usize,
    entries: &'a Slice<String, EntryMetadata>,
}

impl<'a> Block<'a> {
    pub(crate) fn from_slice(start_idx: usize, entries: &'a Slice<String, EntryMetadata>) -> Self {
        let length = entries.last().unwrap().1.end_idx() - entries.first().unwrap().1.start_idx;
        Self {
            start_idx,
            length,
            entries,
        }
    }

    pub(crate) fn num_entries(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn read<D: Read + Seek>(&self, data_source: &mut D) -> Result<Box<[u8]>> {
        let mut buf = vec![0u8; self.length].into_boxed_slice();
        data_source.seek(SeekFrom::Start(self.start_idx as u64))?;
        data_source
            .read_exact(buf.as_mut())
            .map_err(|e| eyre!(e))
            .wrap_err("Failed to read block")?;
        Ok(buf)
    }

    pub(crate) fn to_vec(&self, data: Box<[u8]>) -> Vec<(String, Vec<u8>)> {
        let base_ptr = Box::into_raw(data) as *mut u8;
        self.entries
            .iter()
            .map(|(key, entry)| {
                let start = base_ptr.wrapping_add(entry.start_idx - self.start_idx);
                (key.to_owned(), vec_raw!(start, entry.length))
            })
            .collect()
    }
}
