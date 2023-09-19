use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;

use bincode::Options;
use indexmap::map::Slice;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleMD {
    start_idx: usize,
    length: usize,
}

impl SampleMD {
    pub fn new(start: usize, offset: usize) -> Result<Self> {
        ensure!(offset > 0, "Size must be greater than 0");
        Ok(Self {
            start_idx: start,
            length: offset,
        })
    }

    pub fn start_idx(&self) -> usize {
        self.start_idx
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn end_idx(&self) -> usize {
        self.start_idx + self.length
    }
}

impl Debug for SampleMD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.start_idx, self.length)
    }
}

#[derive(Error, Debug)]
pub enum HeaderError {
    #[error("Invalid max size: {0}")]
    InvalidMaxSize(usize),
    #[error("Max size exceeded")]
    MaxSizeExceeded,
    #[error("Key: {0} already exists")]
    KeyAlreadyExists(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    max_size: usize,
    entries: IndexMap<String, SampleMD>,
}

impl Header {
    pub(crate) fn new(max_size: usize) -> Result<Self> {
        ensure!(max_size > 0, HeaderError::InvalidMaxSize(max_size));
        Ok(Self {
            max_size,
            entries: IndexMap::new(),
        })
    }

    fn get_options(limit: u64) -> impl Options {
        bincode::DefaultOptions::new()
            .with_varint_encoding()
            .with_big_endian()
            .allow_trailing_bytes()
            .with_limit(limit)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn byte_size(&self) -> usize {
        self.max_size + 8
    }

    pub fn byte_start_of(&self, idx: usize) -> Option<usize> {
        self.get_index(idx)
            .map(|(_, entry)| entry.start_idx() + self.byte_size())
    }

    pub fn byte_end_of(&self, idx: usize) -> Option<usize> {
        self.get_index(idx).map(|(_, entry)| entry.end_idx() + self.byte_size())
    }

    pub fn byte_range_of(&self, range: &Range<usize>) -> Option<Range<usize>> {
        Some(self.byte_start_of(range.start)?..self.byte_end_of(range.end - 1)?)
    }

    pub fn get_key(&self, key: &str) -> Option<&SampleMD> {
        self.entries.get(key)
    }

    pub fn get_index(&self, index: usize) -> Option<(&String, &SampleMD)> {
        self.entries.get_index(index)
    }

    pub fn get_range(&self, range: Range<usize>) -> Option<&Slice<String, SampleMD>> {
        self.entries.get_range(range)
    }

    pub fn entries(&self) -> &IndexMap<String, SampleMD> {
        &self.entries
    }

    pub fn read<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        reader.seek(SeekFrom::Start(0))?;
        let mut max_size = [0u8; 8];
        reader.read_exact(&mut max_size)?;
        let max_size = u64::from_be_bytes(max_size) as usize;

        ensure!(max_size > 0, HeaderError::InvalidMaxSize(max_size));
        let mut buf = vec![0u8; max_size];
        reader.read_exact(&mut buf)?;
        let entries = Header::get_options(max_size as u64)
            .deserialize(&buf)
            .map_err(|e| eyre!(e))
            .wrap_err("Failed to read header")?;

        reader.seek(SeekFrom::Start(8 + max_size as u64))?;
        Ok(Self { max_size, entries })
    }

    pub(crate) fn insert(&mut self, key: &str, entry: SampleMD) -> Result<()> {
        ensure!(
            !self.entries.contains_key(key),
            HeaderError::KeyAlreadyExists(key.to_string())
        );
        self.entries.insert(key.to_string(), entry);
        Ok(())
    }

    pub(crate) fn write<W: Write + Seek>(&self, writer: &mut W) -> Result<usize> {
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(&self.max_size.to_be_bytes())?;
        let mut map_bytes = Header::get_options(self.max_size as u64)
            .serialize(&self.entries)
            .map_err(|e| eyre!(e))
            .wrap_err("Failed to write header")?;
        match map_bytes.len().cmp(&self.max_size) {
            Ordering::Greater => {
                bail!(HeaderError::MaxSizeExceeded);
            }
            Ordering::Less => {
                map_bytes.extend_from_slice(&vec![0u8; self.max_size - map_bytes.len()]);
            }
            _ => {}
        }
        writer.write_all(&map_bytes)?;
        Ok(map_bytes.len() + 8)
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.entries)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use super::*;

    #[test]
    fn test_header_read_write() {
        let mut file = tempfile().unwrap();
        let mut header = Header::new(1000).unwrap();
        header.insert("key1", SampleMD::new(0, 10).unwrap()).unwrap();
        header.insert("key2", SampleMD::new(10, 20).unwrap()).unwrap();
        let n_written = header.write(&mut file).unwrap();
        assert_eq!(n_written, 1008);
        let loaded_header = Header::read(&mut file).unwrap();
        assert_eq!(header, loaded_header);
        assert_eq!(loaded_header.byte_size(), 1008);
        assert_eq!(file.metadata().unwrap().len(), 1008);
    }
}
