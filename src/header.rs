use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use bincode::Options;
use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
use indexmap::map::Slice;
use indexmap::IndexMap;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub start_idx: usize,
    pub length: usize,
}

impl EntryMetadata {
    pub fn try_new(start: usize, offset: usize) -> Result<Self> {
        ensure!(offset > 0, "Size must be greater than 0");
        Ok(Self {
            start_idx: start,
            length: offset,
        })
    }

    pub fn end_idx(&self) -> usize {
        self.start_idx + self.length
    }
}

#[derive(Clone, Debug)]
pub struct Header {
    max_size: usize,
    entries: IndexMap<String, EntryMetadata>,
}

impl Header {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            max_size,
            entries: IndexMap::new(),
        }
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

    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn raw_size(&self) -> usize {
        self.max_size + 8
    }

    pub(crate) fn insert(&mut self, key: &str, entry: EntryMetadata) -> Result<()> {
        ensure!(!self.entries.contains_key(key), "Key already exists");
        self.entries.insert(key.to_string(), entry);
        Ok(())
    }

    pub fn get_key(&self, key: &str) -> Option<&EntryMetadata> {
        self.entries.get(key)
    }

    pub fn get_index(&self, index: usize) -> Option<(&String, &EntryMetadata)> {
        self.entries.get_index(index)
    }

    pub fn get_range(&self, range: (usize, usize)) -> Option<&Slice<String, EntryMetadata>> {
        self.entries.get_range(range.0..range.1)
    }

    pub fn entries(&self) -> &IndexMap<String, EntryMetadata> {
        &self.entries
    }

    pub fn read(path: &str) -> Result<Self> {
        let path = Path::new(path);
        ensure!(path.exists(), "File does not exist");
        let mut file = File::open(path)?;

        let mut max_size = [0u8; 8];
        file.read_exact(&mut max_size)?;
        let max_size = u64::from_be_bytes(max_size) as usize;
        file.seek(SeekFrom::Start(8))?;

        ensure!(max_size > 0, "Archive has no entries");
        let entries = Header::get_options(max_size as u64)
            .deserialize_from(&mut file)
            .map_err(|e| eyre!(e))
            .wrap_err("Failed to read header")?;

        Ok(Self { max_size, entries })
    }

    pub(crate) fn write(&self, path: &str) -> Result<()> {
        let path = Path::new(path);
        let mut file = match path.exists() {
            true => OpenOptions::new().write(true).open(path)?,
            false => {
                let mut file = OpenOptions::new().write(true).create(true).open(path)?;
                file.write_all(&vec![0u8; self.max_size + 8])?;
                file.seek(SeekFrom::Start(0))?;
                file
            }
        };

        file.write_all(&self.max_size.to_be_bytes())?;
        file.seek(SeekFrom::Start(8))?;
        Header::get_options(self.max_size as u64)
            .serialize_into(&mut file, &self.entries)
            .map_err(|e| eyre!(e))
            .wrap_err("Failed to write header")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::setup;

    #[test]
    fn header_read_write() {
        setup();
        let path = "tests/cache/header_read_write.raa";
        let mut header = Header::new(1000);

        let entry = EntryMetadata::try_new(0, 100).unwrap();
        header.insert("dummy", entry).unwrap();
        header.write(path).unwrap();

        let header_back = Header::read(path).unwrap();
        assert_eq!(
            header.entries.get("dummy").unwrap(),
            header_back.entries.get("dummy").unwrap()
        );

        fs::remove_file(path).unwrap();
    }
}
