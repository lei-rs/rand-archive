use std::fs::{metadata, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
use indexmap::IndexMap;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

const HEADER_SIZE: usize = 1048576 - 8;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub start: usize,
    pub end: usize,
}

impl EntryMetadata {
    pub fn try_new(start: usize, end: usize) -> Result<Self> {
        ensure!(start < end, "Start must be less than end");
        Ok(Self { start, end })
    }

    pub fn size(&self) -> usize {
        self.end - self.start
    }
}

#[derive(Clone, Debug, Default)]
pub struct Header {
    pub entries: IndexMap<String, EntryMetadata>,
}

impl Header {
    fn insert(&mut self, key: &str, entry: EntryMetadata) -> Result<()> {
        ensure!(!self.entries.contains_key(key), "Key already exists");
        self.entries.insert(key.to_string(), entry);
        Ok(())
    }

    pub fn read(path: &str) -> Result<Self> {
        let path = Path::new(path);
        ensure!(path.exists(), "File does not exist");
        let file = File::open(path)?;

        let mut buffer = Vec::with_capacity(HEADER_SIZE);
        file.take(HEADER_SIZE as u64).read_to_end(&mut buffer)?;
        let header_len = u64::from_be_bytes(buffer[0..8].try_into()?);
        let header: IndexMap<String, EntryMetadata> =
            bitcode::deserialize(&buffer[8..(8 + header_len as usize)])?;

        Ok(Self { entries: header })
    }

    fn write(&self, path: &str) -> Result<()> {
        let path = Path::new(path);
        let mut file = match path.exists() {
            true => OpenOptions::new().write(true).open(path)?,
            false => OpenOptions::new().write(true).create(true).open(path)?,
        };

        let header = bitcode::serialize(&self.entries)?;
        let header_len = header.len() as u64;
        ensure!(header_len < HEADER_SIZE as u64, "Too many entries");
        let padding = vec![0u8; HEADER_SIZE - header_len as usize];

        file.write_all(&header_len.to_be_bytes())?;
        file.seek(SeekFrom::Start(8))?;
        file.write_all(&header)?;
        file.seek(SeekFrom::Start(header_len + 8))?;
        file.write_all(&padding)?;

        Ok(())
    }

    fn collect_block(&self, block_size: usize, first_idx: usize) -> Result<Vec<EntryMetadata>> {
        let mut entries = Vec::new();
        let mut size = 0;

        for (_, entry) in self.entries.iter().skip(first_idx) {
            size += entry.size();
            entries.push(entry.clone());
            if size + entry.size() > block_size {
                break;
            }
        }

        Ok(entries)
    }

    pub fn block_shuffle(&self, block_size: usize, seed: u64) -> Result<Vec<Vec<EntryMetadata>>> {
        ensure!(!self.entries.is_empty(), "No entries to shuffle");

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut blocks = Vec::new();

        let mut idx = 0;
        while idx < self.entries.len() {
            let block = self.collect_block(block_size, idx)?;
            blocks.push(block);
            idx += blocks.last().unwrap().len();
        }

        blocks.shuffle(&mut rng);
        Ok(blocks)
    }
}

#[derive(Clone, Debug)]
pub struct ArchiveWriter {
    pub path: String,
    cache: Vec<u8>,
    header: Header,
    data_size: usize,
    cache_size: usize,
}

impl ArchiveWriter {
    pub fn new(path: String, cache_size: usize) -> Self {
        Self {
            path,
            cache: Vec::new(),
            header: Header::default(),
            data_size: 0,
            cache_size,
        }
    }

    pub fn read(path: &str, cache_size: usize) -> Result<Self> {
        let header = Header::read(path)?;
        let len = metadata(path)?.len() as usize - HEADER_SIZE - 8;
        ensure!(len > 0, "Archive is empty");
        let path = path.to_string();
        Ok(Self {
            path,
            cache: Vec::new(),
            header,
            data_size: len,
            cache_size,
        })
    }

    fn append(&mut self, key: &str, value: &[u8]) -> Result<()> {
        ensure!(!value.is_empty(), "Value is empty");

        self.cache.extend_from_slice(value);
        let entry = EntryMetadata::try_new(self.data_size, self.data_size + value.len())?;
        self.data_size = entry.end;
        self.header.insert(key, entry).unwrap();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.header.write(&self.path)?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&self.cache)?;
        self.cache.clear();
        Ok(())
    }

    pub fn write(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.append(key, value)?;
        if self.cache.len() >= self.cache_size {
            self.flush()
                .map_err(|e| eyre!(e))
                .wrap_err(format!("Failed to flush archive: {}", self.path))?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.flush()
            .map_err(|e| eyre!(e))
            .wrap_err(format!("Failed to flush archive: {}", self.path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use std::{assert_eq, fs};

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            color_eyre::install().unwrap();
        });
    }

    #[test]
    fn header_read_write() {
        setup();
        let path = "tests/cache/header_read_write.raa";
        let mut header = Header::default();
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

    #[test]
    fn archive_flush() {
        setup();
        let path = "tests/cache/archive_flush.raa";
        let mut archive = ArchiveWriter::new(path.to_string(), 100);
        let entry = EntryMetadata::try_new(0, 100).unwrap();
        archive.append("dummy", &[0u8; 100]).unwrap();
        archive.flush().unwrap();
        let header = Header::read(path).unwrap();
        assert_eq!(header.entries.get("dummy").unwrap(), &entry);
        fs::remove_file(path).unwrap();
    }
}
