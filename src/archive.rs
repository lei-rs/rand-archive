use std::fs::{metadata, OpenOptions};
use std::io::{Write};

use color_eyre::eyre::{ensure, eyre, Result, WrapErr};

use crate::header::{EntryMetadata, Header};

#[derive(Clone, Debug)]
pub struct ArchiveWriter {
    pub path: String,
    cache: Vec<u8>,
    header: Header,
    data_size: usize,
    cache_size: usize,
}

impl ArchiveWriter {
    pub fn new(path: String, cache_size: usize, header_max_size: usize) -> Self {
        Self {
            path,
            cache: Vec::new(),
            header: Header::new(header_max_size),
            data_size: 0,
            cache_size,
        }
    }

    pub fn read(path: &str, cache_size: usize) -> Result<Self> {
        let header = Header::read(path)?;
        let len = metadata(path)?.len() as usize - header.max_size - 8;
        ensure!(len > 0, "Archive has no entries");
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
        let entry = EntryMetadata::try_new(self.data_size, value.len())?;
        self.data_size = entry.end();
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
    use crate::setup;
    use super::*;
    use std::{assert_eq, fs};

    #[test]

    #[test]
    fn archive_flush() {
        setup();
        let path = "tests/cache/archive_flush.raa";
        let mut archive = ArchiveWriter::new(path.to_string(), 100, 1000);
        let entry = EntryMetadata::try_new(0, 100).unwrap();

        archive.append("dummy", &[0u8; 100]).unwrap();
        archive.flush().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(header.entries.get("dummy").unwrap(), &entry);

        fs::remove_file(path).unwrap();
    }
}
