use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
use indexmap::IndexMap;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};


const HEADER_SIZE: usize = 1048576 - 8;


#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub start: usize,
    pub end: usize,
}

impl EntryMetadata {
    fn try_new(start: usize, end: usize) -> Result<Self> {
        ensure!(start < end, "Start must be less than end");
        Ok(Self {
            start,
            end,
        })
    }

    fn size(&self) -> usize {
        self.end - self.start
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Header {
    entries: IndexMap<String, EntryMetadata>,
}

impl Header {
    pub fn read(path: &str) -> Result<Self> {
        let path = Path::new(path);
        ensure!(path.exists(), "File does not exist");
        let mut file = File::open(path)
            .wrap_err(format!("Failed to open file: {}", path.display()))?;
        let mut buffer = [0u8; HEADER_SIZE];
        file.read_exact(&mut buffer)
            .wrap_err(format!("Failed to read header: {}", path.display()))?;
        let header_len = u64::from_be_bytes(buffer[..8].try_into()
            .wrap_err(format!("Failed to read header: {}", path.display()))?);
        let header: Header = serde_json::from_slice(&buffer[8..(8 + header_len as usize)])
            .wrap_err(format!("Failed to parse header: {}", path.display()))?;
        Ok(header)
    }

    fn write(&self, path: &str) -> Result<()> {
        let path = Path::new(path);
        let mut file = match path.exists() {
            true => OpenOptions::new().write(true).open(path)?,
            false => OpenOptions::new().write(true).create(true).open(path)?,
        };
        let header = serde_json::to_string(&self)?;
        let header_bytes = header.as_bytes();
        let header_len = header_bytes.len() as u64;
        ensure!(header_len < HEADER_SIZE as u64, "Too many entries");
        file.write_all(&header_len.to_be_bytes())?;
        file.write_at(header_bytes, 8)?;
        Ok(())
    }

    fn collect_block(&self, block_size: usize, first_idx: usize) -> Result<Vec<EntryMetadata>> {
        let mut entries = Vec::new();
        let (_, first_entry) = self.entries.get_index(first_idx).unwrap();
        let start = first_entry.start;
        let mut end = start;
        let mut size = 0;

        for (_, entry) in self.entries.iter().skip(first_idx) {
            size += entry.size();
            end = entry.end;
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
    path: String,
    data: Vec<u8>,
    header: Header,
    len: usize,
    in_mem_size: usize,
}

impl ArchiveWriter {
    pub fn new(path: String, in_mem_size: Option<usize>) -> Self {
        let in_mem_size = in_mem_size.unwrap_or(524288000);
        Self {
            path,
            data: Vec::new(),
            header: Header::default(),
            len: 0,
            in_mem_size,
        }
    }

    fn append(&mut self, key: &str, value: &[u8]) {
        self.data.extend_from_slice(value);
        let entry = EntryMetadata::try_new(
            self.len,
            self.data.len(),
        ).unwrap();
        self.len += entry.size();
        self.header.entries.insert(key.to_string(), entry);
    }

    fn flush(&mut self) -> Result<()> {
        let path = Path::new(&self.path);
        let mut file = match path.exists() {
            true => OpenOptions::new().write(true).append(true).open(&self.path)?,
            false => OpenOptions::new().write(true).append(true).create(true).open(&self.path)?,
        };
        file.write_all(&self.data)?;
        self.data.clear();
        self.header.write(&self.path).map_err(|e| eyre!(e))
            .wrap_err(format!("Failed to write header: {}", path.display()))
    }

    pub fn write(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.append(key, value);
        if self.data.len() > self.in_mem_size {
            self.flush().map_err(|e| eyre!(e))
                .wrap_err(format!("Failed to flush archive: {}", self.path))?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.flush().map_err(|e| eyre!(e))
            .wrap_err(format!("Failed to flush archive: {}", self.path))
    }
}
