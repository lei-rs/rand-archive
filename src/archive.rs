use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

use super::*;
use crate::header::{Header, SampleMD};

#[derive(Error, Debug)]
pub enum WriterError {
    #[error("Invalid cache size: {0}")]
    InvalidCacheSize(usize),
    #[error("Received empty sample")]
    EmptySample,
}

#[derive(Debug)]
pub struct Writer {
    file: File,
    cache: Vec<u8>,
    header: Header,
    data_size: usize,
    cache_size: usize,
}

impl Writer {
    pub fn new(file: File, cache_size: usize, header_max_size: usize) -> Result<Self> {
        ensure!(cache_size > 0, WriterError::InvalidCacheSize(cache_size));
        Ok(Self {
            file,
            cache: Vec::with_capacity(cache_size),
            header: Header::new(header_max_size)?,
            data_size: 0,
            cache_size,
        })
    }

    pub fn load(mut file: File, cache_size: usize) -> Result<Self> {
        ensure!(cache_size > 0, WriterError::InvalidCacheSize(cache_size));
        let header = Header::read(&mut file)?;
        let len = file.metadata()?.len() as usize - header.byte_size();
        Ok(Self {
            file,
            cache: Vec::with_capacity(cache_size),
            header,
            data_size: len,
            cache_size,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    fn append(&mut self, key: &str, value: &[u8]) -> Result<()> {
        ensure!(!value.is_empty(), WriterError::EmptySample);
        self.cache.extend_from_slice(value);
        let entry = SampleMD::new(self.data_size, value.len())?;
        self.data_size = entry.end_idx();
        self.header.insert(key, entry)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.header.write(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&self.cache)?;
        self.cache.clear();
        Ok(())
    }

    pub fn write(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.append(key, value)?;
        if self.cache.len() >= self.cache_size {
            self.flush()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.flush()
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.close().expect("Failed to close writer");
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;
    use std::io::Read;

    use tempfile::tempfile;

    use super::*;
    use crate::test_setup::*;

    #[test]
    fn test_writer_new() {
        setup();
        let file = tempfile().unwrap();
        let writer = Writer::new(file, 1024, 10 * 1024).unwrap();
        assert_eq!(writer.data_size, 0);
        assert_eq!(writer.cache.len(), 0);
        assert_eq!(writer.cache_size, 1024);
    }

    #[test]
    fn test_writer_load() {
        setup();
        let entries_count = 10;
        let value_size = 100;
        let dummy_writer = new_dummy_file(entries_count, value_size).unwrap();
        let loaded_file = dummy_writer.file.try_clone().unwrap();
        let loaded_writer = Writer::load(loaded_file, 1024).unwrap();
        assert_eq!(loaded_writer.data_size, entries_count * value_size);
        assert_eq!(*dummy_writer.header(), *loaded_writer.header());
    }

    #[test]
    fn test_writer_append_and_flush() {
        setup();
        let file = tempfile().unwrap();
        let mut writer = Writer::new(file, 1024, 10 * 1024).unwrap();

        let key = generate_random_key(6);
        let value = generate_random_value(100);
        writer.append(&key, &value).unwrap();
        assert_eq!(writer.data_size, value.len());
        assert_eq!(writer.cache, value);

        writer.flush().unwrap();
        let mut buffer = [0u8; 100];
        writer
            .file
            .seek(SeekFrom::Start(writer.header.byte_size() as u64))
            .unwrap();
        writer.file.read_exact(&mut buffer).unwrap();
        assert_eq!(buffer, value[..]);
    }

    #[test]
    fn test_writer_write_load() {
        setup();
        let dummy_writer = new_dummy_file(10, 100).unwrap();
        let dummy_header = dummy_writer.header().clone();
        let loaded_writer = Writer::load(dummy_writer.file.try_clone().unwrap(), 1024).unwrap();
        assert_eq!(*dummy_writer.header(), *loaded_writer.header());
        assert_eq!(1000, loaded_writer.data_size);
    }
}
