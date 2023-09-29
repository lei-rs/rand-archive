use std::fs::File;
use std::rc::Rc;

use bytes::Bytes;
use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
#[cfg(feature = "gcs")]
use gcs_reader::{Auth, GCSReader};

use crate::header::Header;
use crate::reader::collector::Collector;
use crate::reader::datasource::DataSource;

pub type Sample = (String, Bytes);
pub type RcHeader = Rc<Header>;

#[derive(Default)]
pub struct Reader {
    collector: Collector,
    header: Option<RcHeader>,
    datasource: Option<DataSource>,
}

impl Reader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_file(&mut self, path: &str) -> Result<&mut Self> {
        let mut data = File::open(path).wrap_err_with(|| format!("Failed to open file from {}", path))?;
        let header = Header::read(&mut data)?;
        self.header = Some(Rc::new(header));
        self.datasource = Some(DataSource::new_sync(data));
        Ok(self)
    }

    pub fn by_size(&mut self, size: usize) -> &mut Self {
        self.collector.by_size(size);
        self
    }

    pub fn by_count(&mut self, count: usize) -> &mut Self {
        self.collector.by_count(count);
        self
    }

    pub fn with_shuffling(&mut self, seed: Option<u64>) -> &mut Self {
        self.collector.with_shuffling(seed);
        self
    }

    pub fn with_sharding(&mut self, rank: u16, world_size: u16) -> Result<&mut Self> {
        self.collector.with_sharding(rank, world_size)?;
        Ok(self)
    }

    pub fn with_buffering(&mut self, buffer_size: u32) -> Result<&mut Self> {
        ensure!(
            self.datasource.as_ref().unwrap().is_async(),
            eyre!("Buffering is only supported for async datasources")
        );
        self.collector.with_buffering(buffer_size);
        Ok(self)
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = Sample>> {
        let header = self.header.clone().ok_or(eyre!("Unopened"))?;
        let datasource = self.datasource.clone().unwrap();
        Ok(self.collector.iter(header, datasource))
    }
}

#[cfg(feature = "gcs")]
impl Reader {
    pub fn open_gcs(&mut self, uri: &str) -> Result<&mut Self> {
        let mut data = GCSReader::from_uri(uri, Auth::default())?;
        let header = Header::read(&mut data)?;
        self.header = Some(Rc::new(header));
        self.datasource = Some(DataSource::new_async(data));
        Ok(self)
    }
}

/*
#[cfg(test)]
mod tests {
    use crate::test_setup::setup;
    use super::*;

    #[test]
    #[cfg(feature = "gcs")]
    fn test_gcs_reader() {
        setup();
        let mut reader = Reader::new();
        reader
            .open_gcs("gs://test-rand_archive/dummy.raa")
            .unwrap()
            .by_count(64)
            .with_buffering(32)
            .unwrap()
            .iter()
            .unwrap()
            .for_each(|_| {});
    }
}
*/