use std::fs::File;

use super::*;
use crate::reader::collector::{Collector, CollectorCriteria};

#[cfg(feature = "gcs")]
use gcs_reader::{Auth, GCSReader};

#[cfg(feature = "s3")]
use s3reader::{S3ObjectUri, S3Reader};

#[derive(Error, Debug)]
pub enum ShardingError {
    #[error("rank must be less than world_size, got rank: {0}, world_size: {1}")]
    InvalidRank(u16, u16),
    #[error("world_size must be greater than 0, got world_size: {0}")]
    InvalidWorldSize(u16),
}

pub struct Reader<D: Read + Seek> {
    header: Rc<Header>,
    data: Rc<RefCell<D>>,
    collector: Collector,
}

impl<D: Read + Seek> Reader<D> {
    pub fn with_shuffling(&mut self) -> &mut Self {
        self.collector.shuffle = true;
        self
    }

    pub fn by_size(&mut self, size: usize) -> &mut Self {
        self.collector.criteria = CollectorCriteria::Size(size);
        self
    }

    pub fn by_count(&mut self, count: usize) -> &mut Self {
        self.collector.criteria = CollectorCriteria::Count(count);
        self
    }

    pub fn with_sharding(&mut self, rank: u16, world_size: u16) -> Result<&mut Self> {
        ensure!(rank < world_size, ShardingError::InvalidRank(rank, world_size));
        ensure!(world_size > 0, ShardingError::InvalidWorldSize(world_size));
        self.collector.shard = Some((rank, world_size));
        Ok(self)
    }

    pub fn to_iter(&self) -> impl Iterator<Item = (String, Vec<u8>)> {
        self.collector.to_iter(&self.header, &self.data)
    }
}

impl Reader<File> {
    pub fn open(path: &str) -> Result<Self> {
        let mut data = File::open(path).wrap_err_with(|| format!("Failed to open file from {}", path))?;
        let header = Header::read(&mut data)?;
        let collector = Collector::default();
        Ok(Self {
            header: Rc::new(header),
            data: Rc::new(RefCell::new(data)),
            collector,
        })
    }
}

#[cfg(feature = "gcs")]
impl Reader<GCSReader> {
    pub fn open_gcs(uri: &str) -> Result<Self> {
        let mut data = GCSReader::from_uri(uri, Auth::default())?;
        let header = Header::read(&mut data)?;
        let collector = Collector::default();
        Ok(Self {
            header: Rc::new(header),
            data: Rc::new(RefCell::new(data)),
            collector,
        })
    }
}

#[cfg(feature = "s3")]
impl Reader<S3Reader> {
    pub fn open_s3(uri: &str) -> Result<Self> {
        let uri_obj = S3ObjectUri::new(uri).wrap_err_with(|| format!("Failed to parse S3 URI {}", uri))?;
        let mut data = S3Reader::open(uri_obj).wrap_err_with(|| format!("Failed to open file from {}", uri))?;
        let header = Header::read(&mut data)?;
        let collector = Collector::default();
        Ok(Self {
            header: Rc::new(header),
            data: Rc::new(RefCell::new(data)),
            collector,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::test_setup::setup;

    /*
    #[test]
    fn temp() {
        setup();
        let mut reader = Reader::open("localdata/test.raa").unwrap();
        reader.by_count(10);

        let start_time = Instant::now();
        let mut count = 0;
        for _ in reader.to_iter() {
            count += 1
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        let iterations_per_second = count as f64 / elapsed;
        println!("Iterations per second: {}", iterations_per_second);
        println!("Elapsed: {}", elapsed);
        println!("Count: {}", count)
    }
    */
}
