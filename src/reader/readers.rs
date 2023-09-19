use std::fs::File;

#[cfg(feature = "gcs")]
use gcs_reader::{Auth, GCSReader};
#[cfg(feature = "s3")]
use s3reader::{S3ObjectUri, S3Reader};

use super::*;
use crate::reader::collector::Collector;

pub type Sample = (String, Bytes);
pub type HeaderRc = Rc<Header>;
pub type DataSourceRc = Rc<RefCell<dyn DataSource>>;

pub trait DataSource {
    fn get_range(&mut self, range: Range<usize>) -> Result<Bytes>;
}

impl<T: Read + Seek + 'static> DataSource for T {
    fn get_range(&mut self, range: Range<usize>) -> Result<Bytes> {
        let mut buf = BytesMut::zeroed(range.len());
        self.seek(SeekFrom::Start(range.start as u64))?;
        self.read_exact(&mut buf)?;
        Ok(buf.freeze())
    }
}

#[derive(Default)]
pub struct Reader {
    collector: Collector,
    header: Option<HeaderRc>,
    datasource: Option<DataSourceRc>,
}

impl Reader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_file(&mut self, path: &str) -> Result<&mut Self> {
        let mut data = File::open(path).wrap_err_with(|| format!("Failed to open file from {}", path))?;
        let header = Header::read(&mut data)?;
        self.header = Some(Rc::new(header));
        self.datasource = Some(Rc::new(RefCell::new(data)));
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

    pub fn with_shuffling(&mut self) -> &mut Self {
        self.collector.with_shuffling();
        self
    }

    pub fn with_sharding(&mut self, rank: u16, world_size: u16) -> Result<&mut Self> {
        self.collector.with_sharding(rank, world_size)?;
        Ok(self)
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = Sample>> {
        let header = self.header.clone().ok_or(eyre!("Unopened"))?;
        let datasource = self.datasource.clone().unwrap();
        self.collector.iter(header, datasource)
    }
}

#[cfg(feature = "gcs")]
impl Reader {
    pub fn open_gcs(&mut self, uri: &str) -> Result<&mut Self> {
        let mut data = GCSReader::from_uri(uri, Auth::default())?;
        let header = Header::read(&mut data)?;
        self.header = Some(Rc::new(header));
        self.datasource = Some(Rc::new(RefCell::new(data)));
        Ok(self)
    }
}

#[cfg(feature = "s3")]
impl Reader {
    pub fn open_s3(&mut self, uri: &str) -> Result<&mut Self> {
        let uri_obj = S3ObjectUri::new(uri).wrap_err_with(|| format!("Failed to parse S3 URI {}", uri))?;
        let mut data = S3Reader::open(uri_obj).wrap_err_with(|| format!("Failed to open file from {}", uri))?;
        let header = Header::read(&mut data)?;
        self.header = Some(Rc::new(header));
        self.datasource = Some(Rc::new(RefCell::new(data)));
        Ok(self)
    }
}
