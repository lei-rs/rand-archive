use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::rc::Rc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use color_eyre::eyre::Result;
use futures::executor::block_on;
#[cfg(feature = "gcs")]
use gcs_reader::GCSReader;

#[async_trait]
pub trait AsyncDataSource {
    async fn get_range_async(&self, range: Range<usize>) -> Result<Bytes>;
}

#[cfg(feature = "gcs")]
#[async_trait]
impl AsyncDataSource for GCSReader {
    async fn get_range_async(&self, range: Range<usize>) -> Result<Bytes> {
        self.read_range(range.start as u64, range.end as u64).await
    }
}

pub trait SyncDataSource {
    fn get_range(&mut self, range: Range<usize>) -> Result<Bytes>;
}

impl<T: AsyncDataSource> SyncDataSource for T {
    fn get_range(&mut self, range: Range<usize>) -> Result<Bytes> {
        block_on(self.get_range_async(range))
    }
}

impl SyncDataSource for File {
    fn get_range(&mut self, range: Range<usize>) -> Result<Bytes> {
        let mut buf = BytesMut::zeroed(range.len());
        self.seek(SeekFrom::Start(range.start as u64))?;
        self.read_exact(&mut buf)?;
        Ok(buf.freeze())
    }
}

#[derive(Clone)]
pub enum DataSource {
    Sync(Rc<RefCell<dyn SyncDataSource>>),
    Async(Rc<dyn AsyncDataSource>),
}

impl DataSource {
    pub fn new_sync<D: SyncDataSource + 'static>(data_source: D) -> Self {
        Self::Sync(Rc::new(RefCell::new(data_source)))
    }

    pub fn new_async<D: AsyncDataSource + 'static>(data_source: D) -> Self {
        Self::Async(Rc::new(data_source))
    }

    pub fn is_async(&self) -> bool {
        match self {
            Self::Sync(_) => false,
            Self::Async(_) => true,
        }
    }

    pub fn into_async(self) -> Option<Rc<dyn AsyncDataSource>> {
        match self {
            Self::Sync(_) => None,
            Self::Async(inner) => Some(inner),
        }
    }

    pub fn into_sync(self) -> Option<Rc<RefCell<dyn SyncDataSource>>> {
        match self {
            Self::Sync(inner) => Some(inner),
            Self::Async(inner) => None,
        }
    }
}
