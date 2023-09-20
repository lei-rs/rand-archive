use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;

use bytes::Bytes;
use color_eyre::eyre::{eyre, Result};

use crate::reader::datasource::{AsyncDataSource, SyncDataSource};
use crate::reader::readers::RcHeader;
use crate::reader::Sample;

#[derive(Clone, Debug)]
pub(crate) struct Block {
    header: RcHeader,
    range: Range<usize>,
    buffer: Option<Bytes>,
}

impl Block {
    pub(crate) fn from_range(header: RcHeader, range: Range<usize>) -> Self {
        let header = header.clone();
        Self {
            header,
            range,
            buffer: None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    pub(crate) fn read(mut self, data_source: Rc<RefCell<dyn SyncDataSource>>) -> Result<Self> {
        let data_source = &mut *data_source.borrow_mut();
        let byte_range = self.header.byte_range_of(&self.range).ok_or(eyre!("Invalid range"))?;
        self.buffer = Some(data_source.get_range(byte_range)?);
        Ok(self)
    }

    pub(crate) async fn read_async(mut self, data_source: Rc<dyn AsyncDataSource>) -> Result<Self> {
        let byte_range = self.header.byte_range_of(&self.range).ok_or(eyre!("Invalid range"))?;
        self.buffer = Some(data_source.get_range_async(byte_range).await?);
        Ok(self)
    }

    pub(crate) fn to_vec(&self) -> Result<Vec<Sample>> {
        let mut data = self.buffer.clone().ok_or(eyre!("Unread block"))?;
        Ok(self
            .header
            .get_range(self.range.clone())
            .ok_or(eyre!("Invalid range"))?
            .iter()
            .map(|(key, entry)| (key.to_owned(), data.split_to(entry.length())))
            .collect())
    }
}
