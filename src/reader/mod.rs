use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::rc::Rc;

use block::Block;
use bytes::{Bytes, BytesMut};
pub(crate) use collector::Collector;
use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
use rand::seq::SliceRandom;
pub use readers::{DataSource, Reader, Sample};
use thiserror::Error;

use crate::header::Header;

mod block;
mod collector;
pub mod readers;
