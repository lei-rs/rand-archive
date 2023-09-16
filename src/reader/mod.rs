use std::io::{Read, Seek, SeekFrom};
use std::rc::Rc;
use std::cell::RefCell;

pub(crate) use block::Block;
use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
use rand::seq::SliceRandom;
use thiserror::Error;

use crate::header::{EntryMetadata, Header};

mod block;
mod collector;
pub mod readers;
