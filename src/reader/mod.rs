use std::io::{Read, Seek, SeekFrom};

pub(crate) use block::Block;
use color_eyre::eyre::{ensure, eyre, Result, WrapErr};
use rand::seq::SliceRandom;

use crate::header::{EntryMetadata, Header};

mod block;
mod collector;
pub mod reader;
