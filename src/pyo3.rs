use pyo3::{IntoPy, pyclass, PyObject, Python};
use crate::archive::EntryMetadata;


#[pyclass(name = "EntryMetadata")]
#[derive(Clone, Debug)]
pub struct PyEntryMetadata {
	pub start: usize,
	pub end: usize,
}

impl From<EntryMetadata> for PyEntryMetadata {
	fn from(entry: EntryMetadata) -> Self {
		Self {
			start: entry.start,
			end: entry.end,
		}
	}
}
