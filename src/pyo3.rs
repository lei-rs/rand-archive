use color_eyre::eyre::Result;
use indexmap::IndexMap;
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

use crate::archive::ArchiveWriter;
use crate::header::{EntryMetadata, Header};

const DEF_CACHE_SIZE: usize = 100 * 1024 * 1024;
const DEF_HEADER_SIZE: usize = 1024 * 1024;

impl IntoPy<PyObject> for EntryMetadata {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTuple::new(py, [self.start_idx, self.length]).into_py(py)
    }
}

#[pyclass(name = "Header")]
#[derive(Clone, Debug)]
pub struct PyHeader {
    inner: Header,
}

#[pymethods]
impl PyHeader {
    #[classmethod]
    pub fn calc_header_size(cls: &PyType, key_size: usize, n_entries: usize) -> usize {
        n_entries * (key_size * 8 + 16)
    }

    #[classmethod]
    pub fn read(cls: &PyType, path: &str) -> PyResult<Self> {
        let inner = Header::read(path)?;
        Ok(PyHeader { inner })
    }

    pub fn inner(&self, py: Python) -> IndexMap<String, PyObject> {
        let mut im = IndexMap::new();
        for (key, value) in self.inner.entries().iter() {
            im.insert(key.clone(), value.clone().into_py(py));
        }
        im
    }
}

#[pyclass(name = "ArchiveWriter")]
#[derive(Clone, Debug)]
pub struct PyArchiveWriter {
    inner: ArchiveWriter,
}

#[pymethods]
impl PyArchiveWriter {
    #[new]
    #[pyo3(signature = (path, cache_size=DEF_CACHE_SIZE, max_header_size=DEF_HEADER_SIZE))]
    pub fn new(path: String, cache_size: usize, max_header_size: usize) -> Result<Self> {
        Ok(Self {
            inner: ArchiveWriter::try_new(path, cache_size, max_header_size)?,
        })
    }

    #[classmethod]
    #[pyo3(signature = (path, cache_size=DEF_CACHE_SIZE))]
    pub fn load(cls: &PyType, path: &str, cache_size: usize) -> Result<Self> {
        let inner = ArchiveWriter::load(path, cache_size)?;
        Ok(PyArchiveWriter { inner })
    }

    pub fn write(&mut self, key: &str, value: &[u8]) -> PyResult<()> {
        self.inner.write(key, value).map_err(|e| e.into())
    }

    pub fn close(&mut self) -> PyResult<()> {
        self.inner.close().map_err(|e| e.into())
    }
}

#[pymodule]
fn rand_archive(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyHeader>()?;
    m.add_class::<PyArchiveWriter>()?;
    Ok(())
}
