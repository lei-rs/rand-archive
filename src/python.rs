use std::fs::{File, OpenOptions};

use indexmap::IndexMap;
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

use super::*;
use crate::archive::ArchiveWriter;
use crate::header::{EntryMetadata, Header};
use crate::reader::readers::Reader;

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
    fn calc_header_size(cls: &PyType, key_size: usize, n_entries: usize) -> usize {
        n_entries * (key_size * 8 + 16)
    }

    #[classmethod]
    fn read(cls: &PyType, path: &str) -> PyResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .wrap_err_with(|| format!("Failed to open file from {}", path))
            .unwrap();
        let inner = Header::read(&mut file)?;
        Ok(PyHeader { inner })
    }

    fn inner(&self, py: Python) -> IndexMap<String, PyObject> {
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
    fn new(path: String, cache_size: usize, max_header_size: usize) -> Result<Self> {
        Ok(Self {
            inner: ArchiveWriter::new(path, cache_size, max_header_size)?,
        })
    }

    #[classmethod]
    #[pyo3(signature = (path, cache_size=DEF_CACHE_SIZE))]
    fn load(cls: &PyType, path: &str, cache_size: usize) -> Result<Self> {
        let inner = ArchiveWriter::load(path, cache_size)?;
        Ok(PyArchiveWriter { inner })
    }

    fn write(&mut self, key: &str, value: &[u8]) -> PyResult<()> {
        self.inner.write(key, value).map_err(|e| e.into())
    }

    fn close(&mut self) -> PyResult<()> {
        self.inner.close().map_err(|e| e.into())
    }
}

#[pyclass(unsendable)]
struct EntryIter {
    iter: Option<Box<dyn Iterator<Item = (String, Vec<u8>)>>>,
}

#[pymethods]
impl EntryIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        match slf.iter.as_mut().unwrap().next() {
            Some((key, value)) => {
                Python::with_gil(|gil| {
                    let key = key.to_object(gil);
                    let value = value.to_object(gil);
                    let tuple = PyTuple::new(gil, [key, value]);
                    Some(tuple.into_py(gil))
                })
            },
            None => None,
        }
    }
}

#[pyclass(name = "FileReader", unsendable)]
struct PyReader {
    inner: Box<dyn Reader>,
}

#[pymethods]
impl PyReaderFile {
    #[new]
    fn new(path: &str) -> Result<Self> {
        let inner = Reader::open(path)?;
        Ok(Self { inner })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyResult<Py<EntryIter>> {
        let iter = EntryIter {
            iter: Some(Box::new(slf.inner.to_iter())),
        };
        Py::new(slf.py(), iter)
    }
}

#[pymodule]
fn rand_archive(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyHeader>()?;
    m.add_class::<PyArchiveWriter>()?;
    m.add_class::<EntryIter>()?;
    m.add_class::<PyReaderFile>()?;
    Ok(())
}
