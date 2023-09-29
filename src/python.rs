use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple, PyType};
use pyo3::PyErr;

use super::*;
use crate::archive::Writer;
use crate::header::{Header, SampleMD};
use crate::reader::Reader;

const DEF_CACHE_SIZE: usize = 100 * 1024 * 1024;
const DEF_HEADER_SIZE: usize = 1024 * 1024;

impl IntoPy<PyObject> for SampleMD {
    fn into_py(self, py: Python) -> PyObject {
        PyTuple::new(py, [self.start_idx(), self.length()]).into_py(py)
    }
}

#[pyclass(name = "Header")]
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
    fn load(cls: &PyType, path: &str) -> PyResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .wrap_err_with(|| format!("Failed to open file from {path}"))
            .unwrap();
        let inner = Header::read(&mut file)?;
        Ok(PyHeader { inner })
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.inner.entries().contains_key(key)
    }

    fn __getitem__(&self, py: Python, key: &str) -> PyResult<PyObject> {
        self.inner
            .get_key(key)
            .ok_or(PyErr::new::<PyKeyError, _>(format!("Key {key} not found")))
            .map(|v| v.clone().into_py(py))
    }
}

#[pyclass(name = "Writer")]
pub struct PyWriter {
    inner: Writer,
}

#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature = (path, cache_size=DEF_CACHE_SIZE, max_header_size=DEF_HEADER_SIZE))]
    fn new(path: String, cache_size: usize, max_header_size: usize) -> Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self {
            inner: Writer::new(file, cache_size, max_header_size)?,
        })
    }

    #[classmethod]
    #[pyo3(signature = (path, cache_size=DEF_CACHE_SIZE))]
    fn load(cls: &PyType, path: &str, cache_size: usize) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let inner = Writer::load(file, cache_size)?;
        Ok(PyWriter { inner })
    }

    fn write(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.inner.write(key, value)
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(mut slf: PyRefMut<'_, Self>, exc_type: &PyAny, exc_value: &PyAny, exc_traceback: &PyAny) -> Result<()> {
        slf.close()
    }
}

#[pyclass[name = "Reader", unsendable]]
struct PyReader {
    inner: Reader,
}

#[pymethods]
impl PyReader {
    #[new]
    fn new() -> Self {
        Self { inner: Reader::new() }
    }

    fn open_file<'a>(mut slf: PyRefMut<'a, Self>, path: &str) -> Result<PyRefMut<'a, Self>> {
        slf.inner.open_file(path)?;
        Ok(slf)
    }

    #[cfg(feature = "gcs")]
    fn open_gcs<'a>(mut slf: PyRefMut<'a, Self>, uri: &str) -> Result<PyRefMut<'a, Self>> {
        slf.inner.open_gcs(uri)?;
        Ok(slf)
    }

    fn by_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.inner.by_size(size);
        slf
    }

    fn by_count(mut slf: PyRefMut<'_, Self>, count: usize) -> PyRefMut<'_, Self> {
        slf.inner.by_count(count);
        slf
    }

    #[pyo3(text_signature = "(self, /, *, seed=None)")]
    fn with_shuffling(mut slf: PyRefMut<'_, Self>, seed: Option<u64>) -> PyRefMut<'_, Self> {
        slf.inner.with_shuffling(seed);
        slf
    }

    fn with_sharding(mut slf: PyRefMut<'_, Self>, rank: u16, world_size: u16) -> Result<PyRefMut<'_, Self>> {
        slf.inner.with_sharding(rank, world_size)?;
        Ok(slf)
    }

    fn with_buffering(mut slf: PyRefMut<'_, Self>, buffer_size: u32) -> Result<PyRefMut<'_, Self>> {
        slf.inner.with_buffering(buffer_size)?;
        Ok(slf)
    }

    fn __iter__(&self) -> Result<EntryIter> {
        Ok(EntryIter {
            iter: Arc::new(Mutex::new(self.inner.iter()?)),
        })
    }
}

#[pyclass]
struct EntryIter {
    iter: Arc<Mutex<dyn Iterator<Item = (String, Bytes)>>>,
}

#[pymethods]
impl EntryIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        match slf.iter.lock().unwrap().next() {
            Some((key, value)) => Python::with_gil(|gil| {
                let key = key.to_object(gil);
                let value = PyBytes::new(gil, &value).into_py(gil);
                let tuple = PyTuple::new(gil, [key, value]);
                Some(tuple.into_py(gil))
            }),
            None => None,
        }
    }
}

unsafe impl Send for EntryIter {}
unsafe impl Sync for EntryIter {}

#[pyfunction]
fn __setup() -> Result<()> {
    color_eyre::install()
}

#[pymodule]
fn rand_archive(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyHeader>()?;
    m.add_class::<PyWriter>()?;
    m.add_class::<PyReader>()?;
    m.add_class::<EntryIter>()?;
    m.add_wrapped(wrap_pyfunction!(__setup))?;
    Ok(())
}
