use std::sync::Once;

pub mod archive;
pub mod header;
mod pyo3;
pub mod reader;

static INIT: Once = Once::new();

pub(crate) fn setup() {
    INIT.call_once(|| {
        color_eyre::install().unwrap();
    });
}
