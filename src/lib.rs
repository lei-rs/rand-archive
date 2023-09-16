use color_eyre::eyre::{ensure, eyre, Result, WrapErr};

pub mod archive;
pub mod header;
mod python;
pub mod reader;

#[cfg(test)]
mod test_setup {
    use std::env;
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub(crate) fn setup() {
        INIT.call_once(|| {
            color_eyre::install().unwrap();
            env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "creds.json");
        });
    }
}
