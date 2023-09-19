use color_eyre::eyre::{bail, ensure, eyre, Result, WrapErr};
use thiserror::Error;

pub mod archive;
pub mod header;
mod python;
pub mod reader;

#[cfg(test)]
pub mod test_setup {
    use std::env;
    use std::sync::Once;

    use rand::Rng;
    use tempfile::tempfile;

    use super::*;
    use crate::archive::Writer;

    static INIT: Once = Once::new();

    pub fn setup() {
        INIT.call_once(|| {
            color_eyre::install().unwrap();
            env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "creds.json");
        });
    }

    pub fn generate_random_key(length: usize) -> String {
        let charset: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::thread_rng();
        (0..length)
            .map(|_| {
                let idx = rng.gen_range(0..charset.len());
                charset[idx] as char
            })
            .collect()
    }

    pub fn generate_random_value(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..size).map(|_| rng.gen::<u8>()).collect()
    }

    pub fn new_dummy_file(entries_count: usize, value_size: usize) -> Result<Writer> {
        let file = tempfile()?;
        let mut writer = Writer::new(file.try_clone()?, 1024, 10 * 1024)?;

        for _ in 0..entries_count {
            let key = generate_random_key(6);
            let value = generate_random_value(value_size);
            writer.write(&key, &value)?;
        }

        writer.close()?;
        Ok(writer)
    }
}
