#[cfg(test)]
mod tests {
    use crate::utils::setup;
    use rand_archive::block::Block;
    use rand_archive::reader::Reader;
    use std::fs;

    #[test]
    fn test_reader_read() {
        setup();
        let path = "tests/cache/test_reader_read.raa";
        let block = Block::new(path.to_string(), 100);
        block.write("dummy", &[0u8; 100]).unwrap();
        block.close().unwrap();

        let reader = Reader::new(path);
        let data = reader.read("dummy").unwrap();
        assert_eq!(data, &[0u8; 100]);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_reader_read_invalid_key() {
        setup();
        let path = "tests/cache/test_reader_read_invalid_key.raa";
        let block = Block::new(path.to_string(), 100);
        block.write("dummy", &[0u8; 100]).unwrap();
        block.close().unwrap();

        let reader = Reader::new(path);
        let result = reader.read("invalid");
        assert!(result.is_err());

        fs::remove_file(path).unwrap();
    }

    // Add more tests for other methods and edge cases here
}
