#[cfg(test)]
mod tests {
    use std::assert_eq;
    use std::fs;

    use rand_archive::block::Block;
    use rand_archive::collector::Collector;
    use rand_archive::header::{EntryMetadata, Header};

    use crate::utils::setup;

    #[test]
    fn collector_collect() {
        setup();
        let path = "tests/cache/test_collector_collect.raa";

        let mut collector = Collector::try_new(path.to_string(), 100, 1000);
        collector.collect(Block::new("dummy", &[0u8; 100])).unwrap();
        collector.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.get_key("dummy").unwrap(),
            &EntryMetadata::try_new(0, 100).unwrap()
        );

        fs::remove_file(path).unwrap();
    }

    // Add more tests for other methods and edge cases here
}
