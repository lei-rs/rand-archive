mod utils;
#[cfg(test)]
mod tests {
    use std::{assert_eq, fs};

    use rand_archive::archive::ArchiveWriter;
    use rand_archive::header::{EntryMetadata, Header};
    use crate::utils::setup;

    #[test]
    fn archive_write() {
        setup();
        let path = "tests/cache/test_archive_write.raa";

        let mut archive = ArchiveWriter::new(path.to_string(), 100, 1000);
        archive.write("dummy", &[0u8; 100]).unwrap();
        archive.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.get("dummy").unwrap(),
            &EntryMetadata::try_new(0, 100).unwrap()
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn archive_read() {
        setup();
        let path = "tests/cache/test_archive_read.raa";

        let mut archive = ArchiveWriter::new(path.to_string(), 100, 1000);
        archive.write("dummy", &[0u8; 100]).unwrap();
        archive.close().unwrap();

        let mut archive = ArchiveWriter::read(path, 100).unwrap();
        archive.write("dummy2", &[0u8; 100]).unwrap();
        archive.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.get("dummy").unwrap(),
            &EntryMetadata::try_new(0, 100).unwrap()
        );
        assert_eq!(
            header.get("dummy2").unwrap(),
            &EntryMetadata::try_new(100, 100).unwrap()
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn archive_double_flush() {
        setup();
        let path = "tests/cache/archive_flush.raa";

        let mut archive = ArchiveWriter::new(path.to_string(), 100, 1000);
        archive.write("dummy", &[0u8; 101]).unwrap();
        archive.write("dummy2", &[0u8; 101]).unwrap();
        archive.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.get("dummy").unwrap(),
            &EntryMetadata::try_new(0, 101).unwrap()
        );
        assert_eq!(
            header.get("dummy2").unwrap(),
            &EntryMetadata::try_new(101, 101).unwrap()
        );

        fs::remove_file(path).unwrap();
    }
}
