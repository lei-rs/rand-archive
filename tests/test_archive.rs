mod tests {
    use rand_archive::archive::{ArchiveWriter, EntryMetadata, Header};
    use std::sync::Once;
    use std::{assert_eq, fs};

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            color_eyre::install().unwrap();
        });
    }

    #[test]
    fn archive_write() {
        setup();
        let path = "tests/cache/test_archive_write.raa";

        let mut archive = ArchiveWriter::new(path.to_string(), 100);
        archive.write("dummy", &[0u8; 100]).unwrap();
        archive.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.entries.get("dummy").unwrap(),
            &EntryMetadata::try_new(0, 100).unwrap()
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn archive_read() {
        setup();
        let path = "tests/cache/test_archive_read.raa";

        let mut archive = ArchiveWriter::new(path.to_string(), 100);
        archive.write("dummy", &[0u8; 100]).unwrap();
        archive.close().unwrap();

        let mut archive = ArchiveWriter::read(path, 100).unwrap();
        archive.write("dummy2", &[0u8; 100]).unwrap();
        archive.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.entries.get("dummy").unwrap(),
            &EntryMetadata::try_new(0, 100).unwrap()
        );
        assert_eq!(
            header.entries.get("dummy2").unwrap(),
            &EntryMetadata::try_new(100, 200).unwrap()
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn archive_double_flush() {
        setup();
        let path = "tests/cache/archive_flush.raa";

        let mut archive = ArchiveWriter::new(path.to_string(), 100);
        archive.write("dummy", &[0u8; 101]).unwrap();
        archive.write("dummy2", &[0u8; 101]).unwrap();
        archive.close().unwrap();

        let header = Header::read(path).unwrap();
        assert_eq!(
            header.entries.get("dummy").unwrap(),
            &EntryMetadata::try_new(0, 101).unwrap()
        );
        assert_eq!(
            header.entries.get("dummy2").unwrap(),
            &EntryMetadata::try_new(101, 202).unwrap()
        );

        fs::remove_file(path).unwrap();
    }
}
