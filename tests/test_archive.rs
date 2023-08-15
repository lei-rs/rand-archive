use std::assert_eq;
use std::fs::File;
use rand_archive::archive::{EntryMetadata, Header};


#[test]
fn test_header_read_write() {
    color_eyre::install().unwrap();
    let path = "tests/cache/test_header_read_write.ra";
    let mut header = Header::default();
    let entry = EntryMetadata::try_new(0, 100).unwrap();
    header.insert("dummy", entry).unwrap();
    header.write(path).unwrap();
    let header_back = Header::read(path).unwrap();
    assert_eq!(header.entries.get("dummy").unwrap(), header_back.entries.get("dummy").unwrap());
}

