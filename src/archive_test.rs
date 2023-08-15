use std::fs::File;
use std::io::Write;
use std::assert_eq;
use tempfile::tempfile;
use crate::archive::{Header, EntryMetadata};

#[test]
fn test_header_read_write() {
    // Create a dummy Header and EntryMetadata
    let mut header = Header::default();
    let entry = EntryMetadata::try_new(0, 100).unwrap();
    header.insert("dummy", entry).unwrap();

    // Write the Header to a temporary file
    let mut file = tempfile().unwrap();
    header.write(file.path().to_str().unwrap()).unwrap();

    // Read the Header back from the temporary file
    let header_back = Header::read(file.path().to_str().unwrap()).unwrap();

    // Compare the original and the read back Header
    assert_eq!(header, header_back);
}

