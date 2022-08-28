use std::{path::Path, time::Duration};

mod bloomfilter;
/// LSM Tree Outline
///
/// LSM Tree Schema:
/// Data records in the LSM tree will have a strict schema with
/// all the data types being fixed in size and comprised of the following:
/// 	- an unsigned 32 bit integer for the key
/// 	- a 255 byte username field
///     - a 255 byte email field
/// 	- a 10 byte phone number field
/// Unfilled space in this schema will remain empty
///
/// LSM Tree Mem Table
/// The mem table will be represented in memory as a sorted map,
/// the keys will be the record key and the value will be the record value.
/// Once the map reaches a size of 16 kb the mem table will be closed and a
/// new one will be opened. The closed mem table will then be written to disk.
/// Each mem table will get it's own file, but this may change later
///
/// LSM Tree Disk Representation
/// On disk the LSM tree will be serialized into binary, and each cell will have the
/// following format
/// | key 4 bytes | username 255 bytes | email 255 bytes | phone_number 10 bytes |
/// A 16 byte file header containing a 16 byte Bloom filter, indicating which keys are present in the
/// record. There is currently no file footer
///
/// LSM Tree compaction
// There is a folder with all the mem table files in it.
// Periodically a thread should run to decide whether or not 
// files should be merged based on a MergePredicate. If the thread finds 
// that two files should be merged, it should call merge_files. It should leave
// the original files remain accessible for reads by other threads until 
// the merging is done at which point the mergingin thread waits for any reads to
// the old files to finish then deletes them
// This may require some sort of reader/writer lock scheme on shared file handles
mod memtable;
use memtable::{DataRecord, MemTableManager};

fn main() {
    let mut mtm = 
        MemTableManager::new(Path::new("data").to_path_buf(), 
            | _ | true, 
            Duration::from_secs(60)).unwrap();

    for i in 1..1000 {
        if let Err(err) = mtm.write(
            DataRecord::new(i,
                            format!("user#{}", i),
                            format!("user#{}@gmail.com", i),
                            "1234567892".to_string())
                .unwrap()) {
            println!("{}", err)
        }
    }

    let record = mtm.read(12);
    println!("{:?}", record);

    loop { }
}
