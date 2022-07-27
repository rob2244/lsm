use std::path::Path;

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
/// A background thread will run occasionally and trigger LSM tree compaction.
/// In order to do this, it will read the data from the existing LSM file
/// and write out a new file with all the data ordered correctly
mod memtable;
use memtable::{DataRecord, MemTableManager};

fn main() {
    let mut mtm = MemTableManager::new(Path::new("data").to_path_buf()).unwrap();

    // for i in 1..100 {
    //     if let Err(err) = mtm.write(
    //         DataRecord::new(i,
    //                         format!("user#{}", i),
    //                         format!("user#{}@gmail.com", i),
    //                         "1234567892".to_string())
    //             .unwrap()) {
    //         println!("{}", err)
    //     }
    // }

    // let record = mtm.read(12);
    // println!("{:?}", record);
    mtm.merge_files(&Path::new("C:\\Users\\roseitz\\source\\repos\\lsm\\main\\data\\0a691312-d36f-4344-9c31-1f12266e2a90"), 
                    &Path::new("C:\\Users\\roseitz\\source\\repos\\lsm\\main\\data\\19752a4b-19c6-462e-8755-7489332215fb")).unwrap()
}
