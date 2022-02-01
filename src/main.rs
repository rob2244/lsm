use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

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
/// Initially there will be no mem table headers or footers, but this may change later
///
/// LSM Tree compaction
/// A background thread will run occassionally and trigger LSM tree compaction.
/// In order to do this, it will read the data from the existing LSM file
/// and write out a new file with all the data ordered correctly

/// Mem table constants when serialized to binary
const KEY_OFFSET: usize = 0;
const KEY_LENGTH_BYTES: usize = 4;

const USERNAME_OFFSET: usize = 4;
const USERNAME_LENGTH_BYTES: usize = 255;

const EMAIL_OFFSET: usize = 259;
const EMAIL_LENGTH_BYTES: usize = 255;

const PHONE_NUMBER_OFFSET: usize = 514;
const PHONE_NUMBER_LENGTH_BYTES: usize = 10;

const RECORD_LENGTH_BYTES: usize = 524;

// 16 kb / 524 bytes is approximatley 31 records per memtable
const MEM_TABLE_SIZE: usize = 31;

#[derive(Debug)]
struct DataRecord {
	id: u32,
	username: String,
	email: String,
	phone_number: String,
}

struct MemTable {
	buffer: BTreeMap<u32, DataRecord>,
	length: usize,
}

impl MemTable {
	fn insert(&mut self, record: DataRecord) -> Result<(), &'static str> {
		if self.buffer.len() >= self.length {
			return Err("Mem Table is full, TODO schedule write to disk");
		}

		self.buffer.insert(record.id, record);
		self.length += 1;

		Ok(())
	}

	fn serialize(&self) -> Vec<u8> {
		let mut serialized = Vec::with_capacity(MEM_TABLE_SIZE * 8);

		for (_, value) in &self.buffer {
			let mut buf = vec![0; RECORD_LENGTH_BYTES];
			buf.write_u32::<LittleEndian>(value.id).unwrap();

			let username = value.username.as_bytes();
			buf[USERNAME_OFFSET..USERNAME_OFFSET + username.len()].copy_from_slice(username);

			let email = value.email.as_bytes();
			buf[EMAIL_OFFSET..EMAIL_OFFSET + email.len()].copy_from_slice(email);

			let phone_number = value.phone_number.as_bytes();
			buf[PHONE_NUMBER_OFFSET..PHONE_NUMBER_OFFSET + phone_number.len()]
				.copy_from_slice(phone_number);

			serialized.append(&mut buf)
		}

		serialized
	}

	fn print(&self) {
		for (k, v) in &self.buffer {
			println!("key: '{}' - value: '{:?}'", k, v);
		}
	}

	fn new(length: usize) -> MemTable {
		MemTable {
			buffer: BTreeMap::new(),
			length: length,
		}
	}

	fn parse_from_binary(bytes: &[u8]) -> Result<DataRecord, String> {
		if bytes.len() != RECORD_LENGTH_BYTES {
			return Err(format!(
				"Unable to parse form binary expected length to be: '{}' but got: '{}'",
				RECORD_LENGTH_BYTES,
				bytes.len()
			));
		}

		let mut rdr = Cursor::new(bytes);

		let id = rdr
			.read_u32::<LittleEndian>()
			.expect("Unable to parse id, record is corrupted");

		let username =
			std::str::from_utf8(&bytes[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_LENGTH_BYTES])
				.expect("Unable to parse username, record is corrupted");
		let email = std::str::from_utf8(&bytes[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_LENGTH_BYTES])
			.expect("Unable to parse email, record is corrupted");
		let phone_number = std::str::from_utf8(
			&bytes[PHONE_NUMBER_OFFSET..PHONE_NUMBER_OFFSET + PHONE_NUMBER_LENGTH_BYTES],
		)
		.expect("Unable to parse phone number, record is corrupted");

		Ok(DataRecord {
			id: id,
			username: username.to_string(),
			email: email.to_string(),
			phone_number: phone_number.to_string(),
		})
	}

	/// from_slice assumes every memtable is written to disk when full.
	/// If a mem table is written partially it will fail
	fn from_slice(bytes: &[u8]) -> Result<MemTable, std::io::Error> {
		let num_records = bytes.len() / RECORD_LENGTH_BYTES;

		if num_records != MEM_TABLE_SIZE {
			panic!(
				"Expected to find '{}' records in memtable but got: '{}'",
				MEM_TABLE_SIZE, num_records
			);
		}

		let mut table = MemTable::new(MEM_TABLE_SIZE);

		for i in 0..num_records {
			if let Ok(data_record) = MemTable::parse_from_binary(
				&bytes[i * RECORD_LENGTH_BYTES..i * RECORD_LENGTH_BYTES + RECORD_LENGTH_BYTES],
			) {
				table.insert(data_record).unwrap()
			} else {
				panic!("Corrupt data record found, index: '{}'", i)
			}
		}

		Ok(table)
	}
}

struct MemTableManager {
	memtable_dir: PathBuf,
}

impl MemTableManager {
	fn new(memtable_dir: PathBuf) -> MemTableManager {
		MemTableManager {
			memtable_dir: memtable_dir,
		}
	}

	fn flush_memtable(&self, tbl: MemTable) -> std::io::Result<()> {
		let mut f = OpenOptions::new()
			.write(true)
			.append(true)
			.create(true)
			.open(self.memtable_dir.join("table1"))?;

		f.write_all(&tbl.serialize())?;

		Ok(())
	}

	fn fetch_memtable(self, memtable_id: &str) -> Result<MemTable, std::io::Error> {
		let mut f = OpenOptions::new()
			.read(true)
			.open(self.memtable_dir.join(memtable_id))?;

		let mut buf = Vec::with_capacity(MEM_TABLE_SIZE);

		f.read_to_end(&mut buf)?;

		MemTable::from_slice(&buf)
	}
}

fn main() {
	let mut memtable = MemTable::new(MEM_TABLE_SIZE);

	for i in 1..100 {
		if let Err(err) = memtable.insert(DataRecord {
			id: i,
			username: format!("usern#{}", i),
			email: format!("user#{}@gmail.com", i),
			phone_number: "1234567892".to_string(),
		}) {
			println!("{}", err)
		}
	}

	memtable.print();

	let manager = MemTableManager::new(Path::new("data").to_path_buf());
	let _ = manager.flush_memtable(memtable);

	let mt = manager.fetch_memtable("table1").unwrap();
	println!("Hello World")
}
