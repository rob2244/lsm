use core::cmp::Reverse;
use std::{fmt, fs, io};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use log::{debug, error, info};
use uuid::Uuid;

use crate::bloomfilter::BloomFilter;

const HEADER_OFFSET: usize = 0;
const HEADER_SIZE_BYTES: usize = 16;

const BLOOM_FILTER_OFFSET: usize = 0;
const BLOOM_FILTER_LENGTH_BYTES: usize = 16;

const DATA_OFFSET: usize = 16;
const DATA_LENGTH_BYTES: usize = 31 * 524;

const KEY_OFFSET: usize = 0;
const KEY_LENGTH_BYTES: usize = 4;

const USERNAME_OFFSET: usize = 4;
const USERNAME_LENGTH_BYTES: usize = 255;

const EMAIL_OFFSET: usize = 259;
const EMAIL_LENGTH_BYTES: usize = 255;

const PHONE_NUMBER_OFFSET: usize = 514;
const PHONE_NUMBER_LENGTH_BYTES: usize = 10;

const RECORD_LENGTH_BYTES: usize = 524;

// 16 kb / 524 bytes is approximately 31 records per memtable
const MEM_TABLE_RECORD_COUNT: usize = 31;
const MEM_TABLE_SIZE_BYTES: usize = 16 * 1024;

const BLOOM_FILTER_CAPACITY: usize = 128;
const BLOOM_FILTER_HASH_COUNT: u32 = 6;

const MAX_DISK_RESIDENT_FILESIZE_BYTES: usize = 1 * 1024 * 1024 * 1024;

// TODO: there's a lot of things in this module use Vecs which instead could be using fixed length
// arrays because we know the sizes of the structures

#[derive(Debug, Clone, PartialEq, Eq)]
// TODO add write timestamp field to record and add to Ord and equals trait impl.
pub struct DataRecord {
    id: u32,
    username: String,
    email: String,
    phone_number: String,
}

impl DataRecord {
    pub fn new(
        id: u32,
        username: String,
        email: String,
        phone_number: String,
    ) -> Result<DataRecord, &'static str> {
        if username.len() > 255 {
            Err("Username must have max sie of 255 characters")
        } else if email.len() > 255 {
            Err("email must have max size of 255 characters")
        } else if phone_number.len() > 10 {
            Err("Phone Number must have max size of 10 characters")
        } else {
            Ok(DataRecord {
                id,
                username,
                email,
                phone_number,
            })
        }
    }

    fn marshal(&self) -> [u8; RECORD_LENGTH_BYTES] {
        let mut buf: [u8; RECORD_LENGTH_BYTES] = [0; RECORD_LENGTH_BYTES];

        let id = self.id.to_le_bytes();
        buf[KEY_OFFSET..KEY_OFFSET + KEY_LENGTH_BYTES].copy_from_slice(&id);

        let username = self.username.as_bytes();
        buf[USERNAME_OFFSET..USERNAME_OFFSET + username.len()].copy_from_slice(username);

        let email = self.email.as_bytes();
        buf[EMAIL_OFFSET..EMAIL_OFFSET + email.len()].copy_from_slice(email);

        let phone_number = self.phone_number.as_bytes();
        buf[PHONE_NUMBER_OFFSET..PHONE_NUMBER_OFFSET + phone_number.len()]
            .copy_from_slice(phone_number);

        return buf;
    }

    fn unmarshal(bytes: &[u8]) -> Result<DataRecord, String> {
        if bytes.len() != RECORD_LENGTH_BYTES {
            return Err(format!(
                "Unable to parse form binary expected length to be: '{}' but got: '{}'",
                RECORD_LENGTH_BYTES,
                bytes.len()
            ));
        }

        let id = u32::from_le_bytes(
            bytes[KEY_OFFSET..KEY_OFFSET + KEY_LENGTH_BYTES]
                .try_into()
                .unwrap(),
        );

        // TODO add length prefix instead of using zero byte termination
        let end_index = &bytes[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_LENGTH_BYTES]
            .iter()
            .position(|r| *r == 0)
            .unwrap_or(USERNAME_LENGTH_BYTES);

        let username = std::str::from_utf8(&bytes[USERNAME_OFFSET..USERNAME_OFFSET + end_index])
            .expect("Unable to parse username, record is corrupted");

        let end_index = &bytes[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_LENGTH_BYTES]
            .iter()
            .position(|r| *r == 0)
            .unwrap_or(EMAIL_LENGTH_BYTES);

        let email = std::str::from_utf8(&bytes[EMAIL_OFFSET..EMAIL_OFFSET + end_index])
            .expect("Unable to parse email, record is corrupted");

        let phone_number = std::str::from_utf8(
            &bytes[PHONE_NUMBER_OFFSET..PHONE_NUMBER_OFFSET + PHONE_NUMBER_LENGTH_BYTES],
        )
            .expect("Unable to parse phone number, record is corrupted");

        Ok(DataRecord {
            id,
            username: username.to_string(),
            email: email.to_string(),
            phone_number: phone_number.to_string(),
        })
    }
}

impl PartialOrd<Self> for DataRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

struct MemTable {
    id: String,
    buffer: BTreeMap<u32, DataRecord>,
    capacity: usize,
    bloom_filter: BloomFilter,
}

impl MemTable {
    fn insert(&mut self, record: DataRecord) -> Result<(), &'static str> {
        if self.buffer.len() >= self.capacity {
            return Err("Mem Table is full, TODO schedule write to disk");
        }

        self.bloom_filter.add(&record.id.to_le_bytes());
        self.buffer.insert(record.id, record);
        Ok(())
    }

    // TODO this shouldn't be needed as mem tables should ideally be read only after created
    fn remove(&mut self, id: u32, recompute_bloom_filter: bool) -> Option<DataRecord> {
        let removed = self.buffer.remove(&id);

        // Items can't be removed from the bloom filter because it might also remove other items
        // therefore the bloom filter needs to be recomputed. This is a potentially expensive operation
        // so it should be done infrequently
        if recompute_bloom_filter {
            let mut bf = BloomFilter::new(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_CAPACITY);
            for key in self.buffer.keys() {
                bf.add(&key.to_le_bytes());
            }

            self.bloom_filter = bf;
        }

        removed
    }

    fn is_full(&self) -> bool {
        self.buffer.len() >= self.capacity
    }

    fn is_empty(&self) -> bool {
        self.buffer.len() == 0
    }

    fn get_min_key(&self) -> Option<u32> {
        self.buffer.keys().min().copied()
    }

    fn get_record(&self, key: u32) -> Option<&DataRecord> {
        if !self.bloom_filter.exists(&key.to_le_bytes()) {
            None
        } else {
            self.buffer.get(&key)
        }
    }

    fn size(&self) -> usize {
        self.buffer.len()
    }

    fn new(length: usize) -> MemTable {
        MemTable {
            id: Uuid::new_v4().to_string(),
            buffer: BTreeMap::new(),
            capacity: length,
            // TODO calculate optimal hash count and capacity for memtable,
            // consider disk resident eventual size with merges as well
            bloom_filter: BloomFilter::new(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_CAPACITY),
        }
    }

    /// from_slice assumes every memtable is written to disk when full.
    /// If a mem table is written partially it will fail
    fn unmarshal(bytes: &[u8]) -> Result<MemTable, io::Error> {
        // TODO: fix this
        // let num_records = bytes.len() - BLOOM_FILTER_LENGTH_BYTES / RECORD_LENGTH_BYTES;
        //
        // if num_records != MEM_TABLE_RECORD_COUNT {
        //     panic!(
        //         "Expected to find '{}' records in memtable but got: '{}'",
        //         MEM_TABLE_RECORD_COUNT, num_records
        //     );
        // }

        let mut table = MemTable::new(MEM_TABLE_RECORD_COUNT);

        // TODO better error handling
        let bf = BloomFilter::from_binary(
            &bytes[BLOOM_FILTER_OFFSET..BLOOM_FILTER_OFFSET + BLOOM_FILTER_LENGTH_BYTES],
            BLOOM_FILTER_HASH_COUNT,
            BLOOM_FILTER_CAPACITY,
        )
            .unwrap();

        table.bloom_filter = bf;

        for i in 0..MEM_TABLE_RECORD_COUNT {
            if let Ok(data_record) = DataRecord::unmarshal(
                &bytes[DATA_OFFSET + i * RECORD_LENGTH_BYTES
                    ..DATA_OFFSET + i * RECORD_LENGTH_BYTES + RECORD_LENGTH_BYTES],
            ) {
                // TODO better error handling
                table.insert(data_record).unwrap()
            } else {
                panic!("Corrupt data record found, index: '{}'", i)
            }
        }

        Ok(table)
    }

    fn marshal(&self) -> [u8; MEM_TABLE_SIZE_BYTES] {
        let mut serialized: [u8; MEM_TABLE_SIZE_BYTES] = [0; MEM_TABLE_SIZE_BYTES];

        // TODO better error handling here
        self.bloom_filter.copy_to(
            &mut serialized[BLOOM_FILTER_OFFSET..BLOOM_FILTER_OFFSET + BLOOM_FILTER_LENGTH_BYTES],
        );

        for (i, (_, value)) in self.buffer.iter().enumerate() {
            serialized[DATA_OFFSET + RECORD_LENGTH_BYTES * i
                ..DATA_OFFSET + RECORD_LENGTH_BYTES * i + RECORD_LENGTH_BYTES]
                .copy_from_slice(&value.marshal());
        }

        serialized
    }
}

impl fmt::Display for MemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Current memtable size {} records", self.buffer.len())?;

        for (k, v) in &self.buffer {
            writeln!(f, "key: '{}' - value: '{:?}'", k, v)?;
        }

        Ok(())
    }
}

impl PartialEq for MemTable {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for MemTable {}

pub struct MemTableManager {
    memtable_dir: PathBuf,
    current: MemTable,
}

impl MemTableManager {
    pub fn new(memtable_dir: PathBuf) -> Result<MemTableManager, io::Error> {
        fs::create_dir_all(&memtable_dir)?;

        Ok(MemTableManager {
            memtable_dir,
            current: MemTable::new(MEM_TABLE_RECORD_COUNT),
        })
    }

    pub fn write(&mut self, record: DataRecord) -> Result<(), io::Error> {
        if self.current.is_full() {
            // TODO: Maybe add this to flush queue and do asynchronously?
            debug!(
                "Memtable '{}' is full starting flush to disk",
                self.current.id
            );
            self.flush_memtable(&self.current)?;

            debug!("Provisioning new memtable");
            self.current = MemTable::new(MEM_TABLE_RECORD_COUNT);
        }

        // TODO handle this error
        self.current.insert(record).unwrap();
        Ok(())
    }

    // TODO: Make more efficient by using file stream w/ binary search,
    // open memtable cache, and multiple memtables in one file
    pub fn read(&self, record_id: u32) -> Result<Option<DataRecord>, io::Error> {
        if let Some(record) = self.current.get_record(record_id) {
            Ok(Some(record.to_owned()))
        } else {
            for f in fs::read_dir(&self.memtable_dir)? {
                // TODO better error handling
                let mut fd = OpenOptions::new().read(true).open(f?.path())?;

                let mut buf: [u8; MEM_TABLE_SIZE_BYTES] = [0; MEM_TABLE_SIZE_BYTES];
                fd.read_exact(&mut buf)?;

                // TODO better error handling
                let bf = BloomFilter::from_binary(
                    &mut buf[BLOOM_FILTER_OFFSET..BLOOM_FILTER_OFFSET + BLOOM_FILTER_LENGTH_BYTES],
                    BLOOM_FILTER_HASH_COUNT,
                    BLOOM_FILTER_CAPACITY,
                )
                    .unwrap();

                if !bf.exists(&record_id.to_le_bytes()) {
                    continue;
                }

                let mt = MemTable::unmarshal(&buf)?;

                if let Some(record) = mt.get_record(record_id) {
                    return Ok(Some(record.to_owned()));
                }
            }

            Ok(None)
        }
    }

    fn flush_memtable(&self, mt: &MemTable) -> io::Result<()> {
        debug!(
            "Flushing memtable with id: '{}', size '{}', capacity '{}'",
            self.current.id,
            self.current.size(),
            self.current.capacity
        );

        let disk_path = self.memtable_dir.join(&mt.id);

        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&disk_path)?;

        fd.write_all(&mt.marshal())?;

        debug!("Successfully flushed memtable");
        Ok(())
    }

    fn fetch_memtable(self, memtable_id: &str) -> Result<MemTable, io::Error> {
        // TODO: Cache frequently used memtables in memory, maybe with file descriptors?
        let mut f = OpenOptions::new()
            .read(true)
            .open(self.memtable_dir.join(memtable_id))?;

        let mut buf = Vec::with_capacity(MEM_TABLE_RECORD_COUNT);

        f.read_to_end(&mut buf)?;

        MemTable::unmarshal(&buf)
    }

    // TODO: figure out how to pick which files to merge
    // Could have different merge strategies using trait,
    // maybe simple implementation based on open recency?
    // Also figure out caching strategy for memtables and file handles
    // maybe different component? Also need to handle updates and deletes on merges
    // need to figure out how I'm going to do tombstone deletes
    pub fn merge_files(&mut self, fp1: &Path, fp2: &Path) -> Result<(), io::Error> {
        // TODO merge straight from binary instead of serializing to mem tables first
        let mut fd1 = OpenOptions::new().read(true).open(fp1)?;
        let mut fd2 = OpenOptions::new().read(true).open(fp2)?;

        let mut buf: [u8; MEM_TABLE_SIZE_BYTES] = [0; MEM_TABLE_SIZE_BYTES];

        fd1.read_exact(&mut buf)?;
        let mut mt1 = MemTable::unmarshal(&buf)?;

        if mt1.is_empty() {
            panic!(
                "Loaded empty memtable from disk: '{}'",
                fp1.to_str().unwrap_or("")
            );
        }

        fd2.read_exact(&mut buf)?;
        let mut mt2 = MemTable::unmarshal(&buf)?;
        if mt2.is_empty() {
            panic!(
                "Loaded empty memtable from disk: '{}'",
                fp2.to_str().unwrap_or("")
            );
        }

        // TODO this assumes id's will be unique but that won't always be
        // true, eventually this will have to be an id/insert time pair
        let mut heap: BinaryHeap<Reverse<u32>> = BinaryHeap::new();

        heap.push(Reverse(mt1.get_min_key().unwrap()));
        heap.push(Reverse(mt2.get_min_key().unwrap()));

        let mut merged = MemTable::new(MEM_TABLE_RECORD_COUNT * 2);

        while !heap.is_empty() {
            let Reverse(id) = heap.pop().unwrap();

            let mt = if let Some(_) = mt1.get_record(id) {
                &mut mt1
            } else {
                &mut mt2
            };

            let record = mt.remove(id, false).unwrap();
            merged.insert(record).unwrap();

            if !mt.is_empty() {
                heap.push(Reverse(mt.get_min_key().unwrap()));
            }
        }

        self.flush_memtable(&merged)?;

        // TODO: Some more thought will have to be put into this, what if there are active reads going on from this
        // file? What if file descriptors are cached in memory for these files?
        // Maybe some kind of locking scheme while a delete is occurring?
        fs::remove_file(fp1)?;
        fs::remove_file(fp2)?;

        Ok(())
    }

    // TODO: figure out how to pick which files to merge
    // Could have different merge strategies using trait,
    // maybe simple implementation based on open recency?
    // Also figure out caching strategy for memtables and file handles
    // maybe different component? Also need to handle updates and deletes on merges
    // need to figure out how I'm going to do tombstone deletes
    // Also make this operate entirely on binary data, there's no need for the overhead of
    // serializing/deserializing to a struct
    pub fn flerge_files(&mut self, fp1: &Path, fp2: &Path) -> Result<(), io::Error> {
        // TODO merge straight from binary instead of serializing to mem tables first
        let mut fd1 = OpenOptions::new().read(true).open(fp1)?;
        let mut fd2 = OpenOptions::new().read(true).open(fp2)?;

        // TODO: add data start position and record count fields to header
        // also add better error handling
        let mut c1 = Cursor::new(&mut fd1)?;
        let mut c2 = Cursor::new(&mut fd2)?;

        c1.advance()?;
        c2.advance()?;

        // TODO this assumes id's will be unique but that won't always be
        // true, eventually this will have to be an id/insert time pair
        let mut heap: BinaryHeap<Reverse<u32>> = BinaryHeap::new();

        heap.push(Reverse(c1.current().unwrap().id));
        heap.push(Reverse(c2.current().unwrap().id));

        let mut bf = BloomFilter::new(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_CAPACITY);
        // TODO: Eventually I can make this an exact length when I add the number of records in the header
        let mut merged: Vec<DataRecord> = Vec::with_capacity(MEM_TABLE_RECORD_COUNT * 2);

        while !heap.is_empty() {
            let Reverse(id) = heap.pop().unwrap();

            // TODO: don't like this
            let c = if id == c1.current().unwrap().id {
                &mut c1
            } else {
                &mut c2
            };

            let current = c.current().unwrap();

            bf.add(&current.id.to_le_bytes());
            merged.push(c.current().unwrap());

            c.advance()
                .or_else(| e : io::Error | if e.kind() == ErrorKind::UnexpectedEof { Ok(())} else { Err(e) })?;
        }

        //self.flush_memtable(&merged)?;
        //
        // // TODO: Some more thought will have to be put into this, what if there are active reads going on from this
        // // file? What if file descriptors are cached in memory for these files?
        // // Maybe some kind of locking scheme while a delete is occurring?
        // fs::remove_file(fp1)?;
        // fs::remove_file(fp2)?;

        Ok(())
    }
}

struct Cursor<'a> {
    // TODO: don't know if this should be a file ref or a path that the cursor opens yet
    fd: &'a File,
    buf: [u8; RECORD_LENGTH_BYTES],
    // TODO: don't like this find better way
    initialized: bool,
}

impl<'a> Cursor<'a> {
    fn new(fd: &mut File) -> Result<Cursor, io::Error> {
        // TODO don't like this
        fd.seek(SeekFrom::Start(BLOOM_FILTER_LENGTH_BYTES as u64))?;

        Ok(Cursor {
            fd,
            buf: [0; RECORD_LENGTH_BYTES],
            initialized: false,
        })
    }

    fn current(&self) -> Result<DataRecord, String> {
        if !self.initialized {
            Err("Call advance before reading the current value".to_string())
        } else {
            DataRecord::unmarshal(&self.buf)
        }
    }

    fn advance(&mut self) -> Result<(), io::Error> {
        if !self.initialized { self.initialized = true; }

        self.fd.read_exact(&mut self.buf)?;
        Ok(())
    }
}