use core::cmp::Reverse;
use std::thread::{self, JoinHandle};
use std::{fmt, fs, io, time};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, BTreeMap, HashSet};
use std::fs::{File, OpenOptions, DirEntry};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{Utc, NaiveDateTime, DateTime};
use log::{debug, error, info};
use uuid::Uuid;

use crate::bloomfilter::BloomFilter;

type MergePredicate = fn(&DirEntry) -> bool;

const HEADER_OFFSET: usize = 0;
const HEADER_SIZE_BYTES: usize = 20;

const BLOOM_FILTER_OFFSET: usize = 0;
const BLOOM_FILTER_LENGTH_BYTES: usize = 16;

const FILE_RECORD_COUNT_OFFSET: usize = 16;
const FILE_RECORD_COUNT_LENGTH_BYTES: usize = 4;

const DATA_OFFSET: usize = 20;
const DATA_LENGTH_BYTES: usize = MEM_TABLE_RECORD_COUNT * RECORD_LENGTH_BYTES;

const KEY_OFFSET: usize = 0;
const KEY_LENGTH_BYTES: usize = 4;

const TIMESTAMP_OFFSET: usize = 4;
const TIMESTAMP_LENGTH_BYTES: usize = 8;

const USERNAME_OFFSET: usize = 12;
const USERNAME_LENGTH_BYTES: usize = 255;

const EMAIL_OFFSET: usize = 267;
const EMAIL_LENGTH_BYTES: usize = 255;

const PHONE_NUMBER_OFFSET: usize = 522;
const PHONE_NUMBER_LENGTH_BYTES: usize = 10;

const DELETED_FLAG_OFFSET: usize = 532;
const DELETED_FLAG_LENGTH_BYTES: usize = 1;

const RECORD_LENGTH_BYTES: usize = 533;

// 16 kb / 524 bytes is approximately 31 records per memtable
const MEM_TABLE_RECORD_COUNT: usize = 30;
const MEM_TABLE_SIZE_BYTES: usize = 16 * 1024;

const BLOOM_FILTER_CAPACITY: usize = 128;
const BLOOM_FILTER_HASH_COUNT: u32 = 6;

const MAX_DISK_RESIDENT_FILESIZE_BYTES: usize = 1 * 1024 * 1024 * 1024;
const MERGE_QUEUE_SIZE: usize = 10;

// TODO: there's a lot of things in this module use Vecs which instead could be using fixed length
// arrays because we know the sizes of the structures
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRecord {
    id: u32,
    username: String,
    email: String,
    phone_number: String,
    timestamp: chrono::DateTime<Utc>,
    deleted: bool,
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
                timestamp: chrono::offset::Utc::now(),
                deleted: false,
            })
        }
    }

    fn marshal(&self) -> [u8; RECORD_LENGTH_BYTES] {
        let mut buf: [u8; RECORD_LENGTH_BYTES] = [0; RECORD_LENGTH_BYTES];

        let id = self.id.to_le_bytes();
        buf[KEY_OFFSET..KEY_OFFSET + KEY_LENGTH_BYTES].copy_from_slice(&id);

        let timestamp = self.timestamp.timestamp().to_le_bytes();
        buf[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + TIMESTAMP_LENGTH_BYTES].copy_from_slice(&timestamp);

        let username = self.username.as_bytes();
        buf[USERNAME_OFFSET..USERNAME_OFFSET + username.len()].copy_from_slice(username);

        let email = self.email.as_bytes();
        buf[EMAIL_OFFSET..EMAIL_OFFSET + email.len()].copy_from_slice(email);

        let phone_number = self.phone_number.as_bytes();
        buf[PHONE_NUMBER_OFFSET..PHONE_NUMBER_OFFSET + phone_number.len()]
            .copy_from_slice(phone_number);

        // TODO: figure out if this is okay or if there is a better way to do this.
        buf[DELETED_FLAG_OFFSET] = 
            if self.deleted { 0x1 } else { 0x0 };

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
                .unwrap()
        );

        let timestamp = i64::from_le_bytes(
            bytes[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + TIMESTAMP_LENGTH_BYTES]
                .try_into()
                .unwrap()
        );

        let converted_ts: DateTime<Utc> = 
            chrono::DateTime::from_utc(
                NaiveDateTime::from_timestamp(timestamp, 0), Utc);

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

        // TODO: figure out if this is thebest way to do this
        // or if something more robust is needed
        let deleted = bytes[DELETED_FLAG_OFFSET] != 0;  
        
        Ok(DataRecord {
            id,
            timestamp: converted_ts,
            username: username.to_string(),
            email: email.to_string(),
            phone_number: phone_number.to_string(),
            deleted,
        })
    }

    fn get_key(buf: &[u8]) -> u32 {
        if buf.len() != RECORD_LENGTH_BYTES {
            panic!("Argument 'buf' does not match data record length");
        }

        u32::from_le_bytes(buf[KEY_OFFSET..KEY_OFFSET + KEY_LENGTH_BYTES].try_into().unwrap())
    }
}

impl PartialOrd<Self> for DataRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id).then(self.timestamp.cmp(&other.timestamp))
    }
}

struct MemTable {
    id: String,
    buffer: BTreeMap<u32, DataRecord>,
    capacity: usize,
    bloom_filter: BloomFilter,
}

impl MemTable {
    fn insert(&mut self, record: DataRecord) {
        if self.buffer.len() >= self.capacity {
            panic!("Mem Table is at capacity. capacity: '{}' flush to disk", self.capacity);
        }

        self.bloom_filter.add(&record.id.to_le_bytes());
        self.buffer.insert(record.id, record);
    }

    // All deletes are tombstone deletes, the records will be cleaned up when a merge occurs
    // not sure I like how this code is written
    fn delete(&mut self, mut record: DataRecord) {
        record.deleted = true;
        record.timestamp = chrono::offset::Utc::now();

        self.insert(record);
    }

    fn is_full(&self) -> bool {
        self.buffer.len() >= self.capacity
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

    fn marshal(&self) -> [u8; MEM_TABLE_SIZE_BYTES] {
        let mut serialized: [u8; MEM_TABLE_SIZE_BYTES] = [0; MEM_TABLE_SIZE_BYTES];

        // TODO better error handling here, also add header offset
        self.bloom_filter.copy_to(
            &mut serialized[BLOOM_FILTER_OFFSET..BLOOM_FILTER_OFFSET + BLOOM_FILTER_LENGTH_BYTES],
        );

        serialized[FILE_RECORD_COUNT_OFFSET..FILE_RECORD_COUNT_OFFSET + FILE_RECORD_COUNT_LENGTH_BYTES]
            // TODO: figure out if this u32 conversion will ever be a problem
            .copy_from_slice(&(MEM_TABLE_RECORD_COUNT as u32).to_le_bytes());

        for (i, (_, value)) in self.buffer.iter().enumerate() {
            serialized[DATA_OFFSET + RECORD_LENGTH_BYTES * i
                ..DATA_OFFSET + RECORD_LENGTH_BYTES * i + RECORD_LENGTH_BYTES]
                .copy_from_slice(&value.marshal());
        }

        serialized
    }

    fn binary_search(buf: &[u8], key: u32) -> Option<usize> {
        let mut low = 0;
        let mut high = MemTable::get_record_count(buf);


        while low <= high {
            let mid = low + (high -low) / 2;
            let mr = buf[mid * DATA_OFFSET..mid * DATA_OFFSET + RECORD_LENGTH_BYTES]; 
            
            let mk = DataRecord::get_key(mr);

            if mk == key {
                return Some(mid * DATA_OFFSET);
            }

            if key < mk {
                high = mid;
            } else {
                low = mid 
            } 
        }

        None
    }

    fn get_record_count(buf: &[u8]) -> u32{
        // TODO: don't like that there's no error checking on this
        u32::from_le_bytes(
            buf[FILE_RECORD_COUNT_OFFSET..FILE_RECORD_COUNT_OFFSET+FILE_RECORD_COUNT_LENGTH_BYTES]
            .try_into()
            .unwrap())
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
    merge_thread: JoinHandle<()>,
}

impl MemTableManager {
    pub fn new(memtable_dir: PathBuf, 
               merge_predicate: MergePredicate,
               merge_interval: time::Duration) -> 
            Result<MemTableManager, io::Error> {
        fs::create_dir_all(&memtable_dir)?;

        /*
            TODO: since this variable is read only maybe there's a 
            better way to do this rather then cloning here.
            Maybe an ARC? The ref should live as long as the class
        */
        let path_clone = memtable_dir.clone();

        let merge_thread = thread::spawn(move | | {
            loop {
               let mut merge_set = HashSet::new();

                for f in fs::read_dir(&path_clone).unwrap() {
                    let dr = f.unwrap();
                    if merge_predicate(&dr) {
                        merge_set.insert(dr.path());
                    }

                    if merge_set.len() >= MERGE_QUEUE_SIZE {
                        break;
                    }
                }
                
                if merge_set.len() >= MERGE_QUEUE_SIZE {
                    MemTableManager::merge_files(&path_clone, &merge_set).unwrap();
                    merge_set.clear();
                }
                
                // TODO find better way to do this without blocking 
                // the thread, maybe async wait with event loop
                thread::sleep(merge_interval);
            }
        });
        
        Ok(MemTableManager {
            memtable_dir,
            current: MemTable::new(MEM_TABLE_RECORD_COUNT),
            merge_thread, 
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
        self.current.insert(record);
        Ok(())
    }

    // All deletes are tombstone deletes and deleted records are cleaned up
    // during the file merging process. functionally, this means that 
    // whenever a delete comes in for a record, a copy of the record is inserted
    // with the delete flag set. We don't update the original record because
    // this would break the immutability of the on disk files
    pub fn delete(&mut self, key: u32) -> Result<(), io::Error> {
        match self.read(key) {
            Ok(record) => {
                Ok(self.current.delete(record))
            },
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err)
        }
    }

    // TODO: Make more efficient by using file stream w/ binary search,
    // open memtable cache, and multiple memtables in one file.
    // need to add k way merge between all files that contain a version of the key
    // don't like how I have to keep re-reading file descriptors here. 
    // Maybe I need to make a file manager abastraction which manages access/caches 
    // file descriptors. 
    pub fn read(&self, key: u32) -> Result<DataRecord, io::Error> {
        if let Some(record) = self.current.get_record(key) {
            Ok(record.to_owned())
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

                if !bf.exists(&key.to_le_bytes()) {
                    continue;
                }

                // TODO add binary search
                fd.seek(SeekFrom::Start(0))?;
                let mut c = Cursor::new(fd)?;
                if let Some(record) =  c.find(| record | record.id == key) {
                    return Ok(record.to_owned());
                }
            }

            Err(io::Error::new(ErrorKind::NotFound, format!("No records found for key {}", key)))
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

    // TODO: figure out how to pick which files to merge
    // Could have different merge strategies using trait,
    // maybe simple implementation based on open recency?
    // Also figure out caching strategy for memtables and file handles
    // maybe different component? Also need to handle updates and deletes on merges
    // need to figure out how I'm going to do tombstone deletes
    // Also make this operate entirely on binary data, there's no need for the overhead of
    // serializing/deserializing to a struct.
    // Also need to figure out how to handle cleaning up tombstones
    // as they will proliferate otherwise, maybe a different process? 
    fn merge_files(memtable_dir: &Path, files: &HashSet<PathBuf>) -> Result<(), io::Error> {
        // TODO this assumes id's will be unique but that won't always be
        // true, eventually this will have to be an id/insert time pair
        let mut heap = BinaryHeap::new();
        let mut cursors = Vec::with_capacity(MERGE_QUEUE_SIZE);
        let mut total_record_count : usize = 0;

        for path in files {
            // TODO merge straight from binary instead of serializing to mem tables first
            let fd = OpenOptions::new().read(true).open(path)?;
            let mut cursor = Cursor::new(fd)?;
            total_record_count += cursor.record_count as usize;
 
            if total_record_count as usize * RECORD_LENGTH_BYTES + HEADER_SIZE_BYTES 
                > MAX_DISK_RESIDENT_FILESIZE_BYTES {
                    panic!("TODO: implement file splitting on save");
                }

            cursor.advance()?;
            heap.push(Reverse(cursor.current().unwrap().id));

            cursors.push(cursor);
        }

        let mut bf = BloomFilter::new(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_CAPACITY);
        // TODO: This should probably start off as binary instead of converting to and from records
        let mut merged = BTreeMap::new();

        while !heap.is_empty() {
            let Reverse(id) = heap.pop().unwrap();

            // TODO: don't like this
            let cursor = {
                cursors.iter_mut().find(| c | c.current().unwrap().id == id)
            }.unwrap();

            let current = cursor.current().unwrap();

            if !merged.contains_key(&current.id) { 
                bf.add(&current.id.to_le_bytes());
                merged.insert(current.id, current);
            } else if merged.get(&current.id).unwrap().timestamp > current.timestamp {
                bf.add(&current.id.to_le_bytes());
                merged.insert(current.id, current);
            }

            let advanced = cursor.advance();

            match advanced {
                Ok(_) => heap.push(Reverse(cursor.current().unwrap().id)),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => (),
                _ => advanced?
            } 
        }
        
        let mut new_file = vec![0; HEADER_SIZE_BYTES + total_record_count as usize * RECORD_LENGTH_BYTES];
        bf.copy_to(&mut new_file[0..BLOOM_FILTER_LENGTH_BYTES]);

        // TODO: Figure out if this cast will become a problem and I should change it to 64 bytes
        let record_count = merged.len() as u32;
        assert!(record_count < (MAX_DISK_RESIDENT_FILESIZE_BYTES / RECORD_LENGTH_BYTES) as u32, 
            "record count outside of range of allowable values {record_count}");
        
        new_file[FILE_RECORD_COUNT_OFFSET..FILE_RECORD_COUNT_OFFSET + FILE_RECORD_COUNT_LENGTH_BYTES]
            .copy_from_slice(&record_count.to_le_bytes());

        for (i, value) in merged.values().enumerate() {
            if new_file.len() > MAX_DISK_RESIDENT_FILESIZE_BYTES {
                return Err(io::Error::new(ErrorKind::Other, "Max disk resident file size exceeded"));
            }

            new_file[DATA_OFFSET + RECORD_LENGTH_BYTES * i
                ..DATA_OFFSET + RECORD_LENGTH_BYTES * i + RECORD_LENGTH_BYTES]
                .copy_from_slice(&value.marshal());
        }

        // Account for deleted records
        new_file.truncate(HEADER_SIZE_BYTES + total_record_count);

        let disk_path = memtable_dir.join(Uuid::new_v4().to_string());
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&disk_path)?;

        fd.write_all(&new_file)?;

        // TODO: Some more thought will have to be put into this, what if there are active reads going on from this
        // file? What if file descriptors are cached in memory for these files?
        // Maybe some kind of locking scheme while a delete is occurring?
        for path in files {
            fs::remove_file(path)?;
        }

        Ok(())
    }
}

struct Cursor {
    // TODO: don't know if this should be a file ref or a path that the cursor opens yet
    fd: File,
    buf: [u8; RECORD_LENGTH_BYTES],
    // TODO: don't like this find better way
    initialized: bool,
    record_count: u32,
}

impl Cursor {
    fn new(mut fd: File) -> Result<Cursor, io::Error> {
        // TODO don't like this

        let mut buf = [0; RECORD_LENGTH_BYTES];

        fd.seek(SeekFrom::Start(FILE_RECORD_COUNT_OFFSET as u64))?;
        fd.read_exact(&mut buf[0..FILE_RECORD_COUNT_LENGTH_BYTES])?;

        let rc = u32::from_le_bytes(buf[0..FILE_RECORD_COUNT_LENGTH_BYTES]
            .try_into()
            .unwrap());

        fd.seek(SeekFrom::Start(HEADER_SIZE_BYTES as u64))?;

        Ok(Cursor {
            fd,
            buf,
            initialized: false,
            record_count: rc
        })
    }

    fn current(&self) -> Result<DataRecord, String> {
        if !self.initialized {
            Err("Call advance before reading the current value".to_string())
        } else {
            // TODO cache this and return a reference ot it,
            // no need to unmarshal it multiple times
            DataRecord::unmarshal(&self.buf)
        }
    }

    fn advance(&mut self) -> Result<(), io::Error> {
        if !self.initialized { self.initialized = true; }

        self.fd.read_exact(&mut self.buf)?;
        Ok(())
    }
}

impl Iterator for Cursor {
    type Item = DataRecord;

// TODO better error handling
    fn next(&mut self) -> Option<Self::Item> {
       if let Ok(_) = self.advance() {
        Some(self.current().unwrap())
       } else {
        None
       }
    }
}

