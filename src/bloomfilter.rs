use std::ops::BitOr;

use bitvec::prelude::*;
use fastmurmur3;

#[derive(Clone, Debug)]
pub struct BloomFilter {
    hash_count: u32,
    buf: BitVec<u8>,
    capacity: usize,
}

impl BloomFilter {
    pub fn new(hash_count: u32, capacity: usize) -> BloomFilter {
        return BloomFilter {
            hash_count,
            buf: bitvec!(u8, Lsb0; 0; capacity),
            capacity,
        };
    }

    pub fn add(&mut self, bytes: &[u8]) -> &mut Self {
        // TODO maybe make this a trait to allow for different hash implementations
        for i in 0..self.hash_count {
            let hash = fastmurmur3::murmur3_x64_128(bytes, i);
            let idx = (hash % self.capacity as u128) as usize;
            self.buf.set(idx, true);
        }

        self
    }

    pub fn exists(&self, bytes: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let hash = fastmurmur3::murmur3_x64_128(bytes, i);
            let idx = (hash % self.capacity as u128) as usize;
            if !self.buf[idx] {
                return false;
            }
        }

        true
    }

    pub fn byte_length(&self) -> usize {
        // TODO not ideal if storage size changes because the calculation will be wrong
        (self.capacity as f32 / 8.0).ceil() as usize
    }

    // TODO: maybe add the hash count and the size to the serialization?
    pub fn copy_to(&self, buf: &mut [u8]) {
        if buf.len() != self.byte_length() {
            panic!("size of buffer must equal length returned by byte_length() method")
        } else {
            let raw = self.buf.as_raw_slice();
            buf.copy_from_slice(raw);
        }
    }

    pub fn from_binary(buf: &[u8], hash_count: u32, capacity: usize) -> Result<BloomFilter, String> {
        let expected_length = (capacity as f32 / 8.0).ceil() as usize;
        if expected_length != buf.len() {
            Err(format!("Byte buffer has invalid length: expected length to be {}", expected_length))
        } else {
            Ok(BloomFilter {
                capacity,
                hash_count,
                buf: BitVec::from_slice(buf),
            })
        }
    }

    // TODO custom error types
    // TODO think about if there's a way of making this mutable instead
    // of consuming the source andif it even makes sense
    pub fn merge_from(self, other: Self) -> Result<BloomFilter, &'static str> {
        if self.capacity != other.capacity { return Err("Capacities don't match"); }

        if self.hash_count != other.hash_count { return Err("Hash counts don't match"); }

        if self.buf.len() != other.buf.len() { return Err("Buffer sizes do not match") }

        Ok(BloomFilter {
            capacity: self.capacity,
            hash_count: self.hash_count,
            buf: self.buf.bitor(other.buf)
        }) 
    }
}