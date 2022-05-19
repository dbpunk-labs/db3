//
//
// mod.rs
// Copyright (C) 2022 rtstore.io Author imotai <codego.me@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

mod reader;
mod writer;

pub const HEADER_SIZE: usize = 4 + 2 + 1;
pub const RECYCLABLE_HEADER_SIZE: usize = 4 + 2 + 1 + 4;

#[cfg(test)]
pub const BLOCK_SIZE: usize = 4096;
#[cfg(not(test))]
pub const BLOCK_SIZE: usize = 32768;
pub const LOG_PADDING: &[u8] = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum RecordType {
    // Zero is reserved for preallocated files
    ZeroType = 0,
    FullType = 1,

    // For fragments
    FirstType = 2,
    MiddleType = 3,
    LastType = 4,
    // For recycled log files
    RecyclableFullType = 5,
    // RecyclableFirstType = 6,
    // RecyclableMiddleType = 7,
    RecyclableLastType = 8,
    Unknown = 127,
}

impl From<u8> for RecordType {
    fn from(x: u8) -> Self {
        if x > 8 {
            RecordType::Unknown
        } else {
            unsafe { std::mem::transmute(x) }
        }
    }
}

const MAX_RECORD_TYPE: u8 = RecordType::RecyclableLastType as u8;
const MASK_DELTA: u32 = 0xa282ead8u32;

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum RecordError {
    Eof = 9,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    BadRecord = 10,
    // Returned when we fail to read a valid header.
    BadHeader = 11,
    // Returned when we read an old record from a previous user of the log.
    OldRecord = 12,
    // Returned when we get a bad record length
    BadRecordLen = 13,
    // Returned when we get a bad record checksum
    BadRecordChecksum = 14,
    Unknown = 127,
}

impl From<u8> for RecordError {
    fn from(x: u8) -> Self {
        if !(9..=14).contains(&x) {
            RecordError::Unknown
        } else {
            unsafe { std::mem::transmute(x) }
        }
    }
}

pub use reader::LogReader;
pub use writer::LogWriter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::filesystem::{FileSystem, SyncPosixFileSystem};
    use std::path::Path;
    use tempdir::TempDir;

    #[test]
    fn simple_write_read_test() {
        let tmp_dir_path = TempDir::new("log_test").expect("create temp dir");
        if let Some(tmp_dir_path_str) = tmp_dir_path.path().to_str() {
            let log_path_str = format!("{}/xxxx.log", tmp_dir_path_str);
            let log_path = Path::new(&log_path_str);
            let fs = SyncPosixFileSystem {};
            if let Ok(writer) = fs.open_writable_file_writer(log_path) {
                let mut log_writer = LogWriter::new(writer, 1);
                let empty_data = "";
                assert!(log_writer.add_record(empty_data.as_bytes()).is_ok());
                let data = "hello world";
                assert!(log_writer.add_record(data.as_bytes()).is_ok());
                assert!(log_writer.fsync().is_ok());
            } else {
                panic!("should not be here");
            }
            if let Ok(reader) = fs.open_sequential_file(log_path) {
                let mut log_reader = LogReader::new(reader);
                let mut buffer: Vec<u8> = Vec::new();
                // read a record
                if let Ok(status) = log_reader.read_record(&mut buffer) {
                    assert!(status);
                    let data = String::from_utf8(buffer).unwrap();
                    assert_eq!(&data, "hello world");
                } else {
                    panic!("should not be here");
                }
            }
        } else {
            panic!("should not be here");
        }
    }
}
