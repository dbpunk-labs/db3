//
//
// reader.rs
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

use super::{RecordError, RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::base::filesystem::SequentialFileReader;
use crate::base::slice::Slice;
use crate::error::{RTStoreError, Result};

pub struct LogReader {
    reader: Box<SequentialFileReader>,
    buffer: Vec<u8>,
    data: Slice,
    end_of_buffer_offset: usize,
    eof: bool,
}

impl LogReader {
    pub fn new(reader: Box<SequentialFileReader>) -> Self {
        Self {
            reader,
            buffer: vec![],
            data: Slice::default(),
            end_of_buffer_offset: 0,
            eof: false,
        }
    }

    pub fn read_record(&mut self, record: &mut Vec<u8>) -> Result<bool> {
        let mut in_fragmented_record = false;
        record.clear();
        loop {
            // let physical_record_offset = self.end_of_buffer_offset - self.data.len();
            let (fragment, record_type) = self.read_physical_record()?;
            if record_type < RecordType::RecyclableLastType as u8 {
                let fragment_type = record_type.into();
                match fragment_type {
                    RecordType::ZeroType => {}
                    RecordType::FullType => {
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                        // prospective_record_offset = physical_record_offset;
                        // self.last_record_offset = prospective_record_offset;
                        return Ok(true);
                    }
                    RecordType::FirstType => {
                        // prospective_record_offset = physical_record_offset;
                        in_fragmented_record = true;
                        record.clear();
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                    }
                    RecordType::MiddleType => {
                        if !in_fragmented_record {
                            return Err(RTStoreError::FSLogReaderError(format!(
                                "missing start of fragmented record({})",
                                fragment.len()
                            )));
                        }
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                    }
                    RecordType::LastType => {
                        if !in_fragmented_record {
                            return Err(RTStoreError::FSLogReaderError(format!(
                                "missing start of fragmented record({})",
                                fragment.len()
                            )));
                        }
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                        return Ok(true);
                    }
                    _ => {
                        return Err(RTStoreError::FSLogReaderError(
                            "not support open recycle log".to_string(),
                        ));
                    }
                }
            } else {
                let err_type = record_type.into();
                match err_type {
                    RecordError::Eof => {
                        if in_fragmented_record {
                            record.clear();
                        }
                        return Ok(false);
                    }
                    RecordError::BadRecord
                    | RecordError::BadRecordLen
                    | RecordError::BadRecordChecksum
                    | RecordError::OldRecord => {
                        if in_fragmented_record {
                            record.clear();
                            in_fragmented_record = false;
                        }
                    }
                    _ => {
                        return Ok(false);
                    }
                }
            }
        }
    }

    fn read_physical_record(&mut self) -> Result<(Slice, u8)> {
        loop {
            let mut fragment = Slice::default();
            if self.data.len() < HEADER_SIZE {
                self.try_read_more()?;
                continue;
            }
            let header = &self.buffer[self.data.offset..];
            let a = (header[4] as u32) & 0xff;
            let b = (header[5] as u32) & 0xff;
            let tp = header[6];
            if tp >= RecordType::RecyclableFullType as u8 {
                return Err(RTStoreError::FSLogReaderError(
                    "not support open recycle log".to_string(),
                ));
            }
            let l = (a | (b << 8)) as usize;
            if l + HEADER_SIZE > self.data.len() {
                self.data.limit = 0;
                self.data.offset = 0;
                self.buffer.clear();
                if !self.eof {
                    return Err(RTStoreError::FSLogReaderError("header error".to_string()));
                } else {
                    return Ok((fragment, RecordError::Eof as u8));
                }
            }
            if tp == RecordType::ZeroType as u8 && l == 0 {
                self.buffer.clear();
                self.data.limit = 0;
                self.data.offset = 0;
                return Ok((fragment, RecordError::BadRecord as u8));
            }
            // TODO: checksum
            fragment.offset = self.data.offset + HEADER_SIZE;
            fragment.limit = fragment.offset + l;
            self.data.offset += HEADER_SIZE + l;
            return Ok((fragment, tp));
        }
    }

    fn try_read_more(&mut self) -> Result<()> {
        if self.eof {
            self.data.limit = 0;
            self.data.offset = 0;
            return Err(RTStoreError::FSIoEofError);
        }

        if self.buffer.len() < BLOCK_SIZE {
            self.buffer.resize(BLOCK_SIZE, 0);
        }

        match self.reader.read(&mut self.buffer[..BLOCK_SIZE]) {
            Ok(r) => {
                self.end_of_buffer_offset += r;
                self.data.offset = 0;
                self.data.limit = r;
                if r < BLOCK_SIZE {
                    self.eof = true;
                }
                Ok(())
            }
            Err(_) => Err(RTStoreError::FSIoEofError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{LogReader, LogWriter, BLOCK_SIZE};
    use crate::common::{FileSystem, SyncPosixFileSystem};
    use rand::{thread_rng, Rng};
    use tokio::runtime::Runtime;

    fn inner_test_log_read_and_write(record_size: usize) {
        let dir = tempfile::Builder::new()
            .prefix("test_log_read_and_write")
            .tempdir()
            .unwrap();
        println!("block_size: {}, record_size: {}", BLOCK_SIZE, record_size);
        let fs = SyncPosixFileSystem {};
        let writer = fs
            .open_writable_file_writer(&dir.path().join("sst"))
            .unwrap();
        let mut writer = LogWriter::new(writer, 0);
        let mut rng = thread_rng();
        let mut data: [u8; 100000] = [0u8; 100000];
        rng.fill(&mut data[..]);
        let r = Runtime::new().unwrap();
        r.block_on(async move {
            let mut left = 100000;
            let mut offset = 0;
            while left > 0 {
                let cur = std::cmp::min(left, record_size);
                writer
                    .add_record(&data[offset..(offset + cur)])
                    .await
                    .unwrap();
                writer.fsync().await.unwrap();
                offset += cur;
                left -= cur;
            }
        });
        let reader = fs.open_sequential_file(&dir.path().join("sst")).unwrap();
        let mut reader = LogReader::new(reader);
        r.block_on(async move {
            let mut record = vec![];
            let mut left = 100000;
            let mut offset = 0;
            while reader.read_record(&mut record).await.unwrap() {
                let cur = std::cmp::min(left, record_size);
                assert_eq!(record.as_slice(), &data[offset..(offset + cur)]);
                offset += cur;
                left -= cur;
            }
        });
    }

    #[test]
    fn test_log_read_and_write() {
        inner_test_log_read_and_write(100);
        inner_test_log_read_and_write(BLOCK_SIZE);
        inner_test_log_read_and_write(10000);
    }
}
