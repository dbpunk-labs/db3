//
//
// mod.rs
// Copyright (C) 2022 peasdb.ai Author imotai <codego.me@gmail.com>
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

mod posix_file_system;
mod reader;
mod writer;

use crate::error::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::RTStoreError;
use async_trait::async_trait;
pub use posix_file_system::SyncPosixFileSystem;
pub use reader::RandomAccessFileReader;
pub use reader::SequentialFileReader;
pub use writer::WritableFileWriter;

#[async_trait]
pub trait RandomAccessFile: 'static + Send + Sync {
    async fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        self.read_exact(offset, data.len(), data).await
    }
    async fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize>;
    fn file_size(&self) -> usize;
    fn use_direct_io(&self) -> bool {
        false
    }
}

#[async_trait]
pub trait SequentialFile: 'static + Send + Sync {
    async fn read_sequential(&mut self, data: &mut [u8]) -> Result<usize>;
    fn get_file_size(&self) -> usize;
}

#[async_trait]
pub trait WritableFile: Send {
    async fn append(&mut self, data: &[u8]) -> Result<()>;
    async fn truncate(&mut self, offset: u64) -> Result<()>;
    fn allocate(&mut self, offset: u64, len: u64) -> Result<()>;
    async fn sync(&mut self) -> Result<()>;
    async fn fsync(&mut self) -> Result<()>;
    fn use_direct_io(&mut self) -> bool {
        false
    }
    fn get_file_size(&self) -> usize {
        0
    }
}

#[derive(Default)]
pub struct IOOption {
    pub direct: bool,
    pub high_priority: bool,
    pub buffer_size: usize,
}

#[async_trait]
pub trait FileSystem: Send + Sync {
    fn open_writable_file_in(
        &self,
        path: &Path,
        file_name: String,
    ) -> Result<Box<WritableFileWriter>> {
        let f = path.join(file_name);
        self.open_writable_file_writer(&f)
    }

    fn open_writable_file_writer(&self, file_name: &Path) -> Result<Box<WritableFileWriter>>;
    fn open_writable_file_writer_opt(
        &self,
        file_name: &Path,
        _opts: &IOOption,
    ) -> Result<Box<WritableFileWriter>> {
        self.open_writable_file_writer(file_name)
    }

    fn open_random_access_file(&self, p: &Path) -> Result<Box<RandomAccessFileReader>>;

    fn open_sequential_file(&self, path: &Path) -> Result<Box<SequentialFileReader>>;

    async fn read_file_content(&self, path: &Path) -> Result<Vec<u8>> {
        let mut reader = self.open_sequential_file(path)?;
        let sz = reader.file_size();
        let mut data = vec![0u8; sz];
        const BUFFER_SIZE: usize = 8192;
        let mut offset = 0;
        while offset < data.len() {
            let block_size = std::cmp::min(data.len() - offset, BUFFER_SIZE);
            let read_size = reader
                .read(&mut data[offset..(offset + block_size)])
                .await?;
            offset += read_size;
            if read_size < block_size {
                data.resize(offset, 0);
                break;
            }
        }
        Ok(data)
    }

    fn remove(&self, path: &Path) -> Result<()>;
    fn rename(&self, origin: &Path, target: &Path) -> Result<()>;

    fn list_files(&self, path: &Path) -> Result<Vec<PathBuf>>;

    fn file_exist(&self, path: &Path) -> Result<bool>;
}

#[derive(Default)]
pub struct InMemFileSystemRep {
    files: HashMap<String, Vec<u8>>,
}

#[derive(Default, Clone)]
pub struct InMemFileSystem {
    inner: Arc<Mutex<InMemFileSystemRep>>,
}

#[derive(Default, Clone)]
pub struct InMemFile {
    pub buf: Vec<u8>,
    fs: Arc<Mutex<InMemFileSystemRep>>,
    filename: String,
    offset: usize,
}

#[async_trait]
impl WritableFile for InMemFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        self.buf.resize(offset as usize, 0);
        Ok(())
    }

    fn allocate(&mut self, _offset: u64, _len: u64) -> Result<()> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        self.fsync().await
    }

    async fn fsync(&mut self) -> Result<()> {
        let mut fs = self.fs.lock().unwrap();
        fs.files.insert(self.filename.clone(), self.buf.clone());
        Ok(())
    }
}

#[async_trait]
impl RandomAccessFile for InMemFile {
    async fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        if offset >= self.buf.len() {
            Ok(0)
        } else if offset + data.len() > self.buf.len() {
            let rest = self.buf.len() - offset;
            data[..rest].copy_from_slice(&self.buf[offset..(offset + rest)]);
            Ok(rest)
        } else {
            data.copy_from_slice(&self.buf[offset..(offset + data.len())]);
            Ok(data.len())
        }
    }

    async fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize> {
        if offset >= self.buf.len() {
            Ok(0)
        } else if offset + n > self.buf.len() {
            let rest = self.buf.len() - offset;
            data[..rest].copy_from_slice(&self.buf[offset..(offset + rest)]);
            Ok(rest)
        } else {
            data[..n].copy_from_slice(&self.buf[offset..(offset + n)]);
            Ok(n)
        }
    }
    fn file_size(&self) -> usize {
        self.buf.len()
    }
}

#[async_trait]
impl SequentialFile for InMemFile {
    async fn read_sequential(&mut self, data: &mut [u8]) -> Result<usize> {
        let x = self.read(self.offset, data).await?;
        self.offset += x;
        Ok(x)
    }

    fn get_file_size(&self) -> usize {
        self.buf.len()
    }
}

impl FileSystem for InMemFileSystem {
    fn open_writable_file_writer(&self, filename: &Path) -> Result<Box<WritableFileWriter>> {
        let f = InMemFile {
            fs: self.inner.clone(),
            buf: vec![],
            filename: filename.to_str().unwrap().to_string(),
            offset: 0,
        };
        Ok(Box::new(WritableFileWriter::new(
            Box::new(f),
            filename.to_str().unwrap().to_string(),
            128,
        )))
    }

    fn open_random_access_file(&self, filename: &Path) -> Result<Box<RandomAccessFileReader>> {
        let filename = filename.to_str().unwrap().to_string();
        let fs = self.inner.lock().unwrap();
        match fs.files.get(&filename) {
            None => {
                return Err(RTStoreError::FSInvalidFileError {
                    path: filename.clone(),
                })
            }
            Some(buf) => {
                let f = InMemFile {
                    fs: self.inner.clone(),
                    buf: buf.clone(),
                    filename: filename.clone(),
                    offset: 0,
                };
                Ok(Box::new(RandomAccessFileReader::new(Box::new(f), filename)))
            }
        }
    }

    fn open_sequential_file(&self, path: &Path) -> Result<Box<SequentialFileReader>> {
        let fs = self.inner.lock().unwrap();
        let filename = path.to_str().unwrap();
        match fs.files.get(filename) {
            None => {
                return Err(RTStoreError::FSInvalidFileError {
                    path: filename.to_string(),
                })
            }
            Some(buf) => {
                let f = InMemFile {
                    fs: self.inner.clone(),
                    buf: buf.clone(),
                    filename: filename.to_string(),
                    offset: 0,
                };
                Ok(Box::new(SequentialFileReader::new(
                    Box::new(f),
                    filename.to_string(),
                )))
            }
        }
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let filename = path.to_str().unwrap();
        let mut fs = self.inner.lock().unwrap();
        fs.files
            .remove(filename)
            .ok_or_else(|| RTStoreError::FSInvalidFileError {
                path: filename.to_string(),
            })?;
        Ok(())
    }

    fn rename(&self, origin: &Path, target: &Path) -> Result<()> {
        let filename = origin.to_str().unwrap();
        let mut fs = self.inner.lock().unwrap();
        let f = fs
            .files
            .remove(filename)
            .ok_or_else(|| RTStoreError::FSInvalidFileError {
                path: filename.to_string(),
            })?;
        let filename = target.to_str().unwrap();
        fs.files.insert(filename.to_string(), f);
        Ok(())
    }

    fn list_files(&self, _: &Path) -> Result<Vec<PathBuf>> {
        let fs = self.inner.lock().unwrap();
        let files = fs
            .files
            .iter()
            .map(|(k, _)| PathBuf::from(k.clone()))
            .collect();
        Ok(files)
    }

    fn file_exist(&self, path: &Path) -> Result<bool> {
        let fs = self.inner.lock().unwrap();
        let filename = path.to_str().unwrap();
        Ok(fs.files.get(filename).is_some())
    }
}
