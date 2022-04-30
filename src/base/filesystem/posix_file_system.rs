//
//
// posix_file_system.rs
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

// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fs::{read_dir, rename};
use std::io::Write;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{RTStoreError, Result};
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{close, ftruncate, lseek, Whence};
use nix::NixPath;

use super::reader::SequentialFileReader;
use super::{
    FileSystem, RandomAccessFile, RandomAccessFileReader, SequentialFile, WritableFile,
    WritableFileWriter,
};

const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;
const MIN_ALLOCATE_SIZE: usize = 4 * 1024;

/// A `LogFd` is a RAII file that provides basic I/O functionality.
///
/// This implementation is a thin wrapper around `RawFd`, and primarily targets
/// UNIX-based systems.
pub struct RawFile(RawFd);

pub fn from_nix_error(e: nix::Error, custom: &'static str) -> std::io::Error {
    let kind = std::io::Error::from(e).kind();
    std::io::Error::new(kind, custom)
}

impl RawFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let flags = OFlag::O_RDWR;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        Ok(RawFile(
            fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?,
        ))
    }

    #[allow(unused_variables)]
    pub fn open_for_read<P: ?Sized + NixPath>(path: &P, direct: bool) -> Result<Self> {
        #[allow(unused_mut)]
        let mut flags = OFlag::O_RDONLY;
        #[cfg(target_os = "linux")]
        if direct {
            flags |= OFlag::O_DIRECT;
        }
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        Ok(RawFile(
            fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?,
        ))
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        let fd = fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?;
        Ok(RawFile(fd))
    }

    pub fn close(&self) -> Result<()> {
        close(self.0).map_err(|e| from_nix_error(e, "close"))?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            nix::unistd::fdatasync(self.0).map_err(|e| from_nix_error(e, "fdatasync"))?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            nix::unistd::fsync(self.0).map_err(|e| from_nix_error(e, "fsync"))?;
            Ok(())
        }
    }

    pub fn read(&self, mut offset: usize, buf: &mut [u8]) -> Result<usize> {
        let mut readed = 0;
        while readed < buf.len() {
            let bytes = match pread(self.0, &mut buf[readed..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EAGAIN => continue,
                Err(e) => return Err(RTStoreError::from(from_nix_error(e, "pread"))),
            };
            // EOF
            if bytes == 0 {
                break;
            }
            readed += bytes;
            offset += bytes;
        }
        Ok(readed)
    }

    pub fn write(&self, mut offset: usize, content: &[u8]) -> Result<usize> {
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EAGAIN => continue,
                Err(e) => return Err(RTStoreError::from(from_nix_error(e, "pwrite"))),
            };
            if bytes == 0 {
                break;
            }
            written += bytes;
            offset += bytes;
        }
        Ok(written)
    }

    pub fn file_size(&self) -> Result<usize> {
        let size = lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| from_nix_error(e, "lseek"))?;
        Ok(size)
    }

    pub fn truncate(&self, offset: usize) -> Result<()> {
        ftruncate(self.0, offset as i64).map_err(|e| from_nix_error(e, "ftruncate"))?;
        Ok(())
    }

    #[allow(unused_variables)]
    pub fn allocate(&self, offset: usize, size: usize) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            fcntl::fallocate(
                self.0,
                fcntl::FallocateFlags::empty(),
                offset as i64,
                size as i64,
            )
            .map_err(|e| from_nix_error(e, "fallocate"))?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(())
        }
    }
}

impl Drop for RawFile {
    fn drop(&mut self) {
        if let Err(_e) = self.close() {
            // error!("error while closing file: {}", e);
        }
    }
}

/// A `WritableFile` is a `RawFile` wrapper that implements `Write`.
pub struct PosixWritableFile {
    inner: Arc<RawFile>,
    offset: usize,
    capacity: usize,
}

impl PosixWritableFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let fd = RawFile::open(path)?;
        let file_size = fd.file_size()?;
        Ok(Self::new(Arc::new(fd), file_size))
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let fd = RawFile::create(path)?;
        let file_size = fd.file_size()?;
        Ok(Self::new(Arc::new(fd), file_size))
    }

    pub fn new(fd: Arc<RawFile>, capacity: usize) -> Self {
        Self {
            inner: fd,
            offset: 0,
            capacity,
        }
    }
}

impl WritableFile for PosixWritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.write_all(data)?;
        Ok(())
    }

    fn truncate(&mut self, offset: u64) -> Result<()> {
        self.inner.truncate(offset as usize)?;
        Ok(())
    }

    fn allocate(&mut self, offset: u64, len: u64) -> Result<()> {
        let new_written = offset + len;
        if new_written > self.capacity as u64 {
            let mut real_alloc = MIN_ALLOCATE_SIZE;
            let alloc = new_written as usize - self.capacity;
            while real_alloc < alloc {
                real_alloc *= 2;
            }
            self.inner.allocate(self.capacity, real_alloc)?;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.inner.sync()?;
        Ok(())
    }

    fn fsync(&mut self) -> Result<()> {
        self.inner.sync()?;
        Ok(())
    }
}

impl Write for PosixWritableFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let new_written = self.offset + buf.len();
        if new_written > self.capacity {
            let alloc = std::cmp::max(new_written - self.capacity, FILE_ALLOCATE_SIZE);
            let mut real_alloc = FILE_ALLOCATE_SIZE;
            while real_alloc < alloc {
                real_alloc *= 2;
            }
            self.inner.allocate(self.capacity, real_alloc)?;
            self.capacity += real_alloc;
        }
        let len = self.inner.write(self.offset, buf)?;
        self.offset += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct PosixReadableFile {
    inner: Arc<RawFile>,
}

impl PosixReadableFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let fd = RawFile::open_for_read(path, false)?;
        Ok(Self {
            inner: Arc::new(fd),
        })
    }
}

impl RandomAccessFile for PosixReadableFile {
    fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        let size = self.inner.read(offset, data)?;
        Ok(size)
    }

    fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize> {
        let size = self.inner.read(offset, &mut data[..n])?;
        Ok(size)
    }

    fn file_size(&self) -> usize {
        self.inner.file_size().unwrap()
    }
}

pub struct PosixSequentialFile {
    inner: Arc<RawFile>,
    file_size: usize,
    offset: usize,
}

impl PosixSequentialFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let fd = RawFile::open_for_read(path, false)?;
        let file_size = fd.file_size()?;
        Ok(Self {
            inner: Arc::new(fd),
            file_size,
            offset: 0,
        })
    }
}

impl SequentialFile for PosixSequentialFile {
    fn read_sequential(&mut self, data: &mut [u8]) -> Result<usize> {
        if self.offset >= self.file_size {
            return Ok(0);
        }
        let rest = std::cmp::min(data.len(), self.file_size - self.offset);
        let x = self.inner.read(self.offset, &mut data[..rest])?;
        self.offset += x;
        Ok(x)
    }

    fn get_file_size(&self) -> usize {
        self.file_size
    }
}

pub struct SyncPosixFileSystem {}

impl FileSystem for SyncPosixFileSystem {
    fn open_writable_file_writer(&self, path: &Path) -> Result<Box<WritableFileWriter>> {
        let f = PosixWritableFile::create(path)?;
        let writer = WritableFileWriter::new(Box::new(f), 0);
        Ok(Box::new(writer))
    }

    fn open_random_access_file(&self, p: &Path) -> Result<Box<RandomAccessFileReader>> {
        let f = PosixReadableFile::open(p)?;
        let filename = p
            .file_name()
            .ok_or_else(|| RTStoreError::FSInvalidFileError {
                path: "path has no file name".to_string(),
            })?
            .to_str()
            .ok_or_else(|| RTStoreError::FSInvalidFileError {
                path: "filename is not encode by utf8".to_string(),
            })?;
        let reader = RandomAccessFileReader::new(Box::new(f), filename.to_string());
        Ok(Box::new(reader))
    }

    fn open_sequential_file(&self, path: &Path) -> Result<Box<SequentialFileReader>> {
        let f = PosixSequentialFile::open(path)?;
        let reader = SequentialFileReader::new(
            Box::new(f),
            path.file_name().unwrap().to_str().unwrap().to_string(),
        );
        Ok(Box::new(reader))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn list_files(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut files = vec![];
        for f in read_dir(path)? {
            files.push(f?.path());
        }
        Ok(files)
    }

    fn rename(&self, origin: &Path, target: &Path) -> Result<()> {
        rename(origin, target)?;
        Ok(())
    }

    fn file_exist(&self, path: &Path) -> Result<bool> {
        Ok(path.exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_write_file() {
        let dir = tempfile::Builder::new()
            .prefix("test_write_file")
            .tempdir()
            .unwrap();
        let fs = SyncPosixFileSystem {};
        let mut f = fs
            .open_writable_file_writer(&dir.path().join("sst"))
            .unwrap();
        f.append("abcd".as_bytes()).unwrap();
        f.append("efgh".as_bytes()).unwrap();
        f.append("ijkl".as_bytes()).unwrap();
        f.sync().unwrap();

        let mut f = fs.open_sequential_file(&dir.path().join("sst")).unwrap();
        let mut v = vec![0; 7];
        let x = f.read(&mut v).unwrap();
        assert_eq!(x, 7);
        let s = String::from_utf8(v.clone()).unwrap();
        assert_eq!(s.as_str(), "abcdefg");

        let _x = f.read(&mut v).unwrap();
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(_x, 5);
        }
        let s = String::from_utf8((&v[..5]).to_vec()).unwrap();
        assert_eq!(s.as_str(), "hijkl");
    }
}
