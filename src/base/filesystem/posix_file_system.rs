//
//
// posix_file_system.rs
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

// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fs::{read_dir, rename};
use std::io::{Result as IoResult, Write};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
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
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let flags = OFlag::O_RDWR;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        Ok(RawFile(
            fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?,
        ))
    }

    #[allow(unused_variables)]
    pub fn open_for_read<P: ?Sized + NixPath>(path: &P, direct: bool) -> IoResult<Self> {
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

    pub fn create<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        // fail_point!("log_fd::create::err", |_| {
        //     Err(from_nix_error(nix::Error::EINVAL, "fp"))
        // });
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        let fd = fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?;
        Ok(RawFile(fd))
    }

    pub fn close(&self) -> IoResult<()> {
        // fail_point!("log_fd::close::err", |_| {
        //     Err(from_nix_error(nix::Error::EINVAL, "fp"))
        // });
        close(self.0).map_err(|e| from_nix_error(e, "close"))
    }

    pub fn sync(&self) -> IoResult<()> {
        // fail_point!("log_fd::sync::err", |_| {
        //     Err(from_nix_error(nix::Error::EINVAL, "fp"))
        // });
        #[cfg(target_os = "linux")]
        {
            nix::unistd::fdatasync(self.0).map_err(|e| from_nix_error(e, "fdatasync"))
        }
        #[cfg(not(target_os = "linux"))]
        {
            nix::unistd::fsync(self.0).map_err(|e| from_nix_error(e, "fsync"))
        }
    }

    pub fn read(&self, mut offset: usize, buf: &mut [u8]) -> IoResult<usize> {
        let mut readed = 0;
        while readed < buf.len() {
            // fail_point!("log_fd::read::err", |_| {
            //     Err(from_nix_error(nix::Error::EINVAL, "fp"))
            // });
            let bytes = match pread(self.0, &mut buf[readed..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EAGAIN => continue,
                Err(e) => return Err(from_nix_error(e, "pread")),
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

    pub fn write(&self, mut offset: usize, content: &[u8]) -> IoResult<usize> {
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EAGAIN => continue,
                Err(e) => return Err(from_nix_error(e, "pwrite")),
            };
            if bytes == 0 {
                break;
            }
            written += bytes;
            offset += bytes;
        }
        Ok(written)
    }

    pub fn file_size(&self) -> IoResult<usize> {
        lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| from_nix_error(e, "lseek"))
    }

    pub fn truncate(&self, offset: usize) -> IoResult<()> {
        ftruncate(self.0, offset as i64).map_err(|e| from_nix_error(e, "ftruncate"))
    }

    #[allow(unused_variables)]
    pub fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        #[cfg(target_os = "linux")]
        {
            fcntl::fallocate(
                self.0,
                fcntl::FallocateFlags::empty(),
                offset as i64,
                size as i64,
            )
            .map_err(|e| from_nix_error(e, "fallocate"))
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
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let fd = RawFile::open(path)?;
        let file_size = fd.file_size()?;
        Ok(Self::new(Arc::new(fd), file_size))
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
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

#[async_trait]
impl WritableFile for PosixWritableFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        self.write_all(data).map_err(|e| Error::Io(Box::new(e)))
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        self.inner
            .truncate(offset as usize)
            .map_err(|e| Error::Io(Box::new(e)))
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

    async fn sync(&mut self) -> Result<()> {
        self.inner.sync().map_err(|e| Error::Io(Box::new(e)))
    }

    async fn fsync(&mut self) -> Result<()> {
        self.inner.sync().map_err(|e| Error::Io(Box::new(e)))
    }
}

impl Write for PosixWritableFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
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

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

pub struct PosixReadableFile {
    inner: Arc<RawFile>,
    file_size: usize,
}

impl PosixReadableFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let fd = RawFile::open_for_read(path, false)?;
        let file_size = fd.file_size()?;
        Ok(Self {
            inner: Arc::new(fd),
            file_size,
        })
    }
}

#[async_trait]
impl RandomAccessFile for PosixReadableFile {
    async fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        self.inner
            .read(offset, data)
            .map_err(|e| Error::Io(Box::new(e)))
    }

    async fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize> {
        self.inner
            .read(offset, &mut data[..n])
            .map_err(|e| Error::Io(Box::new(e)))
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
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let fd = RawFile::open_for_read(path, false)?;
        let file_size = fd.file_size()?;
        Ok(Self {
            inner: Arc::new(fd),
            file_size,
            offset: 0,
        })
    }
}

#[async_trait]
impl SequentialFile for PosixSequentialFile {
    async fn read_sequential(&mut self, data: &mut [u8]) -> Result<usize> {
        if self.offset >= self.file_size {
            return Ok(0);
        }
        let rest = std::cmp::min(data.len(), self.file_size - self.offset);
        let x = self
            .inner
            .read(self.offset, &mut data[..rest])
            .map_err(|e| Error::Io(Box::new(e)))?;
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
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
        let f = PosixWritableFile::create(path).map_err(|e| Error::Io(Box::new(e)))?;
        let writer = WritableFileWriter::new(Box::new(f), file_name, 0);
        Ok(Box::new(writer))
    }

    fn open_random_access_file(&self, p: &Path) -> Result<Box<RandomAccessFileReader>> {
        let f = PosixReadableFile::open(p).map_err(|e| Error::Io(Box::new(e)))?;
        let filename = p
            .file_name()
            .ok_or_else(|| Error::InvalidFile("path has no file name".to_string()))?
            .to_str()
            .ok_or_else(|| Error::InvalidFile("filename is not encode by utf8".to_string()))?;
        let reader = RandomAccessFileReader::new(Box::new(f), filename.to_string());
        Ok(Box::new(reader))
    }

    fn open_sequential_file(&self, path: &Path) -> Result<Box<SequentialFileReader>> {
        let f = PosixSequentialFile::open(path).map_err(|e| Error::Io(Box::new(e)))?;
        let reader = SequentialFileReader::new(
            Box::new(f),
            path.file_name().unwrap().to_str().unwrap().to_string(),
        );
        Ok(Box::new(reader))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| Error::Io(Box::new(e)))
    }

    fn list_files(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut files = vec![];
        for f in read_dir(path).map_err(|e| Error::Io(Box::new(e)))? {
            files.push(f?.path());
        }
        Ok(files)
    }

    fn rename(&self, origin: &Path, target: &Path) -> Result<()> {
        rename(origin, target).map_err(|e| Error::Io(Box::new(e)))
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
        let r = Runtime::new().unwrap();
        r.block_on(async move {
            f.append("abcd".as_bytes()).await.unwrap();
            f.append("efgh".as_bytes()).await.unwrap();
            f.append("ijkl".as_bytes()).await.unwrap();
            f.sync().await.unwrap();
        });

        let mut f = fs.open_sequential_file(&dir.path().join("sst")).unwrap();
        r.block_on(async move {
            let mut v = vec![0; 7];
            let x = f.read(&mut v).await.unwrap();
            assert_eq!(x, 7);
            let s = String::from_utf8(v.clone()).unwrap();
            assert_eq!(s.as_str(), "abcdefg");

            let _x = f.read(&mut v).await.unwrap();
            #[cfg(not(target_os = "linux"))]
            {
                assert_eq!(_x, 5);
            }
            let s = String::from_utf8((&v[..5]).to_vec()).unwrap();
            assert_eq!(s.as_str(), "hijkl");
        });
    }
}
