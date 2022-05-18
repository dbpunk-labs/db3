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
pub use posix_file_system::SyncPosixFileSystem;
pub use reader::SequentialFileReader;
use std::path::{Path, PathBuf};
pub use writer::WritableFileWriter;

pub trait SequentialFile: 'static + Send + Sync {
    fn read_sequential(&mut self, data: &mut [u8]) -> Result<usize>;
    fn get_file_size(&self) -> usize;
}

pub trait WritableFile: Send {
    fn append(&mut self, data: &[u8]) -> Result<()>;
    fn truncate(&mut self, offset: u64) -> Result<()>;
    fn allocate(&mut self, offset: u64, len: u64) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn fsync(&mut self) -> Result<()>;
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

    fn open_sequential_file(&self, path: &Path) -> Result<Box<SequentialFileReader>>;

    fn read_file_content(&self, path: &Path) -> Result<Vec<u8>> {
        let mut reader = self.open_sequential_file(path)?;
        let sz = reader.file_size();
        let mut data = vec![0u8; sz];
        const BUFFER_SIZE: usize = 8192;
        let mut offset = 0;
        while offset < data.len() {
            let block_size = std::cmp::min(data.len() - offset, BUFFER_SIZE);
            let read_size = reader.read(&mut data[offset..(offset + block_size)])?;
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
