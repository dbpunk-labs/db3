//
//
// slice.rs
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

#[derive(Default, Clone)]
pub struct Slice {
    pub offset: usize,
    pub limit: usize,
}

impl Slice {
    pub fn len(&self) -> usize {
        if self.offset > self.limit {
            0
        } else {
            self.limit - self.offset
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
