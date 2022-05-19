//
//
// linked_list.rs
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

use crate::error::{RTStoreError, Result};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// the node of linkedlist
pub struct Node<V> {
    pub v: V,
    pub next: AtomicPtr<Node<V>>,
}

impl<V> Node<V> {
    fn new(v: V) -> Node<V> {
        Self {
            v,
            // default is null
            next: AtomicPtr::default(),
        }
    }
}

/// linkedlist with single link
pub struct LinkedList<V> {
    head: AtomicPtr<Node<V>>,
    size: AtomicU64,
}

impl<V> Default for LinkedList<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> LinkedList<V> {
    pub fn new() -> Self {
        let head = AtomicPtr::default();
        Self {
            head,
            size: AtomicU64::new(0),
        }
    }

    pub fn push_front(&self, v: V) -> Result<()> {
        let node_ptr = Box::into_raw(Box::new(Node::new(v)));
        let mut orig_head_ptr = self.head.load(Ordering::Acquire);
        for _i in 0..8 {
            unsafe {
                (*node_ptr).next = AtomicPtr::new(orig_head_ptr);
            }
            match self.head.compare_exchange_weak(
                orig_head_ptr,
                node_ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                // If compare_exchange succeeds, then the `head` ptr was properly updated, i.e.,
                // no other thread was interleaved and snuck in to change `head` since we last loaded it.
                Ok(_old_head_ptr) => {
                    self.size.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(changed_head_ptr) => orig_head_ptr = changed_head_ptr,
            }
        }
        Err(RTStoreError::BaseBusyError(
            "fail to change atomic reference".to_string(),
        ))
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.size.load(Ordering::Relaxed) == 0
    }

    pub fn iter(&self) -> LinkedListIter<V> {
        LinkedListIter { curr: &self.head }
    }
}

pub struct LinkedListIter<'a, T: 'a> {
    curr: &'a AtomicPtr<Node<T>>,
}

impl<'a, T: 'a> Iterator for LinkedListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        let curr_ptr = self.curr.load(Ordering::Acquire);
        if curr_ptr == std::ptr::null_mut() {
            return None;
        }
        // SAFE: curr_ptr was checked for null
        let curr_node: &Node<T> = unsafe { &*curr_ptr };
        self.curr = &curr_node.next;
        Some(&curr_node.v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        let ll: LinkedList<i32> = LinkedList::new();
        assert_eq!(0, ll.size());
    }

    #[test]
    fn test_push_front() {
        let ll: LinkedList<i32> = LinkedList::new();
        if ll.push_front(1).is_err() {
            panic!("should be ok");
        }
        assert_eq!(1, ll.size());
    }

    #[test]
    fn test_it() {
        let ll: LinkedList<i32> = LinkedList::new();
        if ll.push_front(1).is_err() {
            panic!("should be ok");
        }
        assert_eq!(1, ll.size());
        let it = ll.iter();
        for v in it {
            assert_eq!(1, *v);
        }
    }
}
