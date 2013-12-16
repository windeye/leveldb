// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include "leveldb/iterator.h"

namespace leveldb {

struct ReadOptions;

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
// 专门为sstable定制的iterator，two-level iterator是sstable一系列block的
// index iterator，block是一系列的k-v对。就是专为data block使用的吧！
// 返回的two-level iterator会串联一系列blocks的所有k-v对。index_iter
// 不再使用时要删除。
//
// block_function的作用；在遍历不同的data block时去解析block里的k-v对，
// 它返回一个针对data block的iterator。
extern Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(
        void* arg,
        const ReadOptions& options,
        const Slice& index_value),
    void* arg,
    const ReadOptions& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
