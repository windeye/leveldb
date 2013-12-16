// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.
// filter block保存在table的末尾，它包含对table全部数据的过滤。
#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
// FilterBlockBuilder用来构造一个sstable的filters，它生成一个string存储
// 在sstable里的一个特定block里。
// FilterBlockBuilder构建过程中，API的调用顺序必须满足下面的模式：
// (StartBlock AddKey*)* Finish，*标示重复。
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  // 开始构造新的fliter block，TableBuilder在构造函数和Flush中调用。
  void StartBlock(uint64_t block_offset);
  // 添加key，TableBuilder每次向data block中加入key时调用。  
  void AddKey(const Slice& key);
  // TableBuilder结束时调用。
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  // Flattened key contents,所有key连接的结果
  std::string keys_;             
  // Starting index in keys_ of each key,各个key在keys_中的索引值。
  std::vector<size_t> start_;   
  // Filter data computed so far,目前为止生成的filter数据
  std::string result_;         
  // 传给policy_->CreateFilter()的参数
  std::vector<Slice> tmp_keys_;  
  // 每个filter在result_中的位置
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

// FilterBlock的读取类
class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  // fliter block的数据的指针
  const char* data_;    // Pointer to filter data (at block-start)
  // offset数组的起始地址
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
