// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 每2kb生成一个filter，计算这个block要生成几个filter,应该是这个样子的：
  // 当要生成一个新的data block时，block_offset表示新block的起始位置，
  // block_offset / kFilterBase就是目前所有data block要生成的filter的数目，
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    // 会循环多次吗？keys_在一次Generate的调用中就处理完了呢！data block
    // 是按照2kb增长的？不是！实际上除了第一次会生成该block的的filter，循环
    // 后面几次调用都值生成空的filter。
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 这个函数的调用标志着table的data block已经全部生成，要向sstable的meta block
// 中写如这些filter数据。
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // 存储每个filter的偏移
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }
  // filter的数量
  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  // 根据key在keys_的起始地址，依次提取所有key生成key列表
  // 为啥不直接用vector保存每个key？这就不用转一次了！
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 为当前的key集合生成filter，吧结果append到result_后面。
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], num_keys, &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  // 按照数据的布局从后向前解析。
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  // base_lg_只用了1byte。
  base_lg_ = contents[n-1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // block_offset要搜索的data block在sstable中的偏移。
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 获取filter的起始和结束地址
    uint32_t start = DecodeFixed32(offset_ + index*4); 
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= (offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
