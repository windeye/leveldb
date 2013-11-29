// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  // 重设内容，通常在Finish之后调用来构建新的block,BlockBuilder构造完后调用。
  void Reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // 添加新的k-v，要求key大于前面的任意key，Finish()还没有调用。
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  // 结束DataBlock的构建并返回引用block内容的slice，返回的slice的生命周期
  // 将延续到Reset()被调用。
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  // 返回正在构建的block的尺寸的估值（未压缩的）。
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  // 如果自Reset之后没有add任何k-v对，则为空。
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;
  std::string           buffer_;      // Destination buffer,buffer只是用一个string而已，简约啊
  std::vector<uint32_t> restarts_;    // Restart points
  int                   counter_;     // Number of entries emitted since restart,restart之后增加的k-v对数量
  bool                  finished_;    // Has Finish() been called?
  std::string           last_key_;

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
