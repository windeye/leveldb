// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;   //data block的选项
  Options index_block_options; //index block的选项
  WritableFile* file;  // sstable file的类
  uint64_t offset;  // 要写入位置在sstable中的偏移
  Status status;  //当前状态
  BlockBuilder data_block;  // 当前操作的data block
  BlockBuilder index_block; // sstable的index block
  std::string last_key;  // 当前data block最后的k-v对的key 
  int64_t num_entries;   // 目前data block的数目
  bool closed;          // Either Finish() or Abandon() has been called.
  // Filter block是存储的过滤器信息，它会存储{key, 对应data block在sstable的偏移值}，
  // 不一定是完全精确的，以快速定位给定key是否在data block中。
  FilterBlockBuilder* filter_block;  //根据filter数据快速定位key是否在block中  

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent blocks.
  // 直到处理下一个data block的第一个key的时候，才将index写入index block，
  // 这样可以在index中使用更短的key。在给的这个例子里，the quick brown fox"
  // 是一个block的最后一个key。"the who"是下一个block的第一个key，则用"the r"
  // 做前一个block的索引key就可满足index block的条件。

  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  // 要添加到index block的data block的信息,data block的偏移和size。
  BlockHandle pending_handle;  // Handle to add to index block

  // 压缩后的data block。临时存储，写毕即清空。
  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}
// 其他API都比较简单，这个是most important的
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  // 验证文件木有关闭，状态OK！
  assert(!r->closed);
  if (!ok()) return;
  // 如果已经写入过k-v，则确保添加的key大于以前的任意key。
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // pending_index_entry为true表明遇到下一个data block的第一个k-v，根据
  // key调整last_key，这个是有FindShortestSeparator完成的。
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // 对应data block的index entry信息就保存在（BlockHandle）pending_handle。
    r->pending_handle.EncodeTo(&handle_encoding);
    // 这个是索引的key值吗？好像是这个样子的，FindShortestSeparator通过
    // key(下一个data block的第一个key)来生成上一个data block的索引key值。
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != NULL) {
    // 把key加入到filter_block中
    r->filter_block->AddKey(key);
  }

  // 设置last_key，更新entries数，然后添加key到block中.
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  // 尺寸达到配置中的size就flush到文件中。
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  // 保证文件未关闭，状态OK。
  assert(!r->closed);
  if (!ok()) return;
  // 要flush的data block不为空
  if (r->data_block.empty()) return;
  // pending_index_entry要为false，即data block的Add已经完成。
  assert(!r->pending_index_entry);
  // 写data block，并设置其index entry信息=>r->pending_handle
  WriteBlock(&r->data_block, &r->pending_handle);
  // 写入成功，则Flush数据到文件。设置r->pending_index_entry为true，
  // 以根据下一个data block的第一个key来确定index entry的key。
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  // 开始一个新的data block，并将上一个data block的偏移加到filter_block中。
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
}

// 在Flush时，会调用该函数，同时还设置data block的index entry信息。
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 获得data block的序列化字符串
  Slice raw = block->Finish();

  Slice block_contents;
  // 配置中的压缩方式
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
	// 压缩比如果小于12.5%，则存储不压缩的原始数据。
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 正真执行写文件操作的函数
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  // 为index设置data block的信息
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  // 写入data block内容
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize]; // 5 bytes
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    // 写入成功后，更新下一个data block的起始位置。
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

// 所有k-v持久化完成，sstable写入结束，关闭sst文件。
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 先调用Flush，将最后一个data block写入文件，closed设置为true。
  Flush();
  assert(!r->closed);
  r->closed = true;

  // 下面的写的流程很清晰，和sst中的数据布局一致，很赞。
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block,在布局中是meta data block，filter还没看，
  // 之后看了原理就好啦！
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
