// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

// 匿名namespace
namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
// 在memtable和sstable里，都以(userkey,seq,type) => uservalue的形式
// 存储每一条数据，DBIter会合并DB里同一userkey的多个值为一个值，基于
// sequence number，删除标记，覆盖写等因素。
// DBIter内部使用了MergingIterator，在调用MergingItertor的系列seek函
// 数后，DBIter还要处理key的删除标记。否则，遍历时会把已删除的key列举出来。
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  //  When moving forward,保证internal iterator准确定位在this->key()
  //  , this->value()这个entry上。
  //  When moving backwards, internal iterator定位在所有(internal iterator
  //  内包含多个iterator)user key == this->key()的entry之前。
  enum Direction {
    kForward,
    kReverse
  };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_counter_(RandomPeriod()) {
  }
  virtual ~DBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    // direction是kForward直接取iter_->key()，否则取saved key
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  // 很重要的几个辅助函数
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  ssize_t RandomPeriod() {
    return rnd_.Uniform(2*config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;

  Status status_;
  std::string saved_key_;     // == current key when direction_==kReverse
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  Random rnd_;
  ssize_t bytes_counter_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n;
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();
    db_->RecordReadSample(k);
  }
  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    // 当direction为Reverse时，iter的位置是在this->key()的前面的，由Prev
    // 的操作能看出来，this->key返回的是saved_key，所以先把iter_的位置提前
    // 到this->key的范围，在执行正常的skip操作。
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    // saved_key_保存当前的key，这样在FindNextUserEntry里就会跳过它。
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
  }
  // skip操作
  FindNextUserEntry(true, &saved_key_);
}

void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    // 要求iter_->key()的sequence <= 遍历指定的sequence  
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          // 已经被标示为deleted的key，使用skip临时保存被跳过的key。
          // 之后对于这个key的entry全部跳过(skip all upcoming entries)。
          SaveKey(ikey.user_key, skip); // 一般skip传的是saved_key_
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
              // 这是一个被删除的entry(==0时)，或者user key比指定的key小，跳过  
          } else {
            // 找到，清空saved key并返回，iter_已定位到正确的entry
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  // if hit here，表明没有找到有效key。
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    // iter_ 定位在当前的位置，向后扫描直到key改变了。
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      // key改变了，现在iter位于saved_key之前的entry上。
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion; //后面的循环至少执行一次Prev操作  
  // 进入FindPrevUserEntry时iter_刚好位于saved key对应的所有记录之前。
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      // parse iter_->key()返回的key
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        // 第一次循环肯定不会到这个分支，因为value_type = kTypeDeletion
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
            // We encountered a non-deleted value in entries for previous keys,
            // 找到一个non-deleted记录，跳出循环。
            // 此时Key()将返回saved_key,saved_key非空。
          break;
        }
        value_type = ikey.type;
        // 如果是Deletion则清空saved key和saved value  
        // 
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          // 否则，把iter_的user key和value赋给saved key和saved value  
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    // 木有找到要找的数据
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    // 已经全部遍历了，调头。
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    // 可以定位到合法的iter，还需要跳过Delete的entry  
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    // 可以定位到合法的iter，还需要跳过Delete的entry  
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  // 跳过Delete的entry  
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence,
    uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
