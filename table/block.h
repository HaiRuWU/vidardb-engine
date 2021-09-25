//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#ifdef VIDARDB_MALLOC_USABLE_SIZE
#include <malloc.h>
#endif

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "vidardb/iterator.h"
#include "vidardb/options.h"
#include "table/internal_iterator.h"

#include "format.h"

namespace vidardb {

struct BlockContents;
class Comparator;
class BlockIter;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(BlockContents&& contents);

  ~Block() = default;

  size_t size() const { return size_; }

  const char* data() const { return data_; }

  bool cachable() const { return contents_.cachable; }

  size_t usable_size() const {
#ifdef VIDARDB_MALLOC_USABLE_SIZE
    if (contents_.allocation.get() != nullptr) {
      return malloc_usable_size(contents_.allocation.get());
    }
#endif  // VIDARDB_MALLOC_USABLE_SIZE
    return size_;
  }

  uint32_t NumRestarts() const;

  CompressionType compression_type() const {
    return contents_.compression_type;
  }

  enum BlockType : unsigned char {
    kTypeBlock = 0x0,
    kTypeMainColumn = 0x1,
    kTypeSubColumn =0x2,
    kTypeMinMax = 0x3
  };

  // If iter is null, return new Iterator
  // If iter is not null, update this one and return it as Iterator*
  InternalIterator* NewIterator(const Comparator* comparator,
                                BlockIter* iter = nullptr,
                                BlockType type = kTypeBlock);

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const;

 protected:
  BlockContents contents_;
  const char* data_;            // contents_.data.data()
  size_t size_;                 // contents_.data.size()
  uint32_t restart_offset_;     // Offset in data_ of restart array

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);
};

class BlockIter : public InternalIterator {
 public:
  BlockIter();

  BlockIter(const Comparator* comparator, const char* data, uint32_t restarts,
            uint32_t num_restarts);

  virtual void Initialize(const Comparator* comparator, const char* data,
                          uint32_t restarts, uint32_t num_restarts);

  void SetStatus(Status s) { status_ = s; }

  virtual bool Valid() const override final { return current_ < restarts_; }

  virtual Status status() const override final { return status_; }

  virtual Slice key() const override final {
    assert(Valid());
    return key_.GetKey();
  }

  virtual Slice value() override final {
    assert(Valid());
    return value_;
  }

  virtual void Next() override {
    assert(Valid());
    ParseNextKey();
  }

  virtual void Prev() override;

  virtual void Seek(const Slice& target) override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

#ifndef NDEBUG
  virtual ~BlockIter();

  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool IsKeyPinned() const override { return key_.IsKeyPinned(); }

 protected:
  const Comparator* comparator_;
  const char* data_;       // underlying block contents
  uint32_t restarts_;      // Offset of restart array (list of fixed32)
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  IterKey key_;
  Slice value_;
  Status status_;

  int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  virtual uint32_t NextEntryOffset() const {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  virtual void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

  virtual void CorruptionError();

  virtual bool ParseNextKey();

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index);
};

// Sub-column block iterator, used in sub columns' data block
class SubColumnBlockIter final : public BlockIter {
 public:
  SubColumnBlockIter()
      : BlockIter(),
        count_(0),
        idx_(0),
        fixed_length_(std::numeric_limits<uint32_t>::max()),
        size_length_(0) {}
  SubColumnBlockIter(const Comparator* comparator, const char* data,
                     uint32_t restarts, uint32_t num_restarts);

  virtual void Initialize(const Comparator* comparator, const char* data,
                          uint32_t restarts, uint32_t num_restarts) override;

  virtual void Seek(const Slice& target) override;

  void NextValue() {
    if (++idx_ < count_) {
      value_ = values_[idx_];
    } else {
      restart_index_++;
      ParseNextRestart();
    }
  }

  void SeekToFirstInBatch();

 private:
  virtual bool ParseNextKey() override;

  bool ParseNextRestart();

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index) override;

  Slice values_[ColumnTableOptions::kMaxRestartInterval];
  uint32_t count_;
  uint32_t idx_;
  uint32_t fixed_length_;
  uint32_t size_length_;
};

// Main column block iterator, used in main columns' data block
class MainColumnBlockIter final : public BlockIter {
 public:
  MainColumnBlockIter()
      : BlockIter(),
        has_val_(false),
        int_val_(0),
        count_(0),
        idx_(0),
        fixed_length_(std::numeric_limits<uint32_t>::max()),
        size_length_(0) {}
  MainColumnBlockIter(const Comparator* comparator, const char* data,
                      uint32_t restarts, uint32_t num_restarts);

  virtual void Initialize(const Comparator* comparator, const char* data,
                          uint32_t restarts, uint32_t num_restarts) override;

  void NextKey() {
    if (++idx_ < count_) {
      key_.SetKey(keys_[idx_], false);
    } else {
      restart_index_++;
      ParseNextRestart();
    }
  }

  void SeekToFirstInBatch();

 private:
  // Return the offset in data_ just past the end of the current entry.
  virtual uint32_t NextEntryOffset() const override {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>(key_.GetKey().data() + key_.Size() - data_ +
                                 (has_val_ ? 4 : 0));
  }

  virtual void SeekToRestartPoint(uint32_t index) override {
    BlockIter::SeekToRestartPoint(index);
    key_.SetKey(value_, false);

    has_val_ = false;
    int_val_ = 0;
    str_val_.clear();
  }

  virtual void CorruptionError() override;

  virtual bool ParseNextKey() override;

  bool ParseNextRestart();

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index) override;

  bool has_val_;
  uint32_t int_val_;     // integer representation of sequence value
  std::string str_val_;  // big endian representation of sequence value

  Slice keys_[ColumnTableOptions::kMaxRestartInterval];
  uint32_t count_;
  uint32_t idx_;
  uint32_t fixed_length_;
  uint32_t size_length_;
};

// Min max block iterator, used in sub columns' index block
class MinMaxBlockIter final : public BlockIter {
 public:
  MinMaxBlockIter() : BlockIter(), max_storage_len_(0) {}
  MinMaxBlockIter(const Comparator* comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts);

  virtual void Initialize(const Comparator* comparator, const char* data,
                          uint32_t restarts, uint32_t num_restarts) override;

  virtual Slice min() const override {
    assert(Valid());
    return min_;
  }

  virtual Slice max() const override {
    assert(Valid());
    return max_;
  }

 private:
  // Return the offset in data_ just past the end of the current entry.
  virtual uint32_t NextEntryOffset() const override {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>(min_.data() + min_.size() + max_storage_len_ -
                                 data_);
  }

  virtual void SeekToRestartPoint(uint32_t index) override {
    key_.Clear();
    value_.clear();
    max_.clear();
    restart_index_ = index;
    max_storage_len_ = 0;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of max_, but max is not continuously
    // stored, so set min_accordingly
    uint32_t offset = GetRestartPoint(index);
    min_ = Slice(data_ + offset, 0);
  }

  virtual void CorruptionError() override;

  virtual bool ParseNextKey() override;

  Slice min_;
  std::string max_;
  uint32_t max_storage_len_;
};

}  // namespace vidardb
