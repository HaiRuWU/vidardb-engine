//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include "table/block.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "vidardb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/perf_context_imp.h"

namespace vidardb {

BlockIter::BlockIter()
    : comparator_(nullptr),
      data_(nullptr),
      restarts_(0),
      num_restarts_(0),
      current_(0),
      restart_index_(0),
      status_(Status::OK()) {}

BlockIter::BlockIter(const Comparator* comparator, const char* data,
                     uint32_t restarts, uint32_t num_restarts)
    : BlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void BlockIter::Initialize(const Comparator* comparator, const char* data,
                           uint32_t restarts, uint32_t num_restarts) {
  //  assert(data_ == nullptr);  // Now we allow it to get called multiple times
  // as long as its resource is released
  assert(num_restarts > 0);  // Ensure the param is valid

  comparator_ = comparator;
  data_ = data;
  restarts_ = restarts;
  num_restarts_ = num_restarts;
  current_ = restarts_;
  restart_index_ = num_restarts_;
}

#ifndef NDEBUG
BlockIter::~BlockIter() {
  // Assert that the BlockIter is never deleted while Pinning is Enabled.
  assert(!pinned_iters_mgr_ ||
         (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
}
#endif

void BlockIter::Prev() {
  assert(Valid());

  // Scan backwards to a restart point before current_
  const uint32_t original = current_;
  while (GetRestartPoint(restart_index_) >= original) {
    if (restart_index_ == 0) {
      // No more entries
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return;
    }
    restart_index_--;
  }

  SeekToRestartPoint(restart_index_);
  do {
    // Loop until end of current entry hits the start of original entry
  } while (ParseNextKey() && NextEntryOffset() < original);
}

void BlockIter::Seek(const Slice& target) {
  PERF_TIMER_GUARD(block_seek_nanos);
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  uint32_t index = 0;
  bool ok = BinarySeek(target, 0, num_restarts_ - 1, &index);
  if (!ok) {
    return;
  }
  SeekToRestartPoint(index);
  // Linear search (within restart area) for first key >= target

  while (true) {
    if (!ParseNextKey() || Compare(key_.GetKey(), target) >= 0) {
      return;
    }
  }
}

void BlockIter::SeekToFirst() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(0);
  ParseNextKey();
}

void BlockIter::SeekToLast() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(num_restarts_ - 1);
  while (ParseNextKey() && NextEntryOffset() < restarts_) {
    // Keep skipping
  }
}

void BlockIter::CorruptionError() {
  current_ = restarts_;
  restart_index_ = num_restarts_;
  status_ = Status::Corruption("bad entry in block");
  key_.Clear();
  value_.clear();
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not derefence past "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static const char* DecodeEntry(const char* p, const char* limit,
                               uint32_t* shared, uint32_t* non_shared,
                               uint32_t* value_length) {
  if (limit - p < 3) return nullptr;
  *shared = reinterpret_cast<const unsigned char*>(p)[0];
  *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
  *value_length = reinterpret_cast<const unsigned char*>(p)[2];
  if ((*shared | *non_shared | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 3;
  } else {
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
    return nullptr;
  }
  return p;
}

bool BlockIter::ParseNextKey() {
  current_ = NextEntryOffset();
  const char* p = data_ + current_;
  const char* limit = data_ + restarts_;  // Restarts come right after data
  if (p >= limit) {
    // No more entries to return.  Mark as invalid.
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return false;
  }

  // Decode next entry
  uint32_t shared, non_shared, value_length;
  p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
  if (p == nullptr || key_.Size() < shared) {
    CorruptionError();
    return false;
  } else {
    if (shared == 0) {
      // If this key don't share any bytes with prev key then we don't need
      // to decode it and can use it's address in the block directly.
      key_.SetKey(Slice(p, non_shared), false /* copy */);
    } else {
      // This key share `shared` bytes with prev key, we need to decode it
      key_.TrimAppend(shared, p, non_shared);
    }

    value_ = Slice(p + non_shared, value_length);
    while (restart_index_ + 1 < num_restarts_ &&
           GetRestartPoint(restart_index_ + 1) < current_) {
      ++restart_index_;
    }
    return true;
  }
}

// Binary search in restart array to find the first restart point
// with a key >= target (TODO: this comment is inaccurate)
bool BlockIter::BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                           uint32_t* index) {
  assert(left <= right);

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = GetRestartPoint(mid);
    uint32_t shared, non_shared, value_length;
    const char* key_ptr =
        DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                    &non_shared, &value_length);
    if (key_ptr == nullptr || (shared != 0)) {
      CorruptionError();
      return false;
    }
    Slice mid_key(key_ptr, non_shared);
    int cmp = Compare(mid_key, target);
    if (cmp < 0) {
      // Key at "mid" is smaller than "target". Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (cmp > 0) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  *index = left;
  return true;
}

SubColumnBlockIter::SubColumnBlockIter(const Comparator* comparator,
                                       const char* data, uint32_t restarts,
                                       uint32_t num_restarts)
    : SubColumnBlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void SubColumnBlockIter::Initialize(const Comparator* comparator,
                                    const char* data, uint32_t restarts,
                                    uint32_t num_restarts) {
  // reduce num_restarts to reflect the true value, because the last one is
  // fixed_length
  BlockIter::Initialize(comparator, data, restarts, --num_restarts);
  count_ = 0;
  fixed_length_ =
      DecodeFixed32(data_ + restarts_ + num_restarts * sizeof(uint32_t));
  size_length_ = VarintLength(fixed_length_);
}

void SubColumnBlockIter::Seek(const Slice& target) {
  PERF_TIMER_GUARD(block_seek_nanos);
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  uint32_t index = 0;
  bool ok = BinarySeek(target, 0, num_restarts_ - 1, &index);
  if (!ok) {
    return;
  }

  SeekToRestartPoint(index);

  if (!ParseNextKey()) {
    return;
  }

  Slice key = key_.GetKey();
  uint32_t restart_pos = 0;
  GetFixed32BigEndian(&key, &restart_pos);

  uint32_t target_pos = 0;
  GetFixed32BigEndian(&target, &target_pos);

  uint32_t step = target_pos - restart_pos;

  // Linear search (within restart area) for first key >= target
  for (uint32_t i = 0u; i < step; i++) {
    if (!ParseNextKey()) {
      return;
    }
  }
}

void SubColumnBlockIter::SeekToFirstInBatch() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(0);
  ParseNextRestart();
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of the length of the key or value in "key_length"
// or "*value_length". Will not derefence past "limit".
//
// If any errors are detected, returns nullptr. Otherwise, returns a
// pointer to the key delta (just past the decoded values).
static const char* DecodeKeyOrValue(const char* p, const char* limit,
                                    uint32_t* length) {
  if (limit - p < 1) return nullptr;
  *length = reinterpret_cast<const unsigned char*>(p)[0];
  if (*length < 128) {
    // Fast path: key_length is encoded in one byte each
    p++;
  } else {
    if ((p = GetVarint32Ptr(p, limit, length)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < *length) {
    return nullptr;
  }
  return p;
}

bool SubColumnBlockIter::ParseNextKey() {
  current_ = NextEntryOffset();
  const char* p = data_ + current_;
  const char* limit = data_ + restarts_;  // Restarts come right after data
  if (p >= limit) {
    // No more entries to return.  Mark as invalid.
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return false;
  }

  while (restart_index_ + 1 < num_restarts_ &&
         GetRestartPoint(restart_index_ + 1) <= current_) {
    ++restart_index_;
  }

  uint32_t restart_offset = GetRestartPoint(restart_index_);
  // within the restart area, key is not stored because it is merely sequence
  bool has_key = (restart_offset == current_);

  // Decode next entry
  uint32_t key_length = 0;
  if (has_key) {
    p = DecodeKeyOrValue(p, limit, &key_length);
    if (p == nullptr) {
      CorruptionError();
      return false;
    }
    key_.SetKey(Slice(p, key_length), false /* copy */);
  }
  p += key_length;
  uint32_t value_length = 0;
  p = DecodeKeyOrValue(p, limit, &value_length);
  if (p == nullptr) {
    CorruptionError();
    return false;
  }

  value_ = Slice(p, value_length);
  return true;
}

bool SubColumnBlockIter::ParseNextRestart() {
  current_ = NextEntryOffset();  // should be at the end of a restart interval
  count_ = 0;
  if (current_ >= restarts_) {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return false;
  }

  uint32_t restart_offset = GetRestartPoint(restart_index_);
  uint32_t next_restart_offset = (restart_index_ + 1) < num_restarts_
                                     ? GetRestartPoint(restart_index_ + 1)
                                     : restarts_;

  const char* p = data_ + restart_offset;
  const char* limit = data_ + next_restart_offset;

  uint32_t key_length = 0;
  p = DecodeKeyOrValue(p, limit, &key_length);
  if (p == nullptr) {
    CorruptionError();
    return false;
  }
  key_.SetKey(Slice(p, key_length), false /* copy */);
  p += key_length;

  if (fixed_length_ != std::numeric_limits<uint32_t>::max()) {
    uint32_t per_value_length = size_length_ + fixed_length_;
    count_ = (limit - p) / per_value_length;
    for (uint32_t i = 0; i < count_; i++) {
      values_[i] =
          Slice(p + i * per_value_length + size_length_, fixed_length_);
    }
  } else {
    while (p < limit) {
      uint32_t value_length = 0;
      p = DecodeKeyOrValue(p, limit, &value_length);
      values_[count_++] = Slice(p, value_length);
      p += value_length;
    }
  }

  value_ = values_[0];
  return true;
}

// Binary search in restart array to find the first restart point
// with a key >= target (TODO: this comment is inaccurate)
bool SubColumnBlockIter::BinarySeek(const Slice& target, uint32_t left,
                                    uint32_t right, uint32_t* index) {
  assert(left <= right);

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = GetRestartPoint(mid);
    uint32_t key_length;
    const char* key_ptr =
        DecodeKeyOrValue(data_ + region_offset, data_ + restarts_, &key_length);
    if (key_ptr == nullptr) {
      CorruptionError();
      return false;
    }
    Slice mid_key(key_ptr, key_length);
    int cmp = Compare(mid_key, target);
    if (cmp < 0) {
      // Key at "mid" is smaller than "target". Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (cmp > 0) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  *index = left;
  return true;
}

MainColumnBlockIter::MainColumnBlockIter(const Comparator* comparator,
                                         const char* data, uint32_t restarts,
                                         uint32_t num_restarts)
    : MainColumnBlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void MainColumnBlockIter::Initialize(const Comparator* comparator,
                                     const char* data, uint32_t restarts,
                                     uint32_t num_restarts) {
  // reduce num_restarts to reflect the true value, because the last one is
  // fixed_length
  BlockIter::Initialize(comparator, data, restarts, --num_restarts);
  has_val_ = false;
  int_val_ = 0;
  str_val_.clear();
  count_ = 0;
  fixed_length_ =
      DecodeFixed32(data_ + restarts_ + num_restarts * sizeof(uint32_t));
  size_length_ = VarintLength(fixed_length_);
}

void MainColumnBlockIter::SeekToFirstInBatch() {
  if (data_ == nullptr) {  // Not init yet
    return;
  }
  SeekToRestartPoint(0);
  ParseNextRestart();
}

void MainColumnBlockIter::CorruptionError() {
  BlockIter::CorruptionError();
  has_val_ = false;
  int_val_ = 0;
  str_val_.empty();
}

bool MainColumnBlockIter::ParseNextKey() {
  current_ = NextEntryOffset();
  const char* p = data_ + current_;
  const char* limit = data_ + restarts_;  // Restarts come right after data
  if (p >= limit) {
    // No more entries to return.  Mark as invalid.
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return false;
  }

  // Decode next entry
  uint32_t key_length = 0;
  p = DecodeKeyOrValue(p, limit, &key_length);
  if (p == nullptr) {
    CorruptionError();
    return false;
  }
  key_.SetKey(Slice(p, key_length), false /* copy */);

  while (restart_index_ + 1 < num_restarts_ &&
         GetRestartPoint(restart_index_ + 1) <= current_) {
    ++restart_index_;
  }

  uint32_t restart_offset = GetRestartPoint(restart_index_);
  // within the restart area, val is not stored because it is merely sequence
  has_val_ = (restart_offset == current_);

  uint32_t value_length = 4;  // fixed 32 bits
  if (has_val_) {
    value_ = Slice(key_.GetKey().data() + key_.GetKey().size(), value_length);
    GetFixed32BigEndian(&value_, &int_val_);
  } else {
    PutFixed32BigEndian(&str_val_, ++int_val_);
    value_ = Slice(str_val_);
  }

  return true;
}

bool MainColumnBlockIter::ParseNextRestart() {
  current_ = NextEntryOffset();  // should be at the end of a restart interval
  count_ = 0;
  has_val_ = false;
  if (current_ >= restarts_) {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return false;
  }

  uint32_t restart_offset = GetRestartPoint(restart_index_);
  uint32_t next_restart_offset = (restart_index_ + 1) < num_restarts_
                                     ? GetRestartPoint(restart_index_ + 1)
                                     : restarts_;

  const char* p = data_ + restart_offset;
  const char* limit = data_ + next_restart_offset;

  uint32_t key_length = 0;
  p = DecodeKeyOrValue(p, limit, &key_length);
  if (p == nullptr) {
    CorruptionError();
    return false;
  }
  key_.SetKey(Slice(p, key_length), false /* copy */);
  keys_[count_++] = key_.GetKey();
  p += key_length;

  value_ = Slice(p, 4);
  p += 4;

  if (p == limit) {
    // just one element in restart
    has_val_ = true;
    return true;
  }

  if (fixed_length_ != std::numeric_limits<uint32_t>::max()) {
    uint32_t per_key_length = size_length_ + fixed_length_;
    count_ = (limit - p) / per_key_length + 1;
    for (uint32_t i = 1; i < count_; i++) {
      keys_[i] =
          Slice(p + (i - 1) * per_key_length + size_length_, fixed_length_);
    }
  } else {
    while (p < limit) {
      p = DecodeKeyOrValue(p, limit, &key_length);
      keys_[count_++] = Slice(p, key_length);
      p += key_length;
    }
  }

  return true;
}

// Binary search in restart array to find the first restart point
// with a key >= target
bool MainColumnBlockIter::BinarySeek(const Slice& target, uint32_t left,
                                     uint32_t right, uint32_t* index) {
  assert(left <= right);

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = GetRestartPoint(mid);
    uint32_t key_length = 0;
    const char* key_ptr =
        DecodeKeyOrValue(data_ + region_offset, data_ + restarts_, &key_length);
    if (key_ptr == nullptr) {
      CorruptionError();
      return false;
    }
    Slice mid_key(key_ptr, key_length);
    int cmp = Compare(mid_key, target);
    if (cmp < 0) {
      // Key at "mid" is smaller than "target". Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else if (cmp > 0) {
      // Key at "mid" is >= "target". Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    } else {
      left = right = mid;
    }
  }

  *index = left;
  return true;
}

MinMaxBlockIter::MinMaxBlockIter(const Comparator* comparator, const char* data,
                                 uint32_t restarts, uint32_t num_restarts)
    : MinMaxBlockIter() {
  Initialize(comparator, data, restarts, num_restarts);
}

void MinMaxBlockIter::Initialize(const Comparator* comparator, const char* data,
                                 uint32_t restarts, uint32_t num_restarts) {
  BlockIter::Initialize(comparator, data, restarts, num_restarts);
  max_storage_len_ = 0;
  min_.clear();
  max_.clear();
}

void MinMaxBlockIter::CorruptionError() {
  BlockIter::CorruptionError();
  min_.clear();
  max_.clear();
  max_storage_len_ = 0;
}

// Helper routine: decode the next block max starting at "p",
// storing the number of shared bytes, non_shared bytes, in "*shared",
// "*non_shared" respectively.  Will not derefence past "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static const char* DecodeMax(const char* p, const char* limit, uint32_t* shared,
                             uint32_t* non_shared) {
  if (limit - p < 2) return nullptr;
  *shared = reinterpret_cast<const unsigned char*>(p)[0];
  *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
  if ((*shared | *non_shared) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 2;
  } else {
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < *non_shared) {
    return nullptr;
  }
  return p;
}

bool MinMaxBlockIter::ParseNextKey() {
  bool ret = BlockIter::ParseNextKey();
  if (!ret) {
    return false;
  }

  const char* p = value_.data_ + value_.size_;
  const char* limit = data_ + restarts_;  // Restarts come right after data
  // Decode min
  uint32_t shared, non_shared;
  p = DecodeKeyOrValue(p, limit, &non_shared);
  if (p == nullptr) {
    CorruptionError();
    return false;
  }
  min_ = Slice(p, non_shared);
  p += non_shared;
  const char* max_start = p;

  // Decode max
  p = DecodeMax(p, limit, &shared, &non_shared);
  if (p == nullptr || min_.size() < shared) {
    CorruptionError();
    return false;
  }
  max_.clear();
  max_.assign(min_.data(), shared);
  max_.append(p, non_shared);

  p += non_shared;
  max_storage_len_ = p - max_start;
  return true;
}

uint32_t Block::NumRestarts() const {
  assert(size_ >= 2 * sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(BlockContents&& contents)
    : contents_(std::move(contents)),
      data_(contents_.data.data()),
      size_(contents_.data.size()) {
  if (size_ < sizeof(uint32_t)) {
    size_ = 0;  // Error marker
  } else {
    restart_offset_ =
        static_cast<uint32_t>(size_) - (1 + NumRestarts()) * sizeof(uint32_t);
    if (restart_offset_ > size_ - sizeof(uint32_t)) {
      // The size is too small for NumRestarts() and therefore
      // restart_offset_ wrapped around.
      size_ = 0;
    }
  }
}

InternalIterator* Block::NewIterator(const Comparator* cmp, BlockIter* iter,
                                     BlockType type) {
  if (size_ < 2*sizeof(uint32_t)) {
    if (iter != nullptr) {
      iter->SetStatus(Status::Corruption("bad block contents"));
      return iter;
    } else {
      return NewErrorInternalIterator(Status::Corruption("bad block contents"));
    }
  }
  const uint32_t num_restarts = NumRestarts();
  if (num_restarts == 0) {
    if (iter != nullptr) {
      iter->SetStatus(Status::OK());
      return iter;
    } else {
      return NewEmptyInternalIterator();
    }
  } else {
    if (iter != nullptr) {
      iter->Initialize(cmp, data_, restart_offset_, num_restarts);
    } else {
      switch (type) {
        case kTypeBlock:
          return new BlockIter(cmp, data_, restart_offset_, num_restarts);
        case kTypeMainColumn:
          return new MainColumnBlockIter(cmp, data_, restart_offset_,
                                         num_restarts);
        case kTypeSubColumn:
          return new SubColumnBlockIter(cmp, data_, restart_offset_,
                                        num_restarts);
        case kTypeMinMax:
          return new MinMaxBlockIter(cmp, data_, restart_offset_, num_restarts);
        default:
          return NewErrorInternalIterator(
              Status::Corruption("unknown block type"));
      }
    }
  }

  return iter;
}

size_t Block::ApproximateMemoryUsage() const {
  return usable_size();
}

}  // namespace vidardb
