//  Copyright (c) 2021-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include <unordered_map>

#include "vidardb/slice.h"
#include "vidardb/status.h"

namespace vidardb {

// ExternalCache
//
// External cache accepts a given external area as a source to cache data. The
// purpose of an external cache is to serve the range query. The cache interface
// is specifically designed for external cache.
class ExternalCache {
 public:
  ExternalCache(char* const header = nullptr, size_t capacity = 0)
      : header_(header), capacity_(capacity), next_(0) {}

  virtual ~ExternalCache() {}

  // Insert to cache
  //
  // key        Identifier to identify a page uniquely
  // data       Page data
  // size       Size of the page
  //
  // TODO: currently a very simple replacement policy similar to FIFO but even
  // simpler. More advanced policy should be employed in the future.
  Status Insert(const Slice& key, const char* data, size_t size) {
    Status s;
    if (size > capacity_) {
      s = Status::Incomplete("Insert failed due to limited cache capacity.");
    }

    if (capacity_ - next_ < size) {
      h_.clear();
      next_ = 0;
    }

    h_[key.ToString()] = std::make_pair(next_, size);
    memcpy(header_ + next_, data, size);
    next_ += size;
    return s;
  }

  // Lookup cache by page identifier
  //
  // key        Page identifier
  // data       Place where the data should be put
  // size       Size of the page
  void Lookup(const Slice& key, char*& data, size_t& size) {
    auto it = h_.find(key.ToString());
    if (it == h_.end()) {
      data = nullptr;
      size = 0;
    } else {
      data = header_ + it->second.first;
      size = it->second.second;
    }
  }

  const char* header() const { return header_; }
  size_t capacity() const { return capacity_; }

 private:
  char* const header_ = nullptr;
  size_t capacity_ = 0;
  size_t next_ = 0;
  // TODO: currently only support single process & single thread. To support
  // multi-process access, shared hash table should be employed.
  std::unordered_map<std::string, std::pair<size_t, size_t>> h_;
};

}  // namespace vidardb
