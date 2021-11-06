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

typedef bool (*LookupPtr)(const char*, size_t*, size_t*);
typedef void (*InsertPtr)(const char*, size_t, size_t);
typedef void (*ClearPtr)(void);

// A fake version only support single process & single thread. To support
// multi-process access, shared hash table should be employed.
static std::unordered_map<std::string, std::pair<size_t, size_t>> h_;

static bool simple_lookup(const char* key, size_t* offset, size_t* size) {
  auto it = h_.find(std::string(key));
  if (it == h_.end()) {
    *offset = 0;
    *size = 0;
    return false;
  } else {
    *offset = it->second.first;
    *size = it->second.second;
    return true;
  }
}

static void simple_insert(const char* key, size_t offset, size_t size) {
  h_[std::string(key)] = std::make_pair(offset, size);
}

static void simple_clear(void) {
  h_.clear();
}

// ExternalCache
//
// External cache accepts a given external area as a source to cache data. The
// purpose of an external cache is to serve the range query. The cache interface
// is specifically designed for external cache.
class ExternalCache {
 public:
  ExternalCache(char* const header = nullptr, size_t capacity = 0,
                LookupPtr lookup = simple_lookup,
                InsertPtr insert = simple_insert,
                ClearPtr clear = simple_clear)
      : header_(header), capacity_(capacity), next_(0),
        lookup_(lookup), insert_(insert), clear_(clear) {}

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
      clear_();
      next_ = 0;
    }

    insert_(key.ToString().c_str(), next_, size);
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
    size_t offset;
    if (lookup_(key.ToString().c_str(), &offset, &size)) {
      data = header_ + offset;
    } else {
      data = nullptr;
    }
  }

  const char* header() const { return header_; }
  size_t capacity() const { return capacity_; }

 private:
  char* const header_ = nullptr;
  size_t capacity_ = 0;
  size_t next_ = 0;

  LookupPtr lookup_;
  InsertPtr insert_;
  ClearPtr clear_;
};

}  // namespace vidardb
