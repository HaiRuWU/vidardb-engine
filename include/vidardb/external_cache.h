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

typedef bool (*LookupFunc)(const char*, size_t, size_t, size_t*, size_t*);
typedef void (*InsertFunc)(const char*, size_t, size_t, char* const, size_t,
                           const char*, size_t);

// A fake version only support single process & single thread. To support
// multi-process access, shared hash table should be employed.
static std::unordered_map<std::string, std::pair<size_t, size_t>> h_;
static size_t next_ = 0;

static bool simple_lookup(const char* key, size_t klen, size_t shared_id,
                          size_t* off, size_t* size) {
  auto it = h_.find(std::string(key, klen));
  if (it == h_.end()) {
    *off = 0;
    *size = 0;
    return false;
  } else {
    *off = it->second.first;
    *size = it->second.second;
    return true;
  }
}

static void simple_insert(const char* key, size_t klen, size_t shared_id,
                          char* const header, size_t capacity,
                          const char* data, size_t size) {
  if (capacity - next_ < size) {
    h_.clear();
    next_ = 0;
  }

  h_[std::string(key, klen)] = std::make_pair(next_, size);

  memcpy(header + next_, data, size);
  next_ += size;
}

// ExternalCache
//
// External cache accepts a given external area as a source to cache data. The
// purpose of an external cache is to serve the range query. The cache interface
// is specifically designed for external cache.
class ExternalCache {
 public:
  ExternalCache(char* const header = nullptr, size_t capacity = 0,
                size_t shared_id = 0, LookupFunc lookup = simple_lookup,
                InsertFunc insert = simple_insert)
      : header_(header),
        capacity_(capacity),
        shared_id_(shared_id),
        lookup_(lookup),
        insert_(insert) {}

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

    insert_(key.ToString().c_str(), key.size(), shared_id_, header_, capacity_,
            data, size);

    return s;
  }

  // Lookup cache by page identifier
  //
  // key        Page identifier
  // data       Place where the data should be put
  // size       Size of the page
  void Lookup(const Slice& key, char*& data, size_t& size) {
    size_t offset;
    if (lookup_(key.ToString().c_str(), key.size(), shared_id_, &offset,
                &size)) {
      data = header_ + offset;
    } else {
      data = nullptr;
    }
  }

  const char* header() const { return header_; }
  size_t capacity() const { return capacity_; }

 private:
  char* const header_;
  size_t capacity_;
  size_t shared_id_;

  LookupFunc lookup_;
  InsertFunc insert_;
};

}  // namespace vidardb
