//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/comparator.h"
#include "vidardb/db.h"
#include "vidardb/file_iter.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/table.h"
using namespace vidardb;

unsigned int M = 3;
unsigned int P = 4096;
string kDBPath = "/tmp/vidardb_simple_column_example";

int main() {
  int ret = system(string("rm -rf " + kDBPath).c_str());

  DB* db;
  Options options;
  options.splitter.reset(NewPipeSplitter());

  TableFactory* table_factory = NewColumnTableFactory();
  ColumnTableOptions* opts =
      static_cast<ColumnTableOptions*>(table_factory->GetOptions());
  opts->column_count = M;
  for (auto i = 0u; i < opts->column_count; i++) {
    opts->value_comparators.push_back(BytewiseComparator());
  }

  char cache[P];
  opts->external_cache.reset(new ExternalCache(cache, sizeof(cache)));
  const char* header = opts->external_cache->header();

  options.table_factory.reset(table_factory);

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "column1",
              options.splitter->Stitch({"val11", "val12", "val13"}));
  s = db->Put(WriteOptions(), "column2",
              options.splitter->Stitch({"val21", "val22", "val23"}));
  assert(s.ok());

  // test memtable or sstable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions ro;
  ro.columns = {1, 3};
//  ro.columns = {0};

  cout << "Range Query: " << endl;
  FileIter* file_iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (file_iter->SeekToFirst(); file_iter->Valid(); file_iter->Next()) {
    vector<vector<MinMax>> v;
    s = file_iter->GetMinMax(v, nullptr);
    assert(s.ok() || s.IsNotFound());
    if (s.IsNotFound()) continue;

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    bool external_cache = false;
    uint64_t N = file_iter->EstimateRangeQueryBufSize(
        ro.columns.empty() ? 4 : ro.columns.size(), external_cache);
    char buf[N];
    uint64_t valid_count, total_count;
    s = file_iter->RangeQuery(block_bits, buf, N, valid_count, total_count);
    assert(s.ok());

    char* limit = buf + N;
    uint64_t* end = reinterpret_cast<uint64_t*>(limit);
    for (auto c : ro.columns) {
      for (int i = 0; i < valid_count; ++i) {
        uint64_t offset = *(--end), size = *(--end);
        cout << Slice((external_cache ? header : buf) + offset, size).ToString()
             << " ";
      }
      cout << endl;
      limit -= total_count * 2 * sizeof(uint64_t);
      end = reinterpret_cast<uint64_t*>(limit);
    }
  }
  delete file_iter;
  cout << endl;

  string val;
  s = db->Get(ro, "column2", &val);
  if (!s.ok()) cout << "Get not ok!" << endl;
  cout << "Get column2: " << val << endl;

  Iterator* it = db->NewIterator(ro);
  it->Seek("column1");
  if (it->Valid()) {
    cout << "column1 value: " << it->value().ToString() << endl;
  }
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout << "key: " << it->key().ToString()
         << " value: " << it->value().ToString() << endl;
    s = db->Delete(WriteOptions(), it->key());
    if (!s.ok()) cout << "Delete not ok!" << endl;
  }
  delete it;

  delete db;

  cout << "finished!" << endl;
  return 0;
}
