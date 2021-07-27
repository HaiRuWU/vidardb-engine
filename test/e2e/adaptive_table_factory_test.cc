// Copyright (c) 2020-present, VidarDB, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
using namespace std;

#include "vidardb/comparator.h"
#include "vidardb/db.h"
#include "vidardb/file_iter.h"
#include "vidardb/options.h"
#include "vidardb/splitter.h"
#include "vidardb/status.h"
#include "vidardb/table.h"
using namespace vidardb;

enum kTableType { ROW, COLUMN };
const unsigned int kColumn = 3;
const string kDBPath = "/tmp/vidardb_adaptive_table_factory_test";

void TestAdaptiveTableFactory(bool flush, kTableType table,
                              vector<uint32_t> cols) {
  cout << "cols: { ";
  for (auto col : cols) {
    cout << col << " ";
  }
  cout << "}" << endl;

  int ret = system(string("rm -rf " + kDBPath).c_str());

  Options options;
  options.create_if_missing = true;
  options.splitter.reset(NewEncodingSplitter());
  #ifndef VIDARDB_LITE
  options.OptimizeAdaptiveLevelStyleCompaction();
  #endif
  int knob = -1;  // row
  if (table == COLUMN) {
    knob = 0;
  }

  shared_ptr<TableFactory> block_based_table(NewBlockBasedTableFactory());
  shared_ptr<TableFactory> column_table(NewColumnTableFactory());
  ColumnTableOptions* column_opts =
      static_cast<ColumnTableOptions*>(column_table->GetOptions());
  column_opts->column_count = kColumn;
  for (auto i = 0u; i < column_opts->column_count; i++) {
    column_opts->value_comparators.push_back(BytewiseComparator());
  }
  options.table_factory.reset(NewAdaptiveTableFactory(
      block_based_table, block_based_table, column_table, knob));

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  WriteOptions wo;
  s = db->Put(wo, "1", options.splitter->Stitch({"chen1", "33", "hangzhou"}));
  assert(s.ok());
  s = db->Put(wo, "2", options.splitter->Stitch({"wang2", "32", "wuhan"}));
  assert(s.ok());
  s = db->Put(wo, "3", options.splitter->Stitch({"zhao3", "35", "nanjing"}));
  assert(s.ok());
  s = db->Put(wo, "4", options.splitter->Stitch({"liao4", "28", "beijing"}));
  assert(s.ok());
  s = db->Put(wo, "5", options.splitter->Stitch({"jiang5", "30", "shanghai"}));
  assert(s.ok());
  s = db->Put(wo, "6", options.splitter->Stitch({"lian6", "30", "changsha"}));
  assert(s.ok());
//  s = db->Delete(wo, "1");
//  assert(s.ok());
//  s = db->Put(wo, "3", options.splitter->Stitch({"zhao333", "35", "nanjing"}));
//  assert(s.ok());
//  s = db->Put(wo, "6", options.splitter->Stitch({"lian666", "30", "changsha"}));
//  assert(s.ok());
//  s = db->Put(wo, "1",
//              options.splitter->Stitch({"chen1111", "33", "hangzhou"}));
//  assert(s.ok());
//  s = db->Delete(wo, "3");
//  assert(s.ok());

  if (flush) {  // flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());
  }

  ReadOptions ro;
  ro.columns = cols;

  FileIter* iter = dynamic_cast<FileIter*>(db->NewFileIterator(ro));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    vector<vector<MinMax>> v;
    s = iter->GetMinMax(v, nullptr);
    assert(s.ok() || s.IsNotFound());
    if (s.IsNotFound()) continue;

    // block_bits is set for illustration purpose here.
    vector<bool> block_bits(1, true);
    uint64_t N = iter->EstimateRangeQueryBufSize(
        ro.columns.empty() ? 4 : ro.columns.size());
    char* buf = new char[N];
    uint64_t valid_count, total_count;
    s = iter->RangeQuery(block_bits, buf, N, &valid_count, &total_count);
    assert(s.ok());

    char* limit = buf + N;
    uint64_t* end = reinterpret_cast<uint64_t*>(limit);
    for (auto c : ro.columns) {
      for (int i = 0; i < valid_count; ++i) {
        uint64_t offset = *(--end), size = *(--end);
        cout << Slice(buf + offset, size).ToString() << " ";
      }
      cout << endl;
      limit -= total_count * 2 * sizeof(uint64_t);
      end = reinterpret_cast<uint64_t*>(limit);
    }
    delete[] buf;
  }
  delete iter;

  delete db;
  cout << endl;
}

int main() {
  TestAdaptiveTableFactory(false, ROW, {1, 3});
  TestAdaptiveTableFactory(false, ROW, {0});
  TestAdaptiveTableFactory(true, ROW, {1, 3});
  TestAdaptiveTableFactory(true, ROW, {0});

  TestAdaptiveTableFactory(false, COLUMN, {1, 3});
  TestAdaptiveTableFactory(false, COLUMN, {0});
  TestAdaptiveTableFactory(true, COLUMN, {1, 3});
  TestAdaptiveTableFactory(true, COLUMN, {0});

  return 0;
}
