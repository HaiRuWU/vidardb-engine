//  Copyright (c) 2019-present, VidarDB, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "vidardb/options.h"
#include "vidardb/immutable_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>

#include "vidardb/cache.h"
#include "vidardb/comparator.h"
#include "vidardb/env.h"
#include "vidardb/sst_file_manager.h"
#include "vidardb/memtablerep.h"
#include "vidardb/slice.h"
#include "vidardb/table.h"
#include "vidardb/table_properties.h"
#include "table/block_based_table_factory.h"
#include "util/compression.h"
#include "util/statistics.h"

namespace vidardb {

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : compaction_style(options.compaction_style),
      compaction_options_fifo(options.compaction_options_fifo),
      comparator(options.comparator),
      splitter(options.splitter.get()),
      info_log(options.info_log.get()),
      statistics(options.statistics.get()),
      env(options.env),
      delayed_write_rate(options.delayed_write_rate),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      db_paths(options.db_paths),
      memtable_factory(options.memtable_factory.get()),
      table_factory(options.table_factory.get()),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      advise_random_on_open(options.advise_random_on_open),
      disable_data_sync(options.disableDataSync),
      use_fsync(options.use_fsync),
      compression_per_level(options.compression_per_level),
      bottommost_compression(options.bottommost_compression),
      compression_opts(options.compression_opts),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(options.compaction_readahead_size),
      num_levels(options.num_levels),
      listeners(options.listeners) {}

ColumnFamilyOptions::ColumnFamilyOptions()
    : comparator(BytewiseComparator()),
      splitter(nullptr),  // compatible with row store
      write_buffer_size(512 << 20),
      max_write_buffer_number(2),
      min_write_buffer_number_to_merge(1),
      max_write_buffer_number_to_maintain(0),
      compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
      bottommost_compression(kDisableCompressionOption),
      num_levels(7),
      level0_file_num_compaction_trigger(4),
      target_file_size_base(64 * 1048576),
      target_file_size_multiplier(1),
      max_bytes_for_level_base(256 * 1048576),
      max_bytes_for_level_multiplier(10),
      max_bytes_for_level_multiplier_additional(num_levels, 1),
      expanded_compaction_factor(25),
      source_compaction_factor(1),
      max_grandparent_overlap_factor(10),
      arena_block_size(0),
      disable_auto_compactions(false),
      compaction_style(kCompactionStyleLevel),
      compaction_pri(kByCompensatedSize),
      verify_checksums_in_compaction(true),
      memtable_factory(std::shared_ptr<SkipListFactory>(new SkipListFactory)),
      table_factory(
          std::shared_ptr<TableFactory>(new BlockBasedTableFactory())),
      paranoid_file_checks(false),
      report_bg_io_stats(false) {
  assert(memtable_factory.get() != nullptr);
}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : comparator(options.comparator),
      splitter(options.splitter),
      write_buffer_size(options.write_buffer_size),
      max_write_buffer_number(options.max_write_buffer_number),
      min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      compression(options.compression),
      compression_per_level(options.compression_per_level),
      bottommost_compression(options.bottommost_compression),
      compression_opts(options.compression_opts),
      num_levels(options.num_levels),
      level0_file_num_compaction_trigger(
          options.level0_file_num_compaction_trigger),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      max_bytes_for_level_base(options.max_bytes_for_level_base),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      expanded_compaction_factor(options.expanded_compaction_factor),
      source_compaction_factor(options.source_compaction_factor),
      max_grandparent_overlap_factor(options.max_grandparent_overlap_factor),
      arena_block_size(options.arena_block_size),
      disable_auto_compactions(options.disable_auto_compactions),
      compaction_style(options.compaction_style),
      compaction_pri(options.compaction_pri),
      verify_checksums_in_compaction(options.verify_checksums_in_compaction),
      compaction_options_fifo(options.compaction_options_fifo),
      memtable_factory(options.memtable_factory),
      table_factory(options.table_factory),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      paranoid_file_checks(options.paranoid_file_checks),
      report_bg_io_stats(options.report_bg_io_stats) {
  assert(memtable_factory.get() != nullptr);
  if (max_bytes_for_level_multiplier_additional.size() <
      static_cast<unsigned int>(num_levels)) {
    max_bytes_for_level_multiplier_additional.resize(num_levels, 1);
  }
}

DBOptions::DBOptions()
    : create_if_missing(true),
      create_missing_column_families(false),
      error_if_exists(false),
      paranoid_checks(true),
      env(Env::Default()),
      sst_file_manager(nullptr),
      info_log(nullptr),
#ifdef NDEBUG
      info_log_level(INFO_LEVEL),
#else
      info_log_level(DEBUG_LEVEL),
#endif  // NDEBUG
      max_open_files(-1),
      max_file_opening_threads(16),
      max_total_wal_size(0),
      statistics(nullptr),
      disableDataSync(false),
      use_fsync(false),
      db_log_dir(""),
      wal_dir(""),
      delete_obsolete_files_period_micros(6ULL * 60 * 60 * 1000000),
      base_background_compactions(1),
      max_background_compactions(1),
      max_subcompactions(1),
      max_background_flushes(1),
      max_log_file_size(0),
      log_file_time_to_roll(0),
      keep_log_file_num(1000),
      recycle_log_file_num(0),
      max_manifest_file_size(std::numeric_limits<uint64_t>::max()),
      table_cache_numshardbits(6),
      WAL_ttl_seconds(0),
      WAL_size_limit_MB(0),
      manifest_preallocation_size(4 * 1024 * 1024),
      allow_os_buffer(true),
      allow_mmap_reads(false),
      allow_mmap_writes(false),
      allow_fallocate(true),
      is_fd_close_on_exec(true),
      stats_dump_period_sec(600),
      advise_random_on_open(true),
      db_write_buffer_size(0),
      access_hint_on_compaction_start(NORMAL),
      new_table_reader_for_compaction_inputs(false),
      compaction_readahead_size(0),
      random_access_max_buffer_size(1024 * 1024),
      writable_file_max_buffer_size(1024 * 1024),
      use_adaptive_mutex(false),
      bytes_per_sync(0),
      wal_bytes_per_sync(0),
      listeners(),
      enable_thread_tracking(false),
      delayed_write_rate(2 * 1024U * 1024U),
      allow_concurrent_memtable_write(false),
      write_thread_slow_yield_usec(3),
      skip_stats_update_on_db_open(false),
      wal_recovery_mode(WALRecoveryMode::kPointInTimeRecovery),
      fail_if_options_file_error(false),
      dump_malloc_stats(false) {
}

DBOptions::DBOptions(const Options& options)
    : create_if_missing(options.create_if_missing),
      create_missing_column_families(options.create_missing_column_families),
      error_if_exists(options.error_if_exists),
      paranoid_checks(options.paranoid_checks),
      env(options.env),
      sst_file_manager(options.sst_file_manager),
      info_log(options.info_log),
      info_log_level(options.info_log_level),
      max_open_files(options.max_open_files),
      max_file_opening_threads(options.max_file_opening_threads),
      max_total_wal_size(options.max_total_wal_size),
      statistics(options.statistics),
      disableDataSync(options.disableDataSync),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      max_subcompactions(options.max_subcompactions),
      max_background_flushes(options.max_background_flushes),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      WAL_ttl_seconds(options.WAL_ttl_seconds),
      WAL_size_limit_MB(options.WAL_size_limit_MB),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_os_buffer(options.allow_os_buffer),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      stats_dump_period_sec(options.stats_dump_period_sec),
      advise_random_on_open(options.advise_random_on_open),
      db_write_buffer_size(options.db_write_buffer_size),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(options.compaction_readahead_size),
      random_access_max_buffer_size(options.random_access_max_buffer_size),
      writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      use_adaptive_mutex(options.use_adaptive_mutex),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      listeners(options.listeners),
      enable_thread_tracking(options.enable_thread_tracking),
      delayed_write_rate(options.delayed_write_rate),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      write_thread_slow_yield_usec(options.write_thread_slow_yield_usec),
      skip_stats_update_on_db_open(options.skip_stats_update_on_db_open),
      wal_recovery_mode(options.wal_recovery_mode),
      fail_if_options_file_error(options.fail_if_options_file_error),
      dump_malloc_stats(options.dump_malloc_stats) {
}

static const char* const access_hints[] = {
  "NONE", "NORMAL", "SEQUENTIAL", "WILLNEED"
};

void DBOptions::Dump(Logger* log) const {
  Header(log, "[DBOptions]\n");
  Header(log, "\tOptions.error_if_exists: %d", error_if_exists);
  Header(log, "\tOptions.create_if_missing: %d", create_if_missing);
  Header(log, "\tOptions.paranoid_checks: %d", paranoid_checks);
  Header(log, "\tOptions.env: %p", env);
  Header(log, "\tOptions.info_log: %p", info_log.get());
  Header(log, "\tOptions.max_open_files: %d", max_open_files);
  Header(log, "\tOptions.max_file_opening_threads: %d",
         max_file_opening_threads);
  Header(log, "\tOptions.max_total_wal_size: %" PRIu64, max_total_wal_size);
  Header(log, "\tOptions.disableDataSync: %d", disableDataSync);
  Header(log, "\tOptions.use_fsync: %d", use_fsync);
  Header(log, "\tOptions.max_log_file_size: %" VIDARDB_PRIszt,
         max_log_file_size);
  Header(log, "\tOptions.max_manifest_file_size: %" PRIu64,
         max_manifest_file_size);
  Header(log, "\tOptions.log_file_time_to_roll: %" VIDARDB_PRIszt,
         log_file_time_to_roll);
  Header(log, "\tOptions.keep_log_file_num: %" VIDARDB_PRIszt,
         keep_log_file_num);
  Header(log, "\tOptions.recycle_log_file_num: %" VIDARDB_PRIszt,
         recycle_log_file_num);
  Header(log, "\tOptions.allow_os_buffer: %d", allow_os_buffer);
  Header(log, "\tOptions.allow_mmap_reads: %d", allow_mmap_reads);
  Header(log, "\tOptions.allow_fallocate: %d", allow_fallocate);
  Header(log, "\tOptions.allow_mmap_writes: %d", allow_mmap_writes);
  Header(log, "\tOptions.create_missing_column_families: %d",
         create_missing_column_families);
  Header(log, "\tOptions.db_log_dir: %s", db_log_dir.c_str());
  Header(log, "\tOptions.wal_dir: %s", wal_dir.c_str());
  Header(log, "\tOptions.table_cache_numshardbits: %d",
         table_cache_numshardbits);
  Header(log, "\tOptions.delete_obsolete_files_period_micros: %" PRIu64,
         delete_obsolete_files_period_micros);
  Header(log, "\tOptions.base_background_compactions: %d",
         base_background_compactions);
  Header(log, "\tOptions.max_background_compactions: %d",
         max_background_compactions);
  Header(log, "\tOptions.max_subcompactions: %" PRIu32, max_subcompactions);
  Header(log, "\tOptions.max_background_flushes: %d", max_background_flushes);
  Header(log, "\tOptions.WAL_ttl_seconds: %" PRIu64, WAL_ttl_seconds);
  Header(log, "\tOptions.WAL_size_limit_MB: %" PRIu64, WAL_size_limit_MB);
  Header(log, "\tOptions.manifest_preallocation_size: %" VIDARDB_PRIszt,
         manifest_preallocation_size);
  Header(log, "\tOptions.allow_os_buffer: %d", allow_os_buffer);
  Header(log, "\tOptions.allow_mmap_reads: %d", allow_mmap_reads);
  Header(log, "\tOptions.allow_mmap_writes: %d", allow_mmap_writes);
  Header(log, "\tOptions.is_fd_close_on_exec: %d", is_fd_close_on_exec);
  Header(log, "\tOptions.stats_dump_period_sec: %u", stats_dump_period_sec);
  Header(log, "\tOptions.advise_random_on_open: %d", advise_random_on_open);
  Header(log, "\tOptions.db_write_buffer_size: %" VIDARDB_PRIszt "d",
         db_write_buffer_size);
  Header(log, "\tOptions.access_hint_on_compaction_start: %s",
         access_hints[access_hint_on_compaction_start]);
  Header(log, "\tOptions.new_table_reader_for_compaction_inputs: %d",
         new_table_reader_for_compaction_inputs);
  Header(log, "\tOptions.compaction_readahead_size: %" VIDARDB_PRIszt "d",
         compaction_readahead_size);
  Header(log, "\tOptions.random_access_max_buffer_size: %" VIDARDB_PRIszt "d",
         random_access_max_buffer_size);
  Header(log, "\tOptions.writable_file_max_buffer_size: %" VIDARDB_PRIszt "d",
         writable_file_max_buffer_size);
  Header(log, "\tOptions.use_adaptive_mutex: %d", use_adaptive_mutex);
  Header(
      log, "\tOptions.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
      sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
  Header(log, "\tOptions.bytes_per_sync: %" PRIu64, bytes_per_sync);
  Header(log, "\tOptions.wal_bytes_per_sync: %" PRIu64, wal_bytes_per_sync);
  Header(log, "\tOptions.wal_recovery_mode: %d", wal_recovery_mode);
  Header(log, "\tOptions.enable_thread_tracking: %d", enable_thread_tracking);
  Header(log, "\tOptions.allow_concurrent_memtable_write: %d",
         allow_concurrent_memtable_write);
  Header(log, "\tOptions.write_thread_slow_yield_usec: %" PRIu64,
         write_thread_slow_yield_usec);
}  // DBOptions::Dump

void ColumnFamilyOptions::Dump(Logger* log) const {
  Header(log, "[ColumnFamilyOptions]\n");
  Header(log, "\tOptions.comparator: %s", comparator->Name());
  if (splitter) {
    Header(log, "\tOptions.splitter: %s", splitter->Name());
  }
  Header(log, "\tOptions.memtable_factory: %s", memtable_factory->Name());
  Header(log, "\tOptions.table_factory: %s", table_factory->Name());
  Header(log, "\ttable_factory options: %s",
         table_factory->GetPrintableTableOptions().c_str());
  Header(log, "\tOptions.write_buffer_size: %" VIDARDB_PRIszt,
         write_buffer_size);
  Header(log, "\tOptions.max_write_buffer_number: %d", max_write_buffer_number);
  if (!compression_per_level.empty()) {
    for (unsigned int i = 0; i < compression_per_level.size(); i++) {
      Header(log, "\tOptions.compression[%d]: %s", i,
             CompressionTypeToString(compression_per_level[i]).c_str());
    }
  } else {
    Header(log, "\tOptions.compression: %s",
           CompressionTypeToString(compression).c_str());
  }
  Header(log, "\tOptions.bottommost_compression: %s",
         bottommost_compression == kDisableCompressionOption
             ? "Disabled"
             : CompressionTypeToString(bottommost_compression).c_str());
  Header(log, "\tOptions.num_levels: %d", num_levels);
  Header(log, "\tOptions.min_write_buffer_number_to_merge: %d",
         min_write_buffer_number_to_merge);
  Header(log, "\tOptions.max_write_buffer_number_to_maintain: %d",
         max_write_buffer_number_to_maintain);
  Header(log, "\tOptions.compression_opts.window_bits: %d",
         compression_opts.window_bits);
  Header(log, "\tOptions.compression_opts.level: %d", compression_opts.level);
  Header(log, "\tOptions.compression_opts.strategy: %d",
         compression_opts.strategy);
  Header(log, "\tOptions.compression_opts.max_dict_bytes: %" VIDARDB_PRIszt,
         compression_opts.max_dict_bytes);
  Header(log, "\tOptions.level0_file_num_compaction_trigger: %d",
         level0_file_num_compaction_trigger);
  Header(log, "\tOptions.target_file_size_base: %" PRIu64,
         target_file_size_base);
  Header(log, "\tOptions.target_file_size_multiplier: %d",
         target_file_size_multiplier);
  Header(log, "\tOptions.max_bytes_for_level_base: %" PRIu64,
         max_bytes_for_level_base);
  Header(log, "\tOptions.max_bytes_for_level_multiplier: %d",
         max_bytes_for_level_multiplier);
  for (size_t i = 0; i < max_bytes_for_level_multiplier_additional.size();
       i++) {
    Header(log,
           "\tOptions.max_bytes_for_level_multiplier_addtl[%" VIDARDB_PRIszt
           "]: %d",
           i, max_bytes_for_level_multiplier_additional[i]);
  }
  Header(log, "\tOptions.expanded_compaction_factor: %d",
         expanded_compaction_factor);
  Header(log, "\tOptions.source_compaction_factor: %d",
         source_compaction_factor);
  Header(log, "\tOptions.max_grandparent_overlap_factor: %d",
         max_grandparent_overlap_factor);
  Header(log, "\tOptions.arena_block_size: %" VIDARDB_PRIszt, arena_block_size);
  Header(log, "\tOptions.disable_auto_compactions: %d",
         disable_auto_compactions);
  Header(log, "\tOptions.verify_checksums_in_compaction: %d",
         verify_checksums_in_compaction);
  Header(log, "\tOptions.compaction_style: %d", compaction_style);
  Header(log, "\tOptions.compaction_pri: %d", compaction_pri);
  Header(log,
         "\tOptions.compaction_options_fifo.max_table_files_size: %" PRIu64,
         compaction_options_fifo.max_table_files_size);
  std::string collector_names;
  for (const auto& collector_factory : table_properties_collector_factories) {
    collector_names.append(collector_factory->Name());
    collector_names.append("; ");
  }
  Header(log, "\tOptions.table_properties_collectors: %s",
         collector_names.c_str());
  Header(log, "\tOptions.paranoid_file_checks: %d", paranoid_file_checks);
  Header(log, "\tOptions.report_bg_io_stats: %d", report_bg_io_stats);
}  // ColumnFamilyOptions::Dump

void Options::Dump(Logger* log) const {
  DBOptions::Dump(log);
  ColumnFamilyOptions::Dump(log);
}   // Options::Dump

void Options::DumpCFOptions(Logger* log) const {
  ColumnFamilyOptions::Dump(log);
}  // Options::DumpCFOptions

//
// The goal of this method is to create a configuration that
// allows an application to write all files into L0 and
// then do a single compaction to output all files into L1.
Options*
Options::PrepareForBulkLoad()
{
  // never slowdown ingest.
  level0_file_num_compaction_trigger = (1<<30);

  // no auto compactions please. The application should issue a
  // manual compaction after all data is loaded into L0.
  disable_auto_compactions = true;
  disableDataSync = true;

  // A manual compaction run should pick all files in L0 in
  // a single compaction run.
  source_compaction_factor = (1<<30);

  // It is better to have only 2 levels, otherwise a manual
  // compaction would compact at every possible level, thereby
  // increasing the total time needed for compactions.
  num_levels = 2;

  // Need to allow more write buffers to allow more parallism
  // of flushes.
  max_write_buffer_number = 6;
  min_write_buffer_number_to_merge = 1;

  // When compaction is disabled, more parallel flush threads can
  // help with write throughput.
  max_background_flushes = 4;

  // Prevent a memtable flush to automatically promote files
  // to L1. This is helpful so that all files that are
  // input to the manual compaction are all at L0.
  max_background_compactions = 2;
  base_background_compactions = 2;

  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;

  // Might do some extra work after bulkloading to revert back
  memtable_factory.reset(new VectorRepFactory(100));

  return this;
}

Options* Options::OptimizeForSmallDb() {
  ColumnFamilyOptions::OptimizeForSmallDb();
  DBOptions::OptimizeForSmallDb();
  return this;
}

// Optimization functions
DBOptions* DBOptions::OptimizeForSmallDb() {
  max_file_opening_threads = 1;
  max_open_files = 5000;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForSmallDb() {
  write_buffer_size = 2 << 20;
  target_file_size_base = 2 * 1048576;
  max_bytes_for_level_base = 10 * 1048576;
  return this;
}

#ifndef VIDARDB_LITE
ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
    uint64_t block_cache_size_mb) {
  BlockBasedTableOptions block_based_options;
  block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
  table_factory.reset(new BlockBasedTableFactory(block_based_options));
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // start flushing L0->L1 as soon as possible. each file on level0 is
  // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
  // memtable_memory_budget.
  level0_file_num_compaction_trigger = 2;
  // doesn't really matter much, but we don't want to create too many files
  target_file_size_base = memtable_memory_budget / 8;
  // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
  max_bytes_for_level_base = memtable_memory_budget;

  // level style compaction
  compaction_style = kCompactionStyleLevel;

  // try to compress all levels with Snappy
  compression_per_level.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    compression_per_level[i] =
        Snappy_Supported() ? kSnappyCompression : kNoCompression;
  }
  return this;
}

/********************* Shichao ***************************/
Options* Options::OptimizeAdaptiveLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  ColumnFamilyOptions::OptimizeAdaptiveLevelStyleCompaction(
      memtable_memory_budget);
  // Delete sub sst files immediately instead of at closing db.
  delete_obsolete_files_period_micros = 0;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeAdaptiveLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  num_levels = 3;
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  min_write_buffer_number_to_merge = 2;
  max_write_buffer_number = 6;
  level0_file_num_compaction_trigger = 4;
  target_file_size_base = memtable_memory_budget;
  max_bytes_for_level_base = memtable_memory_budget;
  max_bytes_for_level_multiplier = 10;

  compaction_style = kCompactionStyleLevel;
  compaction_pri = kOldestLargestSeqFirst;

  compression_per_level.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    compression_per_level[i] =
        Snappy_Supported() ? kSnappyCompression : kNoCompression;
  }
  return this;
}
/********************* Shichao ***************************/

DBOptions* DBOptions::IncreaseParallelism(int total_threads) {
  max_background_compactions = total_threads - 1;
  max_background_flushes = 1;
  env->SetBackgroundThreads(total_threads, Env::LOW);
  env->SetBackgroundThreads(1, Env::HIGH);
  return this;
}

#endif  // !VIDARDB_LITE

ReadOptions::ReadOptions()
    : verify_checksums(true),
      fill_cache(true),
      snapshot(nullptr),
      read_tier(kReadAllTier),
      tailing(false),
      total_order_seek(false),
      pin_data(false),
      readahead_size(0) {}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : verify_checksums(cksum),
      fill_cache(cache),
      snapshot(nullptr),
      read_tier(kReadAllTier),
      tailing(false),
      total_order_seek(false),
      pin_data(false),
      readahead_size(0) {}
}  // namespace vidardb
