//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_picker_level_hash.h"

#include <map>
#include <algorithm>
#include <cinttypes>
#include <string>
#include <vector>
#include <cmath>

#include "db/compaction/compaction.h"
#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// util function：计算某一层级的 Bucket 总数
// L0 的 G 为 initial_g，每降一层 G+1
static uint32_t GetNumBucketsForLevel(int level, uint32_t initial_g) {
  uint32_t g = initial_g + level;
  if (g > 31) g = 31; 
  return 1u << g;
}

bool LevelHashCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // V1：只要有任意一层的文件数不为0，就可能有 Compaction
  // TODO：优化此检查逻辑，减少计算
  for (int i = 0; i < vstorage->num_levels() - 1; ++i) {
      if (vstorage->NumLevelFiles(i) > 0) {
          return true;
      }
  }
  return false;
}

Compaction* LevelHashCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots*/,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool /*require_max_output_level*/) {
  
  // TODO: 从配置中（某个 option）获取 Initial G
  const uint32_t kInitialG = mutable_cf_options.level_hash_initial_g;  // hard code
  // 目前策略：优先处理上层 (L0 > L1 > ...)
  for (int level = 0; level < vstorage->num_levels() - 1; ++level) {
      int input_level = level;
      int output_level = level + 1;
      uint32_t num_buckets = GetNumBucketsForLevel(input_level, kInitialG);

      // 触发阈值：
      // L0 使用 level0_file_num_compaction_trigger
      // TODO: L1+ 层的判断条件
      size_t trigger_threshold = mutable_cf_options.level0_file_num_compaction_trigger;
      if (input_level > 0) {
          trigger_threshold = std::max(trigger_threshold, size_t(2)); 
      }

      const auto& level_files = vstorage->LevelFiles(input_level);
      if (level_files.empty()) {
          continue;
      }

      // 2. 当前层级每个 bucket 有多少个有效的 sst
      std::map<uint32_t, std::vector<FileMetaData*>> bucket_to_files;
      for (FileMetaData* f : level_files) {
        // if (f->being_compacted) {
        //   continue;
        // }
        // Bitmap
        for (size_t i = 0; i < f->valid_bucket_bitmap.size(); ++i) {
          uint64_t word = f->valid_bucket_bitmap[i];
          if (word == 0) continue;
          // 检查当前 sst 包含的 bucket 
          for (int bit = 0; bit < 64; ++bit) {
            if ((word & (1ULL << bit)) != 0) {
              uint32_t bucket_id = static_cast<uint32_t>(i * 64 + bit);
              // range check
              if (bucket_id < num_buckets) {
                  bucket_to_files[bucket_id].push_back(f);
              }
            }
          }
        }
      }

      // 3. Round-Robin -> Bucket Cursor
      // 从上次 compaction 的 bucket 开始
      int start_bucket_idx = vstorage->NextCompactionIndex(input_level);
      if (start_bucket_idx >= static_cast<int>(num_buckets)) {
          start_bucket_idx = 0;
      }

      uint32_t target_bucket = Compaction::kInvalidBucketId;
      size_t max_files_found = 0;
      for (size_t i = 0; i < num_buckets; ++i) {
          uint32_t curr_bucket = (start_bucket_idx + i) % num_buckets;
          
          auto it = bucket_to_files.find(curr_bucket);
          if (it != bucket_to_files.end()) {
              size_t file_count = it->second.size();
              // 是否有其他文件正在 compaction 此 bucket
              std::vector<FileMetaData*>& candidates = it->second;
              bool bucket_busy = false;
              for (FileMetaData* f : candidates) {
                  if (f->IsBucketLocked(curr_bucket)) { 
                      bucket_busy = true;
                      break; 
                  }
              }

              if (bucket_busy) {
                  continue; 
              }

              // 目前的逻辑：bucket
              if (file_count >= trigger_threshold) {
                  target_bucket = curr_bucket;
                  max_files_found = file_count;
                  // 更新下一次 compaction 的 start index
                  vstorage->SetNextCompactionIndex(input_level, (curr_bucket + 1) % num_buckets);
                  break; // Found one
              }
          }
      }

      // 如果当前层级找到了任务，生成 Compaction 并返回
      if (target_bucket != Compaction::kInvalidBucketId) {
          ROCKS_LOG_BUFFER(log_buffer, 
              "[%s] LevelHash: Picked Level %d Bucket %u with %zu files -> Level %d", 
              cf_name.c_str(), input_level, target_bucket, max_files_found, output_level);

          CompactionInputFiles inputs;
          inputs.level = input_level;
          inputs.files = bucket_to_files[target_bucket]; 
          std::sort(inputs.files.begin(), inputs.files.end(), 
              [](FileMetaData* a, FileMetaData* b) {
                  return a->fd.GetNumber() < b->fd.GetNumber();
              });
          CompressionType compression_type = GetCompressionType(
              vstorage, mutable_cf_options, output_level, 1);
          CompressionOptions compression_opts = GetCompressionOptions(
              mutable_cf_options, vstorage, output_level);
            
          for (FileMetaData* f : inputs.files) {
              f->SetBucketLock(target_bucket, true);
          }

          auto c = new Compaction(
              vstorage, 
              ioptions_, 
              mutable_cf_options, 
              mutable_db_options, 
              {inputs}, // inputs
              output_level, 
              mutable_cf_options.target_file_size_base, 
              mutable_cf_options.max_compaction_bytes, 
              0, // output path id
              compression_type, 
              compression_opts,
              Temperature::kUnknown,
              0, // max_subcompactions
              {}, // grandparents
              std::nullopt, // earliest_snapshot
              snapshot_checker,
              CompactionReason::kLevelL0FilesNum, // Reason (暂时先用这个)
              "", // trim_ts
              -1, // score
              true // l0_files_might_overlap
          );


          c->SetTargetBucketId(target_bucket);
          RegisterCompaction(c);
          return c; 
      }
      // level++
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE