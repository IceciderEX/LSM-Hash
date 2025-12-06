#include "db/compaction/compaction_picker_level_hash.h"
#include "db/version_edit.h"
#include <map>

namespace ROCKSDB_NAMESPACE {

bool LevelHashCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // 先简单返回 true，依靠 PickCompaction 内部判断是否有任务
  return vstorage->NumLevelFiles(0) > 0;
}

Compaction* LevelHashCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots*/,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* /*log_buffer*/, bool /*require_max_output_level*/) {

  // TODO: 完善 compaction 的逻辑
  std::map<uint32_t, std::vector<FileMetaData*>> bucket_to_files;
  
  const auto& l0_files = vstorage->LevelFiles(0);
  for (FileMetaData* f : l0_files) {
    if (f->being_compacted) continue;
    
    // 遍历位图，找到该文件包含的所有 Bucket
    for (size_t i = 0; i < f->valid_bucket_bitmap.size(); ++i) {
      uint64_t word = f->valid_bucket_bitmap[i];
      if (word == 0) continue;
      
      for (int bit = 0; bit < 64; ++bit) {
        if ((word & (1ULL << bit)) != 0) {
          uint32_t bucket_id = static_cast<uint32_t>(i * 64 + bit);
          bucket_to_files[bucket_id].push_back(f);
        }
      }
    }
  }

  // 2. 找到最“热”的 Bucket (包含文件数最多)
  uint32_t target_bucket = 0;
  size_t max_files = 0;
  
  // 阈值：来自配置 (这里硬编码示例，实际应从 mutable_cf_options 获取)
  size_t trigger_threshold = 4; // compaction_trigger_file_count

  for (const auto& entry : bucket_to_files) {
    if (entry.second.size() > max_files) {
      max_files = entry.second.size();
      target_bucket = entry.first;
    }
  }

  if (max_files < trigger_threshold) {
    return nullptr; // 没有达到触发条件
  }

  // 3. 构建 CompactionInputFiles
  CompactionInputFiles inputs;
  inputs.level = 0;
  inputs.files = bucket_to_files[target_bucket]; // 包含该 Bucket 的所有 L0 文件

  // 4. 设置 Output Level (L0 -> L1)
  int output_level = 1; 
  // 先实现 L0->L1
  // 5. 创建 Compaction 对象
  auto c = new Compaction(
      vstorage, 
      ioptions_, 
      mutable_cf_options, 
      mutable_db_options, 
      {inputs}, // inputs
      output_level, 
      mutable_cf_options.max_compaction_bytes, // target file size
      LLONG_MAX, // max compaction bytes
      0, // path id
      kNoCompression, // compression
      {}, // compression opts
      Temperature::kUnknown,
      0, // max_subcompactions
      {}, // grandparents (Level-Hash 不需要检查重叠)
      std::nullopt, nullptr,
      CompactionReason::kLevelL0FilesNum // Reason
  );
  
  c->SetTargetBucketId(target_bucket);
  return c;
}

}  // namespace ROCKSDB_NAMESPACE