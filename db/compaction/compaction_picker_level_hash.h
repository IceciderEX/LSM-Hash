#pragma once
#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class LevelHashCompactionPicker : public CompactionPicker {
 public:
  LevelHashCompactionPicker(const ImmutableOptions& ioptions,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  Compaction* PickCompaction(const std::string& cf_name,
                             const MutableCFOptions& mutable_cf_options,
                             const MutableDBOptions& mutable_db_options,
                             const std::vector<SequenceNumber>& existing_snapshots,
                             const SnapshotChecker* snapshot_checker,
                             VersionStorageInfo* vstorage,
                             LogBuffer* log_buffer,
                             bool require_max_output_level = false) override;

  const char* Name() const override { return "LevelHashCompactionPicker"; }
  
  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;
};

}  // namespace ROCKSDB_NAMESPACE