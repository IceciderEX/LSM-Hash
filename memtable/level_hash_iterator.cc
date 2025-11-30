// memtable/level_hash_iterator.cc
#include "memtable/level_hash_iterator.h"
#include "memtable/level_hash_memtable.h" //

namespace ROCKSDB_NAMESPACE {

LevelHashIterator::LevelHashIterator(const LevelHashMemTable* table)
    : table_(table), current_bucket_(0), current_entry_in_bucket_(0) {
    // 初始化时寻找第一个有效数据
    if (table_ != nullptr) {
        FindNextEntry();
    }
}

bool LevelHashIterator::Valid() const {
  // 1. 检查 Bucket 索引越界
  if (current_bucket_ >= table_->GetBuckets().size()) {
    return false;
  }
  
  // 2. 获取 Bucket 指针 (unique_ptr)
  const auto& bucket_ptr = table_->GetBuckets()[current_bucket_];
  
  // 3. 检查 Bucket 是否为空指针，以及 Entry 索引是否越界
  // 注意：buckets_[i] 是 unique_ptr，需要 -> 访问 entries_
  return bucket_ptr && 
         current_entry_in_bucket_ < bucket_ptr->entries_.size();
}

const char* LevelHashIterator::key() const {
  assert(Valid());
  // 获取数据：table -> bucket_ptr -> entries_ -> vector[index]
  return static_cast<const char*>(
      table_->GetBuckets()[current_bucket_]->entries_[current_entry_in_bucket_]);
}

void LevelHashIterator::Next() {
  assert(Valid());
  
  // 移动到当前 Bucket 的下一个 Entry
  current_entry_in_bucket_++;
  
  const auto& bucket_ptr = table_->GetBuckets()[current_bucket_];
  
  // 如果当前 Bucket 遍历完了，找下一个非空 Bucket
  if (current_entry_in_bucket_ >= bucket_ptr->entries_.size()) {
    current_bucket_++;
    current_entry_in_bucket_ = 0;
    FindNextEntry();
  }
}

void LevelHashIterator::FindNextEntry() {
  const auto& buckets = table_->GetBuckets();
  
  // 循环直到找到一个非空的 Bucket 或者遍历完所有 Bucket
  while (current_bucket_ < buckets.size()) {
    const auto& bucket_ptr = buckets[current_bucket_];
    
    // 检查 Bucket 指针有效且 entries 不为空
    if (bucket_ptr && !bucket_ptr->entries_.empty()) {
      return; // 找到了有效位置，current_entry_in_bucket_ 此时为 0
    }
    
    // 当前 Bucket 为空，跳到下一个
    current_bucket_++;
  }
  // 循环结束说明到底了，Valid() 将返回 false
}

// --- 以下不支持的操作保持 Assert(false) ---

void LevelHashIterator::Prev() {
  assert(false);
}

void LevelHashIterator::Seek(const Slice& /*internal_key*/,
                             const char* /*memtable_key*/) {
  assert(false);
}

void LevelHashIterator::SeekForPrev(const Slice& /*internal_key*/,
                                    const char* /*memtable_key*/) {
  assert(false);
}

void LevelHashIterator::SeekToFirst() {
  current_bucket_ = 0;
  current_entry_in_bucket_ = 0;
  FindNextEntry();
}

void LevelHashIterator::SeekToLast() {
  assert(false);
}

}  // namespace ROCKSDB_NAMESPACE