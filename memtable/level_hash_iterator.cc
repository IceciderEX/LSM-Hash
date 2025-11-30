// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
//
// This file implements the iterator for LevelHashMemTable.
#include "level_hash_iterator.h"

namespace ROCKSDB_NAMESPACE {

LevelHashIterator::LevelHashIterator(const LevelHashMemTable* table)
    : table_(table), current_bucket_(0), current_entry_in_bucket_(0) {}

bool LevelHashIterator::Valid() const {
  return current_bucket_ < table_->buckets_.size() &&
         current_entry_in_bucket_ < table_->buckets_[current_bucket_].size();
}

const char* LevelHashIterator::key() const {
  assert(Valid());
  return static_cast<const char*>(
      table_->buckets_[current_bucket_][current_entry_in_bucket_]);
}

void LevelHashIterator::Next() {
  assert(Valid());
  current_entry_in_bucket_++;
  if (current_entry_in_bucket_ >= table_->buckets_[current_bucket_].size()) {
    current_bucket_++;
    current_entry_in_bucket_ = 0;
    FindNextEntry();
  }
}

void LevelHashIterator::FindNextEntry() {
  while (current_bucket_ < table_->buckets_.size() &&
         table_->buckets_[current_bucket_].empty()) {
    current_bucket_++;
  }
  current_entry_in_bucket_ = 0;
}

void LevelHashIterator::Prev() {
  // 哈希表是无序的，反向遍历的语义不明确，这里不支持。
  // 在 Flush 场景下，只需要正向遍历。
  assert(false);
}

void LevelHashIterator::Seek(const Slice& /*internal_key*/,
                             const char* /*memtable_key*/) {
  // 哈希表不支持高效的 Seek，这里不支持。
  // 在 Flush 场景下，只需要从头遍历。
  assert(false);
}

void LevelHashIterator::SeekForPrev(const Slice& /*internal_key*/,
                                    const char* /*memtable_key*/) {
  // 哈希表不支持高效的 Seek，这里不支持。
  // 在 Flush 场景下，只需要从头遍历。
  assert(false);
}

void LevelHashIterator::SeekToFirst() {
  current_bucket_ = 0;
  current_entry_in_bucket_ = 0;
  FindNextEntry();
}

void LevelHashIterator::SeekToLast() {
  // 哈希表是无序的，SeekToLast 语义不明确，这里不支持。
  assert(false);
}

}  // namespace ROCKSDB_NAMESPACE
