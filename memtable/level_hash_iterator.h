// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
//
// This file defines the iterator for LevelHashMemTable.

#pragma once

#include "rocksdb/memtablerep.h"
#include "level_hash_memtable.h"

namespace ROCKSDB_NAMESPACE {

class LevelHashIterator : public MemTableRep::Iterator {
 public:
  explicit LevelHashIterator(const LevelHashMemTable* table);

  ~LevelHashIterator() override = default;

  bool Valid() const override;

  const char* key() const override;

  void Next() override;

  void Prev() override;

  void Seek(const Slice& internal_key, const char* memtable_key) override;

  void SeekForPrev(const Slice& internal_key, const char* memtable_key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

 private:
  const LevelHashMemTable* table_;
  size_t current_bucket_;
  size_t current_entry_in_bucket_;

  void FindNextEntry();
};

}  // namespace ROCKSDB_NAMESPACE
