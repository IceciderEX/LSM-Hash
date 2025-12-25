// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <mutex> // 引入 mutex
#include <memory>

#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "db/memtable.h"

namespace ROCKSDB_NAMESPACE {

class LevelHashMemTable : public MemTableRep {
 public:
  // 定义 Bucket 结构
  struct Bucket {
    mutable std::shared_mutex mutex_;
    std::vector<KeyHandle> entries_;
    std::atomic<size_t> memory_usage_{0};
  };

  explicit LevelHashMemTable(const MemTableRep::KeyComparator& comparator,
                             Allocator* allocator, uint32_t G,
                             size_t bucket_entries_threshold, // (2) Bucket threshold
                             size_t memory_usage_threshold, // (3) Total memory threshold
                             size_t bucket_memory_limit);  

  ~LevelHashMemTable() override;

  void Insert(KeyHandle handle) override;

  bool Contains(const char* internal_key) const override;

  void Get(const LookupKey& k, void* callback_args, bool (*callback_func)(void* arg, const char* entry)) override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  size_t ApproximateMemoryUsage() override;

  bool IsFull() const;

  bool NeedFlush() const { return flush_requested_.load(std::memory_order_relaxed); }

  void InsertConcurrently(KeyHandle handle) override;

  uint32_t GetG() const { return G_; }

  const std::vector<std::unique_ptr<Bucket>>& GetBuckets() const {
    return buckets_;
  }

 private:
  friend class LevelHashIterator;

  const uint32_t G_;
  const uint32_t num_buckets_;

  // Thresholds
  const size_t bucket_entry_threshold_;
  const size_t total_memory_threshold_;
  const size_t bucket_memory_threshold_;

  std::vector<std::unique_ptr<Bucket>> buckets_;

  std::atomic<size_t> num_entries_;

  // Track total memory usage 
  std::atomic<size_t> current_memory_usage_;
  // Flag to signal RocksDB 
  std::atomic<bool> flush_requested_;
  Allocator* allocator_;
  const MemTableRep::KeyComparator& comparator_;
};

class LevelHashMemTableFactory : public MemTableRepFactory {
 public:
  explicit LevelHashMemTableFactory(uint32_t initial_g = 3,
                                    size_t bucket_entries_threshold = 1000,
                                    size_t bucket_memory_threshold = 32 * 1024 * 1024,
                                    size_t memory_usage_threshold = 256 * 1024 * 1024)
      : initial_g_(initial_g),
        bucket_entries_threshold_(bucket_entries_threshold),
        bucket_memory_threshold_(bucket_memory_threshold),
        memory_usage_threshold_(memory_usage_threshold) {}
  
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& comparator,
                                 Allocator* allocator,
                                 const SliceTransform* /*prefix_extractor*/,
                                 Logger* /*logger*/, uint32_t /* column_family_id*/) override {
    size_t bucket_memory_limit = bucket_memory_threshold_;
    if (bucket_memory_threshold_ == 0) {
      uint32_t num_buckets = 1 << initial_g_;
      bucket_memory_limit = memory_usage_threshold_ / num_buckets;
    }
    return new LevelHashMemTable(comparator, allocator, initial_g_,
                                 bucket_entries_threshold_, memory_usage_threshold_, bucket_memory_limit);
  }

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& comparator,
                                 Allocator* allocator,
                                 const SliceTransform* /*prefix_extractor*/,
                                 Logger* /*logger*/) override {
    size_t bucket_memory_limit = bucket_memory_threshold_;
    if (bucket_memory_threshold_ == 0) {
      uint32_t num_buckets = 1 << initial_g_;
      bucket_memory_limit = memory_usage_threshold_ / num_buckets;
    }
    return new LevelHashMemTable(comparator, allocator, initial_g_,
                                 bucket_entries_threshold_, memory_usage_threshold_, bucket_memory_limit);
  }

  const char* Name() const override { return "LevelHashMemTableFactory"; }

  bool IsInsertConcurrentlySupported() const override { return true; }
  
  uint32_t GetInitialG() const { return initial_g_; }

 private:
  const uint32_t initial_g_;
  const size_t bucket_entries_threshold_; // (1) Bucket entry threshold
  const size_t bucket_memory_threshold_; // (2) Bucket memory threshold
  const size_t memory_usage_threshold_; // (3) Total memory threshold
};

}  // namespace ROCKSDB_NAMESPACE