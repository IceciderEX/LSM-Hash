// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <mutex> // 引入 mutex
#include <memory>

#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class LevelHashMemTable : public MemTableRep {
 public:
  // 定义 Bucket 结构，包含互斥锁和数据
  struct Bucket {
    mutable std::mutex mutex_;
    std::vector<KeyHandle> entries_;
  };

  explicit LevelHashMemTable(const MemTableRep::KeyComparator& comparator,
                             Allocator* allocator, uint32_t G,
                             size_t total_entry_threshold);

  ~LevelHashMemTable() override;

  void Insert(KeyHandle handle) override;

  bool Contains(const char* internal_key) const override;

  void Get(const LookupKey& k, void* callback_args, bool (*callback_func)(void* arg, const char* entry)) override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  size_t ApproximateMemoryUsage() override;

  bool IsFull() const;

  uint32_t GetG() const { return G_; }

  // 返回 Bucket 指针数组，供 Iterator 使用 (注意：Iterator 访问需自行处理并发或仅在 Flush 时使用)
  const std::vector<std::unique_ptr<Bucket>>& GetBuckets() const {
    return buckets_;
  }

 private:
  friend class LevelHashIterator;

  const uint32_t G_;
  const uint32_t num_buckets_;
  const size_t total_entry_threshold_;

  // 修改为 unique_ptr 数组，避免 mutex 拷贝问题
  std::vector<std::unique_ptr<Bucket>> buckets_;

  std::atomic<size_t> num_entries_;
  Allocator* allocator_;
  const MemTableRep::KeyComparator& comparator_;
};

class LevelHashMemTableFactory : public MemTableRepFactory {
 public:
  explicit LevelHashMemTableFactory(uint32_t initial_g = 3,
                                    size_t write_buffer_size_in_entries = 100000)
      : initial_g_(initial_g),
        write_buffer_size_in_entries_(write_buffer_size_in_entries) {}
  
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& comparator,
                                 Allocator* allocator,
                                 const SliceTransform* /*prefix_extractor*/,
                                 Logger* /*logger*/, uint32_t /* column_family_id*/) override {
    return new LevelHashMemTable(comparator, allocator, initial_g_,
                                 write_buffer_size_in_entries_);
  }

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& comparator,
                                 Allocator* allocator,
                                 const SliceTransform* /*prefix_extractor*/,
                                 Logger* /*logger*/) override {
    return new LevelHashMemTable(comparator, allocator, initial_g_,
                                 write_buffer_size_in_entries_);
  }

  const char* Name() const override { return "LevelHashMemTableFactory"; }

  bool IsInsertConcurrentlySupported() const override { return true; }
  
  uint32_t GetInitialG() const { return initial_g_; }

 private:
  const uint32_t initial_g_;
  const size_t write_buffer_size_in_entries_;
};

}  // namespace ROCKSDB_NAMESPACE