// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
#include "memtable/level_hash_memtable.h"
#include "level_hash_iterator.h" 

#include "db/dbformat.h"
#include "util/coding.h"
#include "util/murmurhash.h"
#include "db/dbformat.h" 
#include "memory/arena.h"
#include "db/lookup_key.h"

namespace ROCKSDB_NAMESPACE {

LevelHashMemTable::LevelHashMemTable(
    const MemTableRep::KeyComparator& comparator, Allocator* allocator,
    uint32_t G, size_t total_entry_threshold)
    : MemTableRep(allocator),
      G_(G),
      num_buckets_(1 << G),
      total_entry_threshold_(total_entry_threshold),
      num_entries_(0),
      allocator_(allocator),
      comparator_(comparator) {
  
  // 初始化 Buckets
  buckets_.reserve(num_buckets_);
  for (uint32_t i = 0; i < num_buckets_; ++i) {
    buckets_.emplace_back(new Bucket());
  }
}

LevelHashMemTable::~LevelHashMemTable() {}

// util：翻转所有 64 bit
inline uint64_t ReverseBits64(uint64_t x) {
  x = ((x & 0x5555555555555555ULL) << 1) | ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
  x = ((x & 0x3333333333333333ULL) << 2) | ((x & 0xCCCCCCCCCCCCCCCCULL) >> 2);
  x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x & 0xF0F0F0F0F0F0F0F0ULL) >> 4);
  x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x & 0xFF00FF00FF00FF00ULL) >> 8);
  x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x & 0xFFFF0000FFFF0000ULL) >> 16);
  return (x << 32) | (x >> 32);
}

// util：提取出高 G 位作为桶索引
inline uint32_t GetBucketIndex(uint64_t hash, uint32_t G) {
  if (G == 0) return 0;
  // 1. 翻转所有位
  uint64_t reversed = ReverseBits64(hash);
  // 2. 将翻转后的高 G 位移到低位作为索引
  return static_cast<uint32_t>(reversed >> (64 - G));
}

void LevelHashMemTable::Insert(KeyHandle handle) {
  const char* key_ptr = static_cast<const char*>(handle);
  Slice internal_key = GetLengthPrefixedSlice(key_ptr);
  Slice user_key = ExtractUserKey(internal_key);
  // eg. user_key = "key1", hash = 9539024932675925583
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  // eg. 9539024932675925583 -> bucket 7
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  // 加锁写入（暂定）
  Bucket* bucket = buckets_[bucket_idx].get();
  {
    std::lock_guard<std::mutex> lock(bucket->mutex_);
    bucket->entries_.push_back(handle);
  }
  
  num_entries_.fetch_add(1, std::memory_order_relaxed);
}

bool LevelHashMemTable::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);
  Slice user_key = ExtractUserKey(internal_key);
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  const Bucket* bucket = buckets_[bucket_idx].get();
  // 加锁读取 (防止读取时发生 vector 扩容导致迭代器失效)
  std::lock_guard<std::mutex> lock(bucket->mutex_);
  
  for (auto it = bucket->entries_.rbegin(); it != bucket->entries_.rend(); ++it) {
    Slice current_internal_key = GetLengthPrefixedSlice(static_cast<const char*>(*it));
    if (comparator_(key, current_internal_key.data()) == 0) {
      return true;
    }
  }
  return false;
}

void LevelHashMemTable::Get(const LookupKey& k, void* callback_args,
                            bool (*callback_func)(void* arg, const char* entry)) {
  Slice user_key = k.user_key();
  uint32_t hash = MurmurHash(user_key.data(), static_cast<int>(user_key.size()), 0);
  uint32_t bucket_idx = hash & (num_buckets_ - 1);

  const Bucket* bucket = buckets_[bucket_idx].get();
  
  // 加锁读取
  std::lock_guard<std::mutex> lock(bucket->mutex_);

  for (auto it = bucket->entries_.rbegin(); it != bucket->entries_.rend(); ++it) {
    const char* key_ptr = static_cast<const char*>(*it);
    if (!callback_func(callback_args, key_ptr)) {
      break;
    }
  }
}

size_t LevelHashMemTable::ApproximateMemoryUsage() {
  return num_entries_.load(std::memory_order_relaxed) * 128; // 简易估算
}

bool LevelHashMemTable::IsFull() const {
  return num_entries_.load(std::memory_order_relaxed) >= total_entry_threshold_;
}

MemTableRep::Iterator* LevelHashMemTable::GetIterator(Arena* arena) {
  // 通常由 FlushJob 在 MemTable 变为 Immutable 后调用，
  // 此时不再有写入，因此不需要加锁。但如果在并发写入时迭代，该实现是不安全的。
  if (arena == nullptr) {
    return new LevelHashIterator(this);
  } else {
    void* buf = arena->AllocateAligned(sizeof(LevelHashIterator));
    return new (buf) LevelHashIterator(this);
  }
}

}  // namespace ROCKSDB_NAMESPACE