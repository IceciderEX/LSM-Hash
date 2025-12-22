#include "memtable/level_hash_memtable.h"
#include "level_hash_iterator.h" 

#include "db/dbformat.h"
#include "util/coding.h"
#include "util/murmurhash.h"
#include "db/dbformat.h" 
#include "memory/arena.h"
#include "db/lookup_key.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

LevelHashMemTable::LevelHashMemTable(
    const MemTableRep::KeyComparator& comparator, Allocator* allocator,
    uint32_t G, size_t bucket_entry_threshold, size_t total_memory_threshold)
    : MemTableRep(allocator),
      G_(G),
      num_buckets_(1 << G),
      bucket_entry_threshold_(bucket_entry_threshold),
      total_memory_threshold_(total_memory_threshold),
      current_memory_usage_(0),
      flush_requested_(false), 
      allocator_(allocator),
      comparator_(comparator) {
  
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

// [Varint KeyLen] [Key Bytes] [Varint ValLen] [Val Bytes]
static size_t CalculateEntrySize(const char* ptr) {
    const char* start = ptr;
    // Varint32: 5 byte
    uint32_t key_len = 0;
    const char* p = GetVarint32Ptr(ptr, ptr + 5, &key_len); 
    if (p == nullptr) return 0; 
    p += key_len; 

    uint32_t val_len = 0;
    p = GetVarint32Ptr(p, p + 5, &val_len);
    if (p == nullptr) return 0;
    p += val_len;  

    return p - start;
}

void LevelHashMemTable::Insert(KeyHandle handle) {
  // KeyHandle：一个指向内存中 entry 的指针
  const char* key_ptr = static_cast<const char*>(handle);
  size_t entry_size = CalculateEntrySize(key_ptr);
  // TODO: memory_usage 的计算方式？原生 rocksdb 使用 arena 分配内存，这里先简化
  // TODO: arena 的支持
  current_memory_usage_.fetch_add(entry_size + sizeof(KeyHandle), std::memory_order_relaxed);

  Slice internal_key = GetLengthPrefixedSlice(key_ptr);
  Slice user_key = ExtractUserKey(internal_key);
  // eg. user_key = "key1", hash = 9539024932675925583
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  // eg. 9539024932675925583 -> bucket 7, G = 3
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  std::string user_key_str = user_key.ToString();
  if (user_key_str == "key_1500") {
    int col = 1;
  }

  // 加锁写入（暂定）
  Bucket* bucket = buckets_[bucket_idx].get();
  {
    std::unique_lock<std::shared_mutex> lock(bucket->mutex_);
    bucket->entries_.push_back(handle);

    // 检查单 Bucket 阈值，触发 Flush
    if (bucket->entries_.size() >= bucket_entry_threshold_) {
        flush_requested_.store(true, std::memory_order_relaxed);
    }
  }
  
  num_entries_.fetch_add(1, std::memory_order_relaxed);
}

bool LevelHashMemTable::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);
  Slice user_key = ExtractUserKey(internal_key);
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  const Bucket* bucket = buckets_[bucket_idx].get();
  // 加锁读取 (防止读取时发生 vector 扩容)
  std::shared_lock<std::shared_mutex> lock(bucket->mutex_);

  const MemTable::KeyComparator& impl_comparator = 
      static_cast<const MemTable::KeyComparator&>(comparator_);
  const Comparator* user_comparator = impl_comparator.comparator.user_comparator();

  for (auto it = bucket->entries_.rbegin(); it != bucket->entries_.rend(); ++it) {
    const char* entry_ptr = static_cast<const char*>(*it);
    
    // User Key
    uint32_t key_len;
    const char* k_ptr = GetVarint32Ptr(entry_ptr, entry_ptr + 5, &key_len);
    if (k_ptr == nullptr) continue;
    
    Slice current_internal_key(k_ptr, key_len);
    Slice current_user_key = ExtractUserKey(current_internal_key);
    if (user_comparator->Compare(current_user_key, user_key) == 0) {
      return true;
    }
  }
  return false;
}

void LevelHashMemTable::InsertConcurrently(KeyHandle handle) {
  Insert(handle);
}

void LevelHashMemTable::Get(const LookupKey& k, void* callback_args,
                            bool (*callback_func)(void* arg, const char* entry)) {
  Slice user_key = k.user_key();
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  const Bucket* bucket = buckets_[bucket_idx].get();
  
  // 加锁读取
  std::shared_lock<std::shared_mutex> lock(bucket->mutex_);

  const MemTable::KeyComparator& impl_comparator = 
      static_cast<const MemTable::KeyComparator&>(comparator_);
  const Comparator* user_comparator = impl_comparator.comparator.user_comparator();

  for (auto it = bucket->entries_.rbegin(); it != bucket->entries_.rend(); ++it) {
    // entry_ptr：KeyHandle 
    const char* entry_ptr = static_cast<const char*>(*it);
    // [KeyLen][KeyBytes][ValLen][ValBytes]
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry_ptr, entry_ptr + 5, &key_length);
    if (key_ptr == nullptr) continue; 
    
    Slice internal_key(key_ptr, key_length);
    Slice current_user_key = ExtractUserKey(internal_key);
    if (user_comparator->Compare(current_user_key, user_key) != 0) {
        continue;
    }
    auto s = callback_func(callback_args, entry_ptr);
    if (!s) {
      break;
    }
  }
}

size_t LevelHashMemTable::ApproximateMemoryUsage() {  
  return current_memory_usage_.load(std::memory_order_relaxed);
}

MemTableRep::Iterator* LevelHashMemTable::GetIterator(Arena* arena) {
  // 通常由 FlushJob 在 MemTable 变为 Immutable 后调用，
  // 此时不再有写入，因此不需要加锁。但如果在并发写入时迭代，该实现不安全
  if (arena == nullptr) {
    return new LevelHashIterator(this);
  } else {
    void* buf = arena->AllocateAligned(sizeof(LevelHashIterator));
    return new (buf) LevelHashIterator(this);
  }
}

}  // namespace ROCKSDB_NAMESPACE