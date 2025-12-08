// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
#include "table/level_hash/level_hash_table.h"

#include "db/dbformat.h"
#include "db/table_properties_collector.h" 
#include "util/coding.h"
#include "util/murmurhash.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

static constexpr uint64_t kLevelHashMagicNumber = 0x6C6254687361484Cull;
static constexpr size_t kFooterSize = 20; // 8 (IndexOffset) + 4 (G) + 8 (Magic)

inline uint64_t ReverseBits64(uint64_t x) {
  x = ((x & 0x5555555555555555ULL) << 1) | ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
  x = ((x & 0x3333333333333333ULL) << 2) | ((x & 0xCCCCCCCCCCCCCCCCULL) >> 2);
  x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x & 0xF0F0F0F0F0F0F0F0ULL) >> 4);
  x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x & 0xFF00FF00FF00FF00ULL) >> 8);
  x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x & 0xFFFF0000FFFF0000ULL) >> 16);
  return (x << 32) | (x >> 32);
}

inline uint32_t GetBucketIndex(uint64_t hash, uint32_t G) {
  if (G == 0) return 0;
  // 1. 翻转所有位
  uint64_t reversed = ReverseBits64(hash);
  // 2. 将翻转后的高 G 位移到低位作为索引
  return static_cast<uint32_t>(reversed >> (64 - G));
}

//=== LevelHashTableBuilder ===

LevelHashTableBuilder::LevelHashTableBuilder(
    const TableBuilderOptions& tb_options, WritableFileWriter* file, uint32_t initial_g)
    : tb_options_(tb_options),
      file_(file),
      io_status_(IOStatus::OK()),
      num_entries_(0) {
  const auto& context = static_cast<const TablePropertiesCollectorFactory::Context&>(tb_options);
  int current_level = (context.level_at_creation == TablePropertiesCollectorFactory::Context::kUnknownLevelAtCreation) 
                      ? 0 
                      : context.level_at_creation;
  
  G_ = initial_g + current_level; 
  num_buckets_ = 1 << G_;
  
  buffer_.resize(num_buckets_);
}

LevelHashTableBuilder::~LevelHashTableBuilder() {}

void LevelHashTableBuilder::Add(const Slice& key, const Slice& value) {
  if (!status_.ok()) return;

  Slice user_key = ExtractUserKey(key);
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  try {
    // TODO: 
    // 对于 Compaction (L1+)： 如果后续 Compaction 也复用这个 Builder， 感觉会 oom
    buffer_[bucket_idx].emplace_back(key.ToString(), value.ToString());
    num_entries_++;
    
    // 更新简单的 Properties
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();

  } catch (const std::exception& e) {
    status_ = Status::MemoryLimit("LevelHashTableBuilder buffer allocation failed");
  }
}

// 将 buf 中的数据实际写入到 sst
Status LevelHashTableBuilder::Finish() {
  if (!status_.ok()) return status_;

  // set bitmap
  size_t bitmap_size = (num_buckets_ + 63) / 64;
  valid_bucket_bitmap_.assign(bitmap_size, 0);

  for (uint32_t i = 0; i < num_buckets_; ++i) {
    if (!buffer_[i].empty()) {
      // Bucket i 有数据，置位
      valid_bucket_bitmap_[i / 64] |= (1ULL << (i % 64));
    }
  }
  
  std::vector<uint64_t> bucket_offsets(num_buckets_, 0);
  IOOptions io_opts; 

  // --- 阶段 1: 写入 Data Blocks ---
  for (uint32_t i = 0; i < num_buckets_; ++i) {
    bucket_offsets[i] = file_->GetFileSize();
    
    const auto& bucket_data = buffer_[i];
    std::string bucket_header;
    PutFixed32(&bucket_header, static_cast<uint32_t>(bucket_data.size()));
    
    status_ = file_->Append(io_opts, Slice(bucket_header)); 
    if (!status_.ok()) return status_;

    for (const auto& kv : bucket_data) {
      std::string entry_buf;
      PutLengthPrefixedSlice(&entry_buf, Slice(kv.first));
      PutLengthPrefixedSlice(&entry_buf, Slice(kv.second));
      
      status_ = file_->Append(io_opts, Slice(entry_buf));
      if (!status_.ok()) return status_;
    }
  }

  // --- 阶段 2: 写入 Bucket Index ---
  uint64_t index_offset = file_->GetFileSize();
  std::string index_buf;
  for (uint64_t offset : bucket_offsets) {
    PutFixed64(&index_buf, offset);
  }
  status_ = file_->Append(io_opts, Slice(index_buf));
  if (!status_.ok()) return status_;

  // --- 阶段 3: 写入 Footer ---
  std::string footer_buf;
  PutFixed64(&footer_buf, index_offset);
  PutFixed32(&footer_buf, G_);
  PutFixed64(&footer_buf, kLevelHashMagicNumber);
  status_ = file_->Append(io_opts, Slice(footer_buf));

  if (status_.ok()) {
    status_ = file_->Flush(io_opts);
  }
  
  if (!status_.ok()) {
      if (status_.IsIOError()) {
         // 这是一个简化的转换，实际应根据 Status 细节构建
         io_status_ = IOStatus::IOError(status_.ToString());
      } else {
         io_status_ = IOStatus::OK();
      }
  } else {
      io_status_ = IOStatus::OK();
  }
  
  return status_;
}

void LevelHashTableBuilder::Abandon() {
  buffer_.clear();
}

uint64_t LevelHashTableBuilder::NumEntries() const { return num_entries_; }

uint64_t LevelHashTableBuilder::FileSize() const {
  return file_ ? file_->GetFileSize() : 0;
}

bool LevelHashTableBuilder::IsEmpty() const { return num_entries_ == 0; }

Status LevelHashTableBuilder::status() const { return status_; }

// --- 实现虚函数 ---

IOStatus LevelHashTableBuilder::io_status() const {
  return io_status_;
}

TableProperties LevelHashTableBuilder::GetTableProperties() const {
  return properties_;
}

std::string LevelHashTableBuilder::GetFileChecksum() const {
  if (file_) {
    return file_->GetFileChecksum();
  }
  return kUnknownFileChecksum;
}

const char* LevelHashTableBuilder::GetFileChecksumFuncName() const {
  if (file_) {
    return file_->GetFileChecksumFuncName();
  }
  return kUnknownFileChecksumFuncName;
}

//=== LevelHashTableReader Implementations ===

LevelHashTableReader::LevelHashTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size)
    : table_reader_options_(table_reader_options),
      file_(std::move(file)),
      file_size_(file_size),
      G_(0),
      num_buckets_(0),
      index_offset_(0) {
  table_properties_ = std::make_shared<TableProperties>();
  user_comparator_ = table_reader_options_.internal_comparator.user_comparator();
}

LevelHashTableReader::~LevelHashTableReader() {}

Status LevelHashTableReader::Open(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader) {
  
  if (file_size < kFooterSize) {
    return Status::Corruption("LevelHashTable: File is too short");
  }

  std::unique_ptr<LevelHashTableReader> reader(
      new LevelHashTableReader(table_reader_options, std::move(file), file_size));

  Status s = reader->LoadIndex();
  if (!s.ok()) {
    return s;
  }

  *table_reader = std::move(reader);
  return Status::OK();
}

Status LevelHashTableReader::LoadIndex() {
  char footer_scratch[kFooterSize];
  Slice footer_slice;
  
  IOOptions io_opts; 
  
  Status s = file_->Read(io_opts, file_size_ - kFooterSize, kFooterSize, 
                         &footer_slice, footer_scratch, nullptr, nullptr);
  if (!s.ok()) return s;

  const char* p = footer_slice.data();
  index_offset_ = DecodeFixed64(p);
  p += 8;
  G_ = DecodeFixed32(p);
  p += 4;
  uint64_t magic = DecodeFixed64(p);

  if (magic != kLevelHashMagicNumber) {
    return Status::Corruption("LevelHashTable: Invalid magic number");
  }

  num_buckets_ = 1 << G_;
  size_t index_size = num_buckets_ * sizeof(uint64_t);

  if (index_offset_ + index_size > file_size_ - kFooterSize) {
    return Status::Corruption("LevelHashTable: Invalid index offset");
  }

  std::vector<char> index_scratch(index_size);
  Slice index_slice;
  
  s = file_->Read(io_opts, index_offset_, index_size, 
                  &index_slice, index_scratch.data(), nullptr, nullptr);
  if (!s.ok()) return s;

  bucket_offsets_.resize(num_buckets_);
  const char* index_ptr = index_slice.data();
  for (uint32_t i = 0; i < num_buckets_; ++i) {
    bucket_offsets_[i] = DecodeFixed64(index_ptr);
    index_ptr += 8;
  }

  return Status::OK();
}

// 解析长度前缀的 Slice
const char* ReadLengthPrefixedSlice(const char* p, const char* limit, Slice* result) {
  uint32_t len = 0;
  // 使用 coding.h 中的 GetVarint32Ptr
  p = GetVarint32Ptr(p, limit, &len); 
  if (p == nullptr) return nullptr;
  if (p + len > limit) return nullptr;
  *result = Slice(p, len);
  return p + len;
}



Status LevelHashTableReader::Get(const ReadOptions& /*read_options*/,
                                 const Slice& key,
                                 GetContext* get_context,
                                 const SliceTransform* /*prefix_extractor*/,
                                 bool /*skip_filters*/) {
  Slice user_key = ExtractUserKey(key);
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  uint32_t bucket_idx = GetBucketIndex(hash, G_);

  uint64_t offset_start = bucket_offsets_[bucket_idx];
  uint64_t offset_end = (bucket_idx == num_buckets_ - 1) 
                        ? index_offset_ 
                        : bucket_offsets_[bucket_idx + 1];

  size_t bucket_size = static_cast<size_t>(offset_end - offset_start);
  if (bucket_size == 0) return Status::OK();

  std::vector<char> bucket_scratch(bucket_size);
  Slice bucket_slice;
  
  IOOptions io_opts;
  Status s = file_->Read(io_opts, offset_start, bucket_size, 
                         &bucket_slice, bucket_scratch.data(), nullptr, nullptr);
  if (!s.ok()) return s;

  const char* p = bucket_slice.data();
  const char* limit = p + bucket_slice.size();

  if (limit - p < 4) return Status::Corruption("LevelHashTable: Bucket too short");
  
  uint32_t num_entries = DecodeFixed32(p);
  p += 4;

  for (uint32_t i = 0; i < num_entries; ++i) {
    if (p >= limit) break;

    Slice entry_key_slice;
    p = ReadLengthPrefixedSlice(p, limit, &entry_key_slice);
    if (p == nullptr) return Status::Corruption("LevelHashTable: Bad key entry");

    Slice entry_value_slice;
    p = ReadLengthPrefixedSlice(p, limit, &entry_value_slice);
    if (p == nullptr) return Status::Corruption("LevelHashTable: Bad value entry");

    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(entry_key_slice, &parsed_key, true /* log_err_key */).ok()) {
        return Status::Corruption("LevelHashTable: Invalid internal key in bucket");
    }


    bool matched = false;
    Status read_status;

    if (user_comparator_->Compare(parsed_key.user_key, user_key) != 0) {
        // Key 不匹配，继续查看 Bucket 中的下一个 Entry
        continue; 
    }
    
    // get_context.cc SaveValue —— merge, seqno logic ...
    // 返回 false 意味着找到了最终结果（Found 或 Deleted），应停止搜索
    // 返回 true 意味着虽然 Key 匹配，但可能是一个 Merge 操作，需要继续找更旧的版本
    // TODO: jinyibuqueren
    bool keep_searching = get_context->SaveValue(parsed_key, entry_value_slice, &matched, &read_status, nullptr);

    if (!read_status.ok()) {
        return read_status;
    }

    //  false 表示找到了结果（或者确定不存在），应停止搜索
    //  true 表示需要继续搜索（例如 Key 不匹配，或者只是合并了部分值）
    if (!keep_searching) {
        return Status::OK();
    }
  }

  return Status::OK();
}

uint64_t LevelHashTableReader::ApproximateOffsetOf(const ReadOptions& /*read_options*/,
                                                   const Slice& key,
                                                   TableReaderCaller /*caller*/) {
  Slice user_key = ExtractUserKey(key);
  uint64_t hash = MurmurHash64A(user_key.data(), user_key.size(), 0);
  uint32_t bucket_idx = GetBucketIndex(hash, G_);
  if (bucket_idx < bucket_offsets_.size()) {
    return bucket_offsets_[bucket_idx];
  }
  return 0;
}

uint64_t LevelHashTableReader::ApproximateSize(const ReadOptions& /*read_options*/,
                                               const Slice& /*start*/, const Slice& /*end*/,
                                               TableReaderCaller /*caller*/) {
  return file_size_ / 2;
}

size_t LevelHashTableReader::ApproximateMemoryUsage() const {
  return bucket_offsets_.capacity() * sizeof(uint64_t);
}

InternalIterator* LevelHashTableReader::NewIterator(
    const ReadOptions& /*read_options*/, const SliceTransform* /*prefix_extractor*/,
    Arena* arena, bool /*skip_filters*/, TableReaderCaller /*caller*/,
    size_t /*compaction_readahead_size*/, bool /*allow_unprepared_value*/) {
  
  if (arena) {
    void* mem = arena->AllocateAligned(sizeof(LevelHashTableIterator));
    return new (mem) LevelHashTableIterator(file_.get(), bucket_offsets_, index_offset_, num_buckets_);
  } else {
    return new LevelHashTableIterator(file_.get(), bucket_offsets_, index_offset_, num_buckets_);
  }
}

//=== LevelHashTableFactory ===

TableBuilder* LevelHashTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    WritableFileWriter* file) const {
  return new LevelHashTableBuilder(table_builder_options, file, initial_g_);
}

Status LevelHashTableFactory::NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file_reader, 
      uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch) const {
  // 默认 ReadOptions
  ReadOptions ro;
  return NewTableReader(ro, table_reader_options, std::move(file_reader),
                        file_size, table_reader, prefetch);
}

Status LevelHashTableFactory::NewTableReader(
    const ReadOptions& ro,
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  
  return LevelHashTableReader::Open(table_reader_options, std::move(file),
                                    file_size, table_reader);
}

}  // namespace ROCKSDB_NAMESPACE