// Copyright (C) 2023-present, RocksDB-Level-Hash. All rights reserved.
#pragma once

#include <memory>
#include <vector>
#include <string>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/table_builder.h" 

namespace ROCKSDB_NAMESPACE {
  
const char* ReadLengthPrefixedSlice(const char* p, const char* limit, Slice* result);

class LevelHashTableIterator : public InternalIterator {
 public:
  LevelHashTableIterator(RandomAccessFileReader* file,
                         const std::vector<uint64_t>& bucket_offsets,
                         uint64_t index_offset,
                         uint32_t num_buckets)
      : file_(file),
        bucket_offsets_(bucket_offsets),
        index_offset_(index_offset),
        num_buckets_(num_buckets),
        current_bucket_idx_(0),
        valid_(false),
        status_(Status::OK()) {}

  ~LevelHashTableIterator() override {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    current_bucket_idx_ = 0;
    LoadBucket(current_bucket_idx_);
    // 如果第一个 Bucket 是空的或者没有有效 Entry，继续找下一个
    if (!valid_entry_) {
      FindNextValidEntry();
    }
  }

  void SeekToLast() override {
    // Hash 表无序，SeekToLast 语义通常用于 Range Scan。
    // 这里简单实现为跳到最后一个 Bucket 的最后一个 Entry。
    // 实际 Compaction 很少用到 SeekToLast。
    if (num_buckets_ == 0) {
        valid_ = false;
        return;
    }
    current_bucket_idx_ = num_buckets_ - 1;
    LoadBucket(current_bucket_idx_);
    
    // 遍历到当前 Bucket 的末尾
    if (valid_entry_) {
        // 简单做法：一直 Next 直到当前 Bucket 结束
        // (更高效的做法是记录 offsets，但这里为了简化直接遍历)
        while (HasNextInCurrentBucket()) {
            NextEntry();
        }
    } else {
        // 如果最后一个 Bucket 空，向前找？不支持反向遍历
        valid_ = false; 
    }
  }

  void Seek(const Slice& /*target*/) override {
    // Hash 表不支持范围 Seek。
    // 对于 Level-Hash Compaction，我们只需要遍历所有数据，所以退化为 SeekToFirst。
    SeekToFirst();
  }

  void SeekForPrev(const Slice& /*target*/) override {
    SeekToFirst(); 
  }

  void Next() override {
    if (!valid_) return;
    
    // 尝试在当前 Bucket 读下一个 Entry
    if (NextEntry()) {
      return;
    }

    // 当前 Bucket 读完了，移动到下一个 Bucket
    current_bucket_idx_++;
    FindNextValidEntry();
  }

  void Prev() override {
    valid_ = false; // 不支持
    status_ = Status::NotSupported("LevelHashTableIterator::Prev not supported");
  }

  Slice key() const override {
    assert(valid_);
    return current_key_;
  }

  Slice value() const override {
    assert(valid_);
    return current_value_;
  }

  Status status() const override { return status_; }

 private:
  RandomAccessFileReader* file_;
  const std::vector<uint64_t>& bucket_offsets_;
  uint64_t index_offset_;
  uint32_t num_buckets_;

  // 迭代状态
  uint32_t current_bucket_idx_;
  bool valid_;
  Status status_;
  
  // 当前 Bucket 数据缓存
  std::vector<char> bucket_buf_;
  const char* buf_ptr_ = nullptr;
  const char* buf_limit_ = nullptr;
  uint32_t entries_remaining_in_bucket_ = 0;
  bool valid_entry_ = false;

  // 当前 Key/Value
  Slice current_key_;
  Slice current_value_;

  // 加载指定 Bucket 到内存
  void LoadBucket(uint32_t bucket_idx) {
    valid_entry_ = false;
    valid_ = false;
    entries_remaining_in_bucket_ = 0;
    
    if (bucket_idx >= num_buckets_) {
      return;
    }

    uint64_t start = bucket_offsets_[bucket_idx];
    uint64_t end = (bucket_idx == num_buckets_ - 1) ? index_offset_ : bucket_offsets_[bucket_idx + 1];
    
    // 空 Bucket (Offset 相等)
    if (start >= end) {
      return;
    }

    size_t size = static_cast<size_t>(end - start);
    bucket_buf_.resize(size);
    Slice result;
    IOOptions io_opts;
    status_ = file_->Read(io_opts, start, size, &result, bucket_buf_.data(), nullptr, nullptr);
    if (!status_.ok()) {
      return;
    }

    buf_ptr_ = result.data();
    buf_limit_ = buf_ptr_ + result.size();

    // 解析 Header (Num Entries)
    if (buf_limit_ - buf_ptr_ < 4) {
      status_ = Status::Corruption("Bucket too small");
      return;
    }
    
    entries_remaining_in_bucket_ = DecodeFixed32(buf_ptr_);
    buf_ptr_ += 4;
    
    if (entries_remaining_in_bucket_ > 0) {
        // 尝试读取第一个 Entry
        if (NextEntry()) {
            valid_ = true;
        }
    }
  }

  bool HasNextInCurrentBucket() const {
      return entries_remaining_in_bucket_ > 0 && buf_ptr_ < buf_limit_;
  }

  // 解析当前 Buffer 中的下一个 Entry
  bool NextEntry() {
    if (entries_remaining_in_bucket_ == 0 || buf_ptr_ >= buf_limit_) return false;

    Slice k, v;
    const char* p = buf_ptr_;
    
    // 使用 level_hash_table.cc 中定义的辅助函数
    p = ReadLengthPrefixedSlice(p, buf_limit_, &k);
    if (p == nullptr) {
        status_ = Status::Corruption("Bad key in bucket");
        return false;
    }
    
    p = ReadLengthPrefixedSlice(p, buf_limit_, &v);
    if (p == nullptr) {
        status_ = Status::Corruption("Bad value in bucket");
        return false;
    }

    buf_ptr_ = p;
    current_key_ = k;
    current_value_ = v;
    entries_remaining_in_bucket_--;
    valid_entry_ = true;
    valid_ = true;
    return true;
  }

  // 寻找下一个非空 Bucket
  void FindNextValidEntry() {
    while (current_bucket_idx_ < num_buckets_) {
      LoadBucket(current_bucket_idx_);
      if (valid_) return; // 找到了
      if (!status_.ok()) return; // 出错了
      current_bucket_idx_++;
    }
    valid_ = false;
  }
};

// Builder: 负责接收无序的 Key-Value 流，并在内存中按 Bucket 组织，最后写入文件
class LevelHashTableBuilder : public TableBuilder {
 public:
  LevelHashTableBuilder(const TableBuilderOptions& tb_options, 
                        WritableFileWriter* file, 
                        uint32_t initial_g);

  ~LevelHashTableBuilder() override;

  void Add(const Slice& key, const Slice& value) override;

  Status Finish() override;
  void Abandon() override;
  uint64_t NumEntries() const override;
  uint64_t FileSize() const override;
  bool IsEmpty() const override;
  Status status() const override;

  IOStatus io_status() const override;
  TableProperties GetTableProperties() const override;
  std::string GetFileChecksum() const override;
  const char* GetFileChecksumFuncName() const override;
  const std::vector<uint64_t>& GetValidBucketBitmap() const {
    return valid_bucket_bitmap_;
  }

 private:
  const TableBuilderOptions& tb_options_;
  WritableFileWriter* file_;
  Status status_;
  IOStatus io_status_; // 记录 IO 状态
  
  uint32_t G_;
  uint32_t num_buckets_;
  uint64_t num_entries_;

  std::vector<std::vector<std::pair<std::string, std::string>>> buffer_;
  std::vector<uint64_t> valid_bucket_bitmap_;
  // 缓存 TableProperties
  TableProperties properties_;
};

// Reader for Level-Hash table format.
class LevelHashTableReader : public TableReader {
 public:
  static Status Open(const TableReaderOptions& table_reader_options,
                     std::unique_ptr<RandomAccessFileReader>&& file,
                     uint64_t file_size,
                     std::unique_ptr<TableReader>* table_reader);

  ~LevelHashTableReader() override;

  Status Get(const ReadOptions& read_options, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters) override;

  InternalIterator* NewIterator(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool skip_filters, TableReaderCaller caller,
      size_t compaction_readahead_size = 0,
      bool allow_unprepared_value = false) override;

  uint64_t ApproximateOffsetOf(const ReadOptions& read_options,
                               const Slice& key,
                               TableReaderCaller caller) override;

  uint64_t ApproximateSize(const ReadOptions& read_options,
                           const Slice& start, const Slice& end,
                           TableReaderCaller caller) override;

  size_t ApproximateMemoryUsage() const override;

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }
  
 private:
  LevelHashTableReader(const TableReaderOptions& table_reader_options,
                       std::unique_ptr<RandomAccessFileReader>&& file,
                       uint64_t file_size);

  Status LoadIndex();

  const TableReaderOptions& table_reader_options_;
  std::unique_ptr<RandomAccessFileReader> file_;
  uint64_t file_size_;
  std::shared_ptr<const TableProperties> table_properties_;

  uint32_t G_;
  uint32_t num_buckets_;
  uint64_t index_offset_;
  std::vector<uint64_t> bucket_offsets_;

  const Comparator* user_comparator_;
};

// Factory
class LevelHashTableFactory : public TableFactory {
 public:
  explicit LevelHashTableFactory(uint32_t initial_g = 3) : initial_g_(initial_g) {}

  const char* Name() const override { return "LevelHashTableFactory"; }

  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<LevelHashTableFactory>(initial_g_);
  }

  Status NewTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file_reader,
    uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool prefetch) const override;

  Status NewTableReader(
      const ReadOptions& ro,
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file_reader, 
      uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override;

 private:
  uint32_t initial_g_;
};


}  // namespace ROCKSDB_NAMESPACE