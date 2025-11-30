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
};

// Factory
class LevelHashTableFactory : public TableFactory {
 public:
  explicit LevelHashTableFactory(uint32_t initial_g = 3) : initial_g_(initial_g) {}

  const char* Name() const override { return "LevelHashTableFactory"; }

  Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override;

 private:
  uint32_t initial_g_;
};


}  // namespace ROCKSDB_NAMESPACE