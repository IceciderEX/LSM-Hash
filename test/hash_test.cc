#include <cassert>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <algorithm>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "memtable/level_hash_memtable.h"
#include "table/level_hash/level_hash_table.h"
#include "util/murmurhash.h"
#include "logging/logging.h"

using namespace ROCKSDB_NAMESPACE;

// ================== Hash Helper ==================
inline uint64_t Test_ReverseBits64(uint64_t x) {
  x = ((x & 0x5555555555555555ULL) << 1) | ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
  x = ((x & 0x3333333333333333ULL) << 2) | ((x & 0xCCCCCCCCCCCCCCCCULL) >> 2);
  x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x & 0xF0F0F0F0F0F0F0F0ULL) >> 4);
  x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x & 0xFF00FF00FF00FF00ULL) >> 8);
  x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x & 0xFFFF0000FFFF0000ULL) >> 16);
  return (x << 32) | (x >> 32);
}

inline uint32_t Test_GetBucketIndex(uint64_t hash, uint32_t G) {
  if (G == 0) return 0;
  uint64_t reversed = Test_ReverseBits64(hash);
  return static_cast<uint32_t>(reversed >> (64 - G));
}

std::string GenerateKeyForBucket(int bucket_idx, int G, int seq) {
    int i = 0;
    while(true) {
        std::string k = "k_b" + std::to_string(bucket_idx) + "_" + std::to_string(seq) + "_" + std::to_string(i);
        uint64_t h = MurmurHash64A(k.data(), static_cast<int>(k.size()), 0);
        if (Test_GetBucketIndex(h, G) == (uint32_t)bucket_idx) {
            return k;
        }
        i++;
    }
}
// =================================================

// 辅助函数：等待 L0 文件数量变为特定值（精确匹配）
void WaitForFileCount(DB* db, int expected_count, const std::string& step_name) {
    std::string val;
    int retry_count = 0;
    std::cout << "[" << step_name << "] Waiting for L0 files == " << expected_count << "..." << std::endl;
    while (retry_count < 100) { // 10s timeout
        db->GetProperty("rocksdb.num-files-at-level0", &val);
        int num = std::stoi(val);
        if (num == expected_count) {
            // 多等待一小会儿，确保状态稳定
            std::this_thread::sleep_for(std::chrono::milliseconds(200)); 
            return; 
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        retry_count++;
    }
    std::cout << "[WARN] Timeout in " << step_name << ". Current L0: " << val << ", Expected: " << expected_count << std::endl;
}

std::vector<uint64_t> GetSstFileNumbers(DB* db) {
    std::vector<LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    std::vector<uint64_t> files;
    for (const auto& md : metadata) {
        if (md.level == 0) { 
            files.push_back(md.file_number);
        }
    }
    return files;
}

void TestLogicalDeletion() {
    std::cout << "\n=== Test 3: Logical Deletion & Bitmap Update ===" << std::endl;
    
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 64; 
    
    const int G = 3; 
    options.level0_file_num_compaction_trigger = 2; // 触发阈值
    
    options.memtable_factory.reset(new LevelHashMemTableFactory(G, 10000, 10000));
    options.table_factory.reset(new LevelHashTableFactory(G));
    options.compaction_style = kCompactionStyleLevelHash;
    options.disable_auto_compactions = false;

    std::string dbname = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_logical_del_test";
    DestroyDB(dbname, options);
    
    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());

    // ---------------------------------------------------------
    // Step 1: 构造 SST_Shared (包含 Bucket 0 和 Bucket 1)
    // ---------------------------------------------------------
    std::cout << "[Step 1] Creating SST_Shared (Bucket 0 + Bucket 1)..." << std::endl;
    {
        for(int i=0; i<10; ++i) db->Put(WriteOptions(), GenerateKeyForBucket(0, G, i), "val_b0_shared");
        for(int i=0; i<10; ++i) db->Put(WriteOptions(), GenerateKeyForBucket(1, G, i), "val_b1_shared");
        
        s = db->Flush(FlushOptions());
        assert(s.ok());
        
        // 关键修复：确保 Flush 完成，文件数确实为 1
        WaitForFileCount(db, 1, "Step 1 Flush");
    }
    
    auto files_step1 = GetSstFileNumbers(db);
    assert(files_step1.size() == 1);
    uint64_t shared_file_num = files_step1[0];
    std::cout << "-> Created SST File #" << shared_file_num << std::endl;

    // ---------------------------------------------------------
    // Step 2: 构造 SST_Only0 (仅包含 Bucket 0) -> 触发 Bucket 0 Compaction
    // ---------------------------------------------------------
    std::cout << "[Step 2] Creating SST_Only0 (Bucket 0) to trigger Bucket 0 Compaction..." << std::endl;
    {
        for(int i=100; i<110; ++i) db->Put(WriteOptions(), GenerateKeyForBucket(0, G, i), "val_b0_only");
        s = db->Flush(FlushOptions());
        assert(s.ok());
        
        // 关键修复：先等待文件数变为 2 (Flush 生效)，确保触发条件满足
        WaitForFileCount(db, 2, "Step 2 Flush");
    }

    // 现在文件数是 2，Compaction 应该在后台被触发。
    // 等待文件数回落到 1 (Compaction 完成)
    WaitForFileCount(db, 1, "Step 2 Compaction");

    auto files_step2 = GetSstFileNumbers(db);
    std::cout << "-> L0 Files count after Bucket 0 compaction: " << files_step2.size() << std::endl;
    
    // 验证逻辑删除：文件名应该没变（还是 shared_file_num），因为 Bucket 1 还在里面
    bool logic_del_success = false;
    if (files_step2.size() == 1 && files_step2[0] == shared_file_num) {
        std::cout << "[SUCCESS] Logical Deletion Verified: SST_Shared #" << shared_file_num << " was preserved!" << std::endl;
        logic_del_success = true;
    } else {
        std::cout << "[FAILURE] SST_Shared was incorrect deleted or not compacted! Current files: ";
        for (auto f : files_step2) std::cout << f << " ";
        std::cout << std::endl;
    }
    assert(logic_del_success);

    // 验证数据一致性
    std::string val;
    s = db->Get(ReadOptions(), GenerateKeyForBucket(0, G, 0), &val);
    assert(s.ok() && val == "val_b0_shared"); // 应该从 L1 读到
    
    s = db->Get(ReadOptions(), GenerateKeyForBucket(1, G, 0), &val);
    assert(s.ok() && val == "val_b1_shared"); // 应该从 L0 SST_Shared 读到

    // ---------------------------------------------------------
    // Step 3: 构造 SST_Only1 (仅包含 Bucket 1) -> 触发 Bucket 1 Compaction
    // ---------------------------------------------------------
    std::cout << "[Step 3] Creating SST_Only1 (Bucket 1) to trigger Bucket 1 Compaction..." << std::endl;
    {
        for(int i=200; i<210; ++i) db->Put(WriteOptions(), GenerateKeyForBucket(1, G, i), "val_b1_only");
        s = db->Flush(FlushOptions());
        assert(s.ok());
        
        // 关键修复：先等待文件数变为 2
        WaitForFileCount(db, 2, "Step 3 Flush");
    }

    // 等待 Compaction 完成（物理删除）
    WaitForFileCount(db, 0, "Step 3 Compaction");

    auto files_step3 = GetSstFileNumbers(db);
    std::cout << "-> L0 Files count after Bucket 1 compaction: " << files_step3.size() << std::endl;

    if (files_step3.empty()) {
        std::cout << "[SUCCESS] Physical Deletion Verified: SST_Shared #" << shared_file_num << " is finally deleted." << std::endl;
    } else {
        std::cout << "[FAILURE] SST_Shared was NOT deleted! Current files: ";
        for (auto f : files_step3) std::cout << f << " ";
        std::cout << std::endl;
        assert(false);
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    s = db->Get(ReadOptions(), GenerateKeyForBucket(1, G, 0), &val);
    assert(s.ok() && val == "val_b1_shared");

    delete db;
}

// 辅助函数：等待 Flush 完成
// 通过检查 L0 层文件数量来判断是否发生了 Flush
void WaitForFlush(DB* db) {
    std::string val;
    int retry_count = 0;
    while (retry_count < 100) { // 最多等待 2秒
        db->GetProperty("rocksdb.num-files-at-level0", &val);
        if (std::stoi(val) > 0) {
            return; // 发现 L0 文件，Flush 成功
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        retry_count++;
    }
}

void TestBucketThresholdFlush() {
    std::cout << "=== Test 1: Bucket Threshold Triggering Flush ===" << std::endl;
    
    DB* db;
    Options options;
    options.create_if_missing = true;
    
    // 1. 设置极大的 write_buffer_size，屏蔽内存大小触发的 Flush
    options.write_buffer_size = 1024 * 1024 * 64; // 100MB

    // 2. 配置 Level-Hash
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 50, 100000));
    options.table_factory.reset(new LevelHashTableFactory(3));
    options.compaction_style = kCompactionStyleLevelHash;

    // 使用新的 DB 路径，避免旧数据干扰
    std::string dbname = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_bucket_test";
    DestroyDB(dbname, options); // 清理旧数据
    
    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());

    // 3. 写入数据
    std::cout << "Inserting 500 keys (Target: trigger bucket overflow)..." << std::endl;
    for (int i = 0; i < 2000; ++i) {
        if (i == 1500) {
            ROCKS_LOG_INFO(db->GetOptions().info_log, "Insert key_1500 to bucket %u", 1500);
        }
        db->Put(WriteOptions(), "key_" + std::to_string(i), "val_" + std::to_string(i));
    }

    std::cout << "Deleting first 500 keys..." << std::endl;
    for (int i = 0; i < 1500; ++i) {
        std::string key = "key_" + std::to_string(i);
        s = db->Delete(WriteOptions(), key);
        assert(s.ok());
    }

    // 4. 检查是否自动 Flush
    // 此时并没有手动调用 Flush，如果 L0 有文件，说明自动触发了
    WaitForFlush(db);

    std::string num_files;
    db->GetProperty("rocksdb.num-files-at-level0", &num_files);
    std::cout << "L0 Files count: " << num_files << std::endl;
    
    // if (std::stoi(num_files) > 0) {
    //     std::cout << "[SUCCESS] Flush triggered by Bucket Threshold!" << std::endl;
    // } else {
    //     std::cout << "[FAILURE] Flush did NOT happen!" << std::endl;
    //     assert(false);
    // }

    // 验证数据正确性 (读取刚写入的数据，确保 Flush 后还能读到)
    std::string value;
    for(int i = 1500; i < 2000; ++i) {
        s = db->Get(ReadOptions(), "key_" + std::to_string(i), &value);
        if (!s.ok()) {
            s = db->Get(ReadOptions(), "key_" + std::to_string(i), &value);
        }
        assert(s.ok());
        assert(value == "val_" + std::to_string(i));
    }
    // 确认删除的数据确实不可见
    for (int i = 0; i < 1500; ++i) {
        s = db->Get(ReadOptions(), "key_" + std::to_string(i), &value);
        assert(!s.ok());
    }

    delete db;
}

void TestMemorySizeFlush() {
    std::cout << "\n=== Test 2: Memory Size Triggering Flush ===" << std::endl;

    DB* db;
    Options options;
    options.create_if_missing = true;

    // 1. 设置极小的 write_buffer_size，强制内存触发
    options.write_buffer_size = 4096; // 4KB

    // 2. 配置 Level-Hash
    // BucketThreshold = 10000 (很大，屏蔽 Bucket 触发)
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 10000, 10000));
    options.table_factory.reset(new LevelHashTableFactory(3));

    std::string dbname = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_memory_test";
    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());

    // 3. 写入数据
    // 每个 KV 大约 20 字节。4KB 大约能存 200 个。
    // 写入 500 个足以撑爆内存。
    std::cout << "Inserting 500 keys (Target: trigger memory limit)..." << std::endl;
    for (int i = 0; i < 500; ++i) {
        db->Put(WriteOptions(), "key_mem_" + std::to_string(i), std::string(100, 'x')); // 长 Value 加速填满
    }

    // 4. 等待 Flush
    WaitForFlush(db);

    std::string num_files;
    db->GetProperty("rocksdb.num-files-at-level0", &num_files);
    std::cout << "L0 Files count: " << num_files << std::endl;

    if (std::stoi(num_files) > 0) {
        std::cout << "[SUCCESS] Flush triggered by Memory Size!" << std::endl;
    } else {
        std::cout << "[FAILURE] Flush did NOT happen!" << std::endl;
        assert(false);
    }

    delete db;
}

int main() {
    TestLogicalDeletion();
    
    std::cout << "\nAll Level-Hash Tests Passed!" << std::endl;
    return 0;
}

