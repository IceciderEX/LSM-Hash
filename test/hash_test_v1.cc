#include <cassert>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "memtable/level_hash_memtable.h"
#include "table/level_hash/level_hash_table.h"

using namespace ROCKSDB_NAMESPACE;

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
    options.write_buffer_size = 1024 * 1024 * 100; // 100MB

    // 2. 配置 Level-Hash
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 50, 100000));
    options.table_factory.reset(new LevelHashTableFactory(3));

    // 使用新的 DB 路径，避免旧数据干扰
    std::string dbname = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_bucket_test";
    DestroyDB(dbname, options); // 清理旧数据
    
    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());

    // 3. 写入数据
    std::cout << "Inserting 500 keys (Target: trigger bucket overflow)..." << std::endl;
    for (int i = 0; i < 500; ++i) {
        db->Put(WriteOptions(), "key_" + std::to_string(i), "val_" + std::to_string(i));
    }

    // 4. 检查是否自动 Flush
    // 此时并没有手动调用 Flush，如果 L0 有文件，说明自动触发了
    WaitForFlush(db);

    std::string num_files;
    db->GetProperty("rocksdb.num-files-at-level0", &num_files);
    std::cout << "L0 Files count: " << num_files << std::endl;
    
    if (std::stoi(num_files) > 0) {
        std::cout << "[SUCCESS] Flush triggered by Bucket Threshold!" << std::endl;
    } else {
        std::cout << "[FAILURE] Flush did NOT happen!" << std::endl;
        assert(false);
    }

    // 验证数据正确性 (读取刚写入的数据，确保 Flush 后还能读到)
    std::string value;
    for(int i = 0; i < 500; ++i) {
        s = db->Get(ReadOptions(), "key_" + std::to_string(i), &value);
        if (!s.ok()) {
            s = db->Get(ReadOptions(), "key_" + std::to_string(i), &value);
        }
        assert(s.ok());
        assert(value == "val_" + std::to_string(i));
    }
    s = db->Get(ReadOptions(), "key_2000", &value);
    assert(!s.ok());

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
    TestBucketThresholdFlush();
    TestMemorySizeFlush();
    
    std::cout << "\nAll Automatic Flush Tests Passed!" << std::endl;
    return 0;
}

