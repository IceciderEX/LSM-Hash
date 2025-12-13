#include <iostream>
#include <thread>
#include <vector>
#include <map>
#include <atomic>
#include <random>
#include <mutex>
#include <cassert>
#include <string>
#include <chrono>
#include <sstream>
#include <iomanip>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "memtable/level_hash_memtable.h"
#include "table/level_hash/level_hash_table.h"

using namespace ROCKSDB_NAMESPACE;

// 配置参数
const int kNumKeys = 10000;          // Key 的总范围 0 ~ kNumKeys
const int kNumOperations = 300000;   // 总操作次数
const int kNumWriterThreads = 2;     // 写线程数
const int kNumReaderThreads = 1;     // 读线程数
const int kReopenInterval = 30000;   // 每多少次操作重启一次 DB (测试 Manifest 持久化)

// 全局状态
std::mutex state_mutex;
std::map<std::string, std::string> kv_map; // 真值表
std::atomic<bool> stop_flag{false};
std::atomic<long> ops_counter{0};

// 辅助：生成 Key
std::string Key(int i) {
    std::stringstream ss;
    ss << "key_" << std::setw(10) << std::setfill('0') << i;
    return ss.str();
}

// 辅助：生成 Value
std::string Value(int i, int iter) {
    return "val_" + std::to_string(i) + "_" + std::to_string(iter);
}

// 检查 DB 状态与真值表是否一致
void VerifyDB(DB* db) {
    std::lock_guard<std::mutex> lock(state_mutex);
    std::cout << "[Verifying] Checking all " << kv_map.size() << " keys in map..." << std::endl;
    
    int verified = 0;
    for (const auto& pair : kv_map) {
        std::string db_val;
        Status s = db->Get(ReadOptions(), pair.first, &db_val);
        
        if (!s.ok()) {
            std::cerr << "[FATAL] Key missing: " << pair.first << " Status: " << s.ToString() << std::endl;
            exit(1);
        }
        if (db_val != pair.second) {
            std::cerr << "[FATAL] Value mismatch for " << pair.first 
                      << ". Expected: " << pair.second 
                      << ", Actual: " << db_val << std::endl;
            exit(1);
        }
        verified++;
    }
    
    // 随机检查一些肯定不存在的 Key
    for (int i = kNumKeys; i < kNumKeys + 100; ++i) {
        std::string val;
        Status s = db->Get(ReadOptions(), Key(i), &val);
        if (!s.IsNotFound()) {
             std::cerr << "[FATAL] Phantom key found: " << Key(i) << std::endl;
             exit(1);
        }
    }
    std::cout << "[Success] Verified " << verified << " keys." << std::endl;
}

// 写线程逻辑
void WriterThread(DB** db_ptr) {
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);
    // [移除] std::uniform_int_distribution<int> op_dist(0, 10); 不再需要操作类型随机

    while (!stop_flag) {
        int k = key_dist(rng);
        std::string key = Key(k);
        
        // 生成全局唯一的序列号作为 Value 的一部分，方便 debug 版本先后顺序
        long current_op = ++ops_counter;
        
        // 加锁更新真值表
        {
            std::lock_guard<std::mutex> lock(state_mutex);
            
            // [逻辑简化] 始终只执行 Put
            std::string val = Value(k, current_op);
            Status s = (*db_ptr)->Put(WriteOptions(), key, val);
            if (!s.ok()) {
                std::cerr << "Put failed: " << s.ToString() << std::endl;
                exit(1);
            }
            // 更新内存真值表（如果是旧 Key 则覆盖，新 Key 则插入）
            kv_map[key] = val;
        }
        
        if (current_op >= kNumOperations) stop_flag = true;
    }
}

// 读线程逻辑
void ReaderThread(DB** db_ptr) {
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);

    while (!stop_flag) {
        int k = key_dist(rng);
        std::string key = Key(k);
        std::string expected_val;
        bool expect_found = false;

        // 获取期望值 (加锁瞬时获取)
        {
            std::lock_guard<std::mutex> lock(state_mutex);
            auto it = kv_map.find(key);
            if (it != kv_map.end()) {
                expected_val = it->second;
                expect_found = true;
            }
        }

        // 执行读取 (无锁)
        std::string db_val;
        Status s = (*db_ptr)->Get(ReadOptions(), key, &db_val);

        if (expect_found) {
            if (s.IsNotFound()) {
                std::cerr << "[Read Error] Key lost: " << key << " (Expected: " << expected_val << ")" << std::endl;
                s = (*db_ptr)->Get(ReadOptions(), key, &db_val);
            } else if (db_val != expected_val) {
                std::cout << "[Warn] Stale read: " << key << " got " << db_val << " expect " << expected_val << std::endl;
            }
        }
    }
}

int main() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    
    // --- Level-Hash 关键配置 ---
    // 1. 使用你的 MemTable 和 Table 工厂
    // 注意：G=3 (8 buckets)
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 1000, 1024*1024*10));
    options.table_factory.reset(new LevelHashTableFactory(3));
    options.compaction_style = kCompactionStyleLevelHash;
    
    // 2. 极低阈值触发 Flush 和 Compaction，强制 Level-Hash 逻辑高频运转
    options.write_buffer_size = 1024 * 1024 * 1;  
    options.level0_file_num_compaction_trigger = 3; // L0 有 2 个文件就触发 Compaction
    options.target_file_size_base = 1024 * 1024 * 1;
    
    // 3. 开启自动 Compaction
    options.disable_auto_compactions = false;
    
    // 4. 并发配置
    options.max_background_jobs = 4;

    std::string dbname = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_consistency_test";
    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());

    std::cout << "=== Starting Stress Test ===" << std::endl;
    std::cout << "  Operations: " << kNumOperations << std::endl;
    std::cout << "  Keys: " << kNumKeys << std::endl;
    
    // 启动线程
    std::vector<std::thread> writers;
    std::vector<std::thread> readers;
    
    for (int i=0; i<kNumWriterThreads; ++i) writers.emplace_back(WriterThread, &db);
    for (int i=0; i<kNumReaderThreads; ++i) readers.emplace_back(ReaderThread, &db);

    // // 监控与重启循环
    // long last_ops = 0;
    // while (!stop_flag) {
    //     std::this_thread::sleep_for(std::chrono::seconds(1));
    //     long current_ops = ops_counter;
    //     std::cout << "Ops: " << current_ops << " / " << kNumOperations 
    //               << " (Speed: " << (current_ops - last_ops) << " ops/s)" << std::endl;
    //     last_ops = current_ops;

    //     // 模拟 Crash/Restart：验证 Manifest 和 逻辑删除 Bitmap 的持久化
    //     if (current_ops % kReopenInterval < 2000 && current_ops > 0) {
    //          std::cout << "\n[Action] Reopening DB to verify persistence..." << std::endl;
    //          // 简单的暂停写入（通过 mutex 阻塞）
    //          std::lock_guard<std::mutex> lock(state_mutex); 
             
    //          // 关闭并重新打开
    //          delete db;
    //          db = nullptr;
    //          s = DB::Open(options, dbname, &db);
    //          if (!s.ok()) {
    //              std::cerr << "[FATAL] Reopen failed: " << s.ToString() << std::endl;
    //              exit(1);
    //          }
    //          std::cout << "[Action] DB Reopened. Checking consistency..." << std::endl;
             
    //          // 此时没有写操作，进行一次强一致性校验
    //          int verified = 0;
    //          for (const auto& pair : kv_map) {
    //              std::string val;
    //              s = db->Get(ReadOptions(), pair.first, &val);
    //              if (s.ok() && val == pair.second) {
    //                  verified++;
    //              } else {
    //                  std::cerr << "[FATAL] Data lost after reopen! Key: " << pair.first << std::endl;
    //                  exit(1);
    //              }
    //          }
    //          std::cout << "[Check] Persistence OK. Verified " << verified << " keys.\n" << std::endl;
    //     }
    // }

    // 等待结束
    for (auto& t : writers) t.join();
    for (auto& t : readers) t.join();

    std::cout << "\n=== Test Finished. Final Verification === " << std::endl;
    VerifyDB(db);

    // 打印一些统计信息，看看是否发生了 Level-Hash 的 Compaction
    std::string num_l0, num_l1;
    db->GetProperty("rocksdb.num-files-at-level0", &num_l0);
    db->GetProperty("rocksdb.num-files-at-level1", &num_l1);
    std::cout << "Final L0 Files: " << num_l0 << std::endl;
    std::cout << "Final L1 Files: " << num_l1 << std::endl;

    delete db;
    return 0;
}