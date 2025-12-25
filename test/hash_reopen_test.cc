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
#include <filesystem>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "memtable/level_hash_memtable.h"
#include "table/level_hash/level_hash_table.h"

using namespace ROCKSDB_NAMESPACE;

// --- 测试配置 ---
const int kNumRounds = 5;            // 重启 5 轮
const int kOpsPerRound = 50000;      // 每轮写入/删除操作数
const int kNumKeys = 5000;           // Key 范围
std::string kDBPath = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_reopen_test";

// --- 全局真值表 ---
// 只要进程不退，这个 Map 就一直存在，用于跨 DB 生命周期验证
std::mutex kv_mutex;
std::map<std::string, std::string> global_kv_map; 

std::string Key(int i) {
    std::stringstream ss;
    ss << "key_" << std::setw(10) << std::setfill('0') << i;
    return ss.str();
}

std::string Value(int i, int round) {
    return "val_" + std::to_string(i) + "_r" + std::to_string(round);
}

// 验证 DB 数据是否与内存 Map 一致
void VerifyDB(DB* db, int round) {
    std::lock_guard<std::mutex> lock(kv_mutex);
    std::cout << "[Round " << round << "] Verifying " << global_kv_map.size() << " keys..." << std::endl;
    
    int errors = 0;
    int checked = 0;
    for (const auto& pair : global_kv_map) {
        std::string key = pair.first;
        std::string expected_val = pair.second;
        
        std::string db_val;
        Status s = db->Get(ReadOptions(), key, &db_val);
        
        if (expected_val == "DELETED") {
            if (!s.IsNotFound()) {
                std::cerr << "[FATAL] Zombie Key Detected! Key: " << key 
                          << " Should be Deleted, but got: " << db_val << std::endl;
                errors++;
            }
        } else {
            if (s.IsNotFound()) {
                std::cerr << "[FATAL] Key Lost! Key: " << key << " Expected: " << expected_val << std::endl;
                errors++;
            } else if (db_val != expected_val) {
                std::cerr << "[FATAL] Value Mismatch! Key: " << key 
                          << " Expected: " << expected_val << " Got: " << db_val << std::endl;
                errors++;
            }
        }
        checked++;
        if (errors > 10) break;
    }

    if (errors == 0) {
        std::cout << "[Round " << round << "] Verification Passed (" << checked << " keys)." << std::endl;
    } else {
        std::cout << "[Round " << round << "] Verification FAILED with " << errors << " errors." << std::endl;
        exit(1);
    }
}

// 构造 Options
Options GetOptions() {
    Options options;
    options.create_if_missing = true;
    // Level-Hash 配置
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 200, 1024*1024*2));
    options.table_factory.reset(new LevelHashTableFactory(3));
    options.compaction_style = kCompactionStyleLevelHash;
    
    // 激进 Flush/Compact
    options.write_buffer_size = 64 * 1024; // 64KB MemTable 
    options.level0_file_num_compaction_trigger = 4; 
    options.target_file_size_base = 64 * 1024;
    options.max_background_jobs = 4;
    
    return options;
}

int main() {
    // 1. 清理环境 (可选，如果想接着之前的跑可以注释掉 DestroyDB)
    DestroyDB(kDBPath, GetOptions());
    
    // 随机数生成
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);
    std::uniform_int_distribution<int> op_dist(0, 100);

    for (int round = 1; round <= kNumRounds; ++round) {
        std::cout << "\n=== Round " << round << " Start ===" << std::endl;

        DB* db;
        Status s = DB::Open(GetOptions(), kDBPath, &db);
        if (!s.ok()) {
            std::cerr << "DB Open Failed: " << s.ToString() << std::endl;
            return 1;
        }

        // 2. 写入/删除数据
        std::cout << "[Round " << round << "] Writing " << kOpsPerRound << " ops..." << std::endl;
        for (int i = 0; i < kOpsPerRound; ++i) {
            int k = key_dist(rng);
            std::string key = Key(k);
            
            // 30% 概率删除，70% 概率写入
            bool is_delete = (op_dist(rng) < 30);
            
            std::lock_guard<std::mutex> lock(kv_mutex);
            if (is_delete) {
                db->Delete(WriteOptions(), key);
                global_kv_map[key] = "DELETED"; // 标记为已删除
            } else {
                std::string val = Value(k, round);
                db->Put(WriteOptions(), key, val);
                global_kv_map[key] = val;
            }
        }

        // 3. 强制 Flush，确保数据落盘生成 SST
        std::cout << "[Round " << round << "] Flushing..." << std::endl;
        FlushOptions fo;
        fo.wait = true;
        db->Flush(fo);

        // 4. 睡眠等待 Compaction 发生 (模拟真实后台运行)
        // 这一步至关重要：Compaction 会修改 FileMetaData 的 bitmap，
        // 我们要测试的就是这个修改后的 bitmap 能否被正确持久化到 Manifest。
        std::cout << "[Round " << round << "] Sleeping 5s to allow Compaction..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 打印文件层级
        std::string num_l0, num_l1, num_l2, num_l3;
        db->GetProperty("rocksdb.num-files-at-level0", &num_l0);
        db->GetProperty("rocksdb.num-files-at-level1", &num_l1);
        db->GetProperty("rocksdb.num-files-at-level2", &num_l2);
        db->GetProperty("rocksdb.num-files-at-level3", &num_l3);
        std::cout << "[Round " << round << "] Stats - L0: " << num_l0 << ", L1: " << num_l1 << ", L2: " << num_l2 << ", L3: " << num_l3 << std::endl;

        // 5. 关闭 DB (模拟 Crash/Restart)
        std::cout << "[Round " << round << "] Closing DB..." << std::endl;
        delete db; 

        // 6. 重新打开并验证
        // 这里验证的是：重启后，通过 Manifest 加载的 Bitmap 是否正确屏蔽了已删除或已移动的 Bucket
        std::cout << "[Round " << round << "] Reopening DB for Verification..." << std::endl;
        s = DB::Open(GetOptions(), kDBPath, &db);
        if (!s.ok()) {
            std::cerr << "DB Reopen Failed: " << s.ToString() << std::endl;
            return 1;
        }

        VerifyDB(db, round);
        
        // 保持 DB 关闭状态进入下一轮
        delete db;
    }

    std::cout << "\n=== All Recovery Tests Passed ===" << std::endl;
    return 0;
}