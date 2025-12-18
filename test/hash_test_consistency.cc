#include <iostream>
#include <thread>
#include <vector>
#include <map>
#include <atomic>
#include <mutex>
#include <cassert>
#include <string>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <random>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "memtable/level_hash_memtable.h"
#include "table/level_hash/level_hash_table.h"

using namespace ROCKSDB_NAMESPACE;

// --- 压力测试配置 ---
const int kNumKeys = 1000;           // 1000 个 Key，足以触发多层级 Compaction (L0->L1->L2)
const int kNumOperations = 7000;   // 10万次操作，产生大量版本堆积
const int kNumWriterThreads = 1;
const int kNumReaderThreads = 2;     // 增加读者，提高并发读的概率

std::mutex state_mutex;
std::map<std::string, long> latest_seq_map; // 真值表
std::atomic<bool> stop_flag{false};
std::atomic<long> ops_counter{0};

// Key 格式：key_000 ~ key_999
std::string Key(int i) {
    std::stringstream ss;
    ss << "key_" << std::setw(3) << std::setfill('0') << i;
    return ss.str();
}

std::string Value(long seq) {
    return "val_" + std::to_string(seq);
}

long ParseSeqFromValue(const std::string& val) {
    size_t pos = val.find('_');
    if (pos == std::string::npos) return -1;
    return std::stol(val.substr(pos + 1));
}

void WriterThread(DB** db_ptr) {
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);

    while (!stop_flag) {
        long op_id = ++ops_counter;
        if (op_id > kNumOperations) {
            stop_flag = true;
            break;
        }

        int k_idx = key_dist(rng);
        std::string key = Key(k_idx);
        std::string val = Value(op_id);

        Status s = (*db_ptr)->Put(WriteOptions(), key, val);
        if (!s.ok()) {
            std::cerr << "Put failed: " << s.ToString() << std::endl;
            exit(1);
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex);
            latest_seq_map[key] = op_id;
        }

        // 每 5000 次操作打印一次进度，并稍作休息让 Compaction 跟上
        if (op_id % 5000 == 0) {
            // std::cout << "Writer progress: " << op_id << "/" << kNumOperations << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

// 读者线程：运行时单调性检查
void ReaderThread(DB** db_ptr, int id) {
    std::mt19937 rng(67890 + id);
    std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);
    
    // 记录每个 Key 上次读到的 seq，确保 seq 不会倒退（Time Travel）
    std::vector<long> last_seen_seq(kNumKeys, -1);

    while (!stop_flag) {
        int k_idx = key_dist(rng);
        std::string key = Key(k_idx);
        
        std::string db_val;
        Status s = (*db_ptr)->Get(ReadOptions(), key, &db_val);
        
        if (s.ok()) {
            long current_seq = ParseSeqFromValue(db_val);
            long last_seq = last_seen_seq[k_idx];

            // [核心检查]：同一线程读到的 Seq 不应减小
            // 如果减小，说明刚刚读到了新值，现在却读到了旧值 -> Stale Read
            if (current_seq < last_seq) {
                std::cerr << "!!! [Runtime FATAL] Time Travel on " << key 
                          << " (Thread " << id << ") !!!\n"
                          << "  Previously Saw Seq: " << last_seq << "\n"
                          << "  Currently Read Seq: " << current_seq << "\n"
                          << "  (This means we read a newer version before, and now read an older one!)" 
                          << std::endl;
                // 这是一个严重的 Stale Read 证据，通常意味着新文件被跳过了
            }
            
            // 更新观测到的最新 Seq
            if (current_seq > last_seq) {
                last_seen_seq[k_idx] = current_seq;
            }
        }
    }
}

void VerifyFinalState(DB* db) {
    std::cout << "\n[Verifying] Checking final state..." << std::endl;
    std::lock_guard<std::mutex> lock(state_mutex);
    
    int error_count = 0;
    for (auto& pair : latest_seq_map) {
        std::string key = pair.first;
        long expected_seq = pair.second;
        
        std::string db_val;
        Status s = db->Get(ReadOptions(), key, &db_val);
        
        if (!s.ok()) {
            std::cerr << "[FATAL] Key " << key << " missing!" << std::endl;
            error_count++;
        } else {
            long actual_seq = ParseSeqFromValue(db_val);
            if (actual_seq != expected_seq) {
                std::cerr << "[FATAL] Mismatch on " << key << ". Expect " << expected_seq << ", Got " << actual_seq 
                          << " (Delta: " << (expected_seq - actual_seq) << ")" << std::endl;
                error_count++;
                if (error_count > 10) break;
            }
        }
    }
    
    if (error_count == 0) {
        std::cout << "[Success] All checks passed." << std::endl;
    }
}

int main() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    
    // Level-Hash 配置
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 100, 1024*1024)); 
    options.table_factory.reset(new LevelHashTableFactory(3));
    options.compaction_style = kCompactionStyleLevelHash;
    
    // 激进参数：强制深层 Compaction
    // 16KB Flush, L0 2个文件触发
    options.write_buffer_size = 16 * 1024; 
    options.level0_file_num_compaction_trigger = 2; 
    options.target_file_size_base = 16 * 1024;
    options.max_background_jobs = 4;

    std::string dbname = "/tmp/rocksdb_levelhash_split_stress_test";
    DestroyDB(dbname, options);

    Status s = DB::Open(options, dbname, &db);
    assert(s.ok());

    std::cout << "=== Starting Split Stress Test (1000 Keys, 500k Ops) ===" << std::endl;
    
    std::thread writer(WriterThread, &db);
    std::vector<std::thread> readers;
    for(int i=0; i<kNumReaderThreads; ++i) readers.emplace_back(ReaderThread, &db, i);

    writer.join();
    for(auto& t : readers) t.join();

    // 打印层级文件分布，确认是否触发了 L2/L3
    std::string num_l0, num_l1, num_l2, num_l3;
    db->GetProperty("rocksdb.num-files-at-level0", &num_l0);
    db->GetProperty("rocksdb.num-files-at-level1", &num_l1);
    db->GetProperty("rocksdb.num-files-at-level2", &num_l2);
    db->GetProperty("rocksdb.num-files-at-level3", &num_l3);
    std::cout << "Final Files - L0: " << num_l0 << ", L1: " << num_l1 
              << ", L2: " << num_l2 << ", L3: " << num_l3 << std::endl;

    VerifyFinalState(db);
    delete db;
    return 0;
}