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
const int kNumKeys = 1000;        
const int kNumOperations = 300000;  
const int kNumWriterThreads = 2;
const int kNumReaderThreads = 2;    

// 记录 Key 的状态：不仅记录最新的 seq，还记录是否被删除
struct KeyState {
    long seq;
    bool deleted;
};

std::mutex state_mutex;
std::map<std::string, KeyState> global_key_state; // 真值表
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

void WriterThread(DB** db_ptr, int id) {
    std::mt19937 rng(12345 + id);
    std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);
    std::uniform_int_distribution<int> op_dist(0, 99); // 0-99 用于决定操作类型

    while (!stop_flag) {
        long op_id = ++ops_counter;
        if (op_id > kNumOperations) {
            stop_flag = true;
            break;
        }

        int k_idx = key_dist(rng);
        std::string key = Key(k_idx);
        
        // 20% 概率删除，80% 概率写入
        bool is_delete = (op_dist(rng) < 20); 

        Status s;
        if (is_delete) {
            s = (*db_ptr)->Delete(WriteOptions(), key);
        } else {
            std::string val = Value(op_id);
            s = (*db_ptr)->Put(WriteOptions(), key, val);
        }

        if (!s.ok()) {
            std::cerr << "Write failed: " << s.ToString() << std::endl;
            exit(1);
        }

        // 更新真值表
        {
            std::lock_guard<std::mutex> lock(state_mutex);
            global_key_state[key] = {op_id, is_delete};
        }

        // 降低打印频率
        if (op_id % 10000 == 0) {
            std::cout << "Progress: " << op_id << "/" << kNumOperations << " ops..." << std::endl;
        }
        
        // 微小延迟模拟真实负载
        if (op_id % 1000 == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

// 读者线程：运行时单调性检查 + 僵尸数据检测
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
        
        if (s.IsNotFound()) {
            // 读到 NotFound 是合法的（可能被删除了，或者还没写）
            // 注意：我们不要重置 last_seen_seq。
            // 关键逻辑：如果 Key 先被读到 seq=100，然后被删除。
            // 此时读到 NotFound 是对的。
            // 如果之后因为 Bug 读到了旧的 seq=90（僵尸数据复活），
            // 下一次循环或者其他逻辑捕捉到它时，90 < 100 依然成立，能捉住 Bug。
            continue; 
        } else if (!s.ok()) {
            std::cerr << "Read Error: " << s.ToString() << std::endl;
            continue;
        }

        // 读到了有效值
        long current_seq = ParseSeqFromValue(db_val);
        long last_seq = last_seen_seq[k_idx];

        if (current_seq < last_seq) {
            std::cerr << "!!! [Runtime FATAL] Time Travel / Zombie Value Detected on " << key 
                      << " (Thread " << id << ") !!!\n"
                      << "  Previously Saw Seq: " << last_seq << "\n"
                      << "  Currently Read Seq: " << current_seq << "\n"
                      << "  (This means we read a newer version before, and now read an older one!)" 
                      << std::endl;
            // 可选：exit(1) 立即终止
        }
        
        if (current_seq > last_seq) {
            last_seen_seq[k_idx] = current_seq;
        }
    }
}

void VerifyFinalState(DB* db) {
    std::cout << "\n[Verifying] Checking final state with Deletes..." << std::endl;
    std::lock_guard<std::mutex> lock(state_mutex);
    
    int error_count = 0;
    int checked_count = 0;

    for (auto& pair : global_key_state) {
        std::string key = pair.first;
        KeyState expected = pair.second;
        
        std::string db_val;
        Status s = db->Get(ReadOptions(), key, &db_val);
        
        checked_count++;

        if (expected.deleted) {
            // 预期状态：已删除
            if (!s.IsNotFound()) {
                std::cerr << "[FATAL] Key " << key << " should be DELETED, but found value: " << db_val 
                          << " (Expected Tombstone at seq " << expected.seq << ")" << std::endl;
                error_count++;
            }
        } else {
            // 预期状态：存在
            if (s.IsNotFound()) {
                std::cerr << "[FATAL] Key " << key << " MISSING! Expected seq " << expected.seq << std::endl;
                error_count++;
            } else if (!s.ok()) {
                std::cerr << "[FATAL] Key " << key << " Read Error: " << s.ToString() << std::endl;
                error_count++;
            } else {
                long actual_seq = ParseSeqFromValue(db_val);
                if (actual_seq != expected.seq) {
                    std::cerr << "[FATAL] Value Mismatch on " << key << ". Expect " << expected.seq << ", Got " << actual_seq 
                              << " (Delta: " << (expected.seq - actual_seq) << ")" << std::endl;
                    error_count++;
                }
            }
        }
        if (error_count > 20) break;
    }
    
    std::cout << "Checked " << checked_count << " keys." << std::endl;
    if (error_count == 0) {
        std::cout << "[Success] All checks passed (including Deletes)." << std::endl;
    } else {
        std::cout << "[Failed] Found " << error_count << " errors." << std::endl;
    }
}

int main() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    
    // --- Level-Hash 关键配置 ---
    // MemTable: G=3 (8 buckets), Bucket阈值=100, Mem总阈值=1MB
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 100, 1024*1024)); 
    // SST: Initial G=3
    options.table_factory.reset(new LevelHashTableFactory(3));
    // Compaction Picker
    options.compaction_style = kCompactionStyleLevelHash; // 需确认您的代码中定义了这个 Enum 或方式
    
    // --- 激进的 Compaction 参数 ---
    // 强制快速触发 Flush 和 Compaction，以测试数据流转
    options.write_buffer_size = 1024 * 1024 * 4; // 512KB MemTable
    options.level0_file_num_compaction_trigger = 4; 
    options.target_file_size_base = 1024 * 1024 * 4;
    options.max_background_jobs = 4;

    std::string dbname = "/home/wam/HWKV/rocksdb/db_tmp/rocksdb_levelhash_consistency_test";
    
    // 清理旧数据
    Status s = DestroyDB(dbname, options);
    
    s = DB::Open(options, dbname, &db);
    if (!s.ok()) {
        std::cerr << "Open DB failed: " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << "=== Starting Split Stress Test (Mixed Put/Delete) ===" << std::endl;
    std::cout << "Keys: " << kNumKeys << ", Ops: " << kNumOperations << std::endl;
    
    std::vector<std::thread> writers;
    for(int i=0; i<kNumWriterThreads; ++i) writers.emplace_back(WriterThread, &db, i);

    std::vector<std::thread> readers;
    for(int i=0; i<kNumReaderThreads; ++i) readers.emplace_back(ReaderThread, &db, i);

    for(auto& t : writers) t.join();
    for(auto& t : readers) t.join();

    // 打印层级统计
    std::string stats;
    db->GetProperty("rocksdb.levelstats", &stats);
    std::cout << "\nFinal Level Stats:\n" << stats << std::endl;

    VerifyFinalState(db);
    
    delete db;
    return 0;
}