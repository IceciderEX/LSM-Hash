#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <algorithm>
#include <cstring>
#include <random>
#define XXH_INLINE_ALL 
#include "util/xxhash.h"

#include "util/murmurhash.h"
#include "rocksdb/slice.h" 

inline uint64_t ReverseBits64(uint64_t x) {
    x = ((x & 0x5555555555555555ULL) << 1) | ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
    x = ((x & 0x3333333333333333ULL) << 2) | ((x & 0xCCCCCCCCCCCCCCCCULL) >> 2);
    x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x & 0xF0F0F0F0F0F0F0F0ULL) >> 4);
    x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x & 0xFF00FF00FF00FF00ULL) >> 8);
    x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x & 0xFFFF0000FFFF0000ULL) >> 16);
    return (x << 32) | (x >> 32);
}

// GetBucketIndex 逻辑 (使用 rocksdb 原生 MurmurHash)
uint32_t GetBucketIndex_Murmur(const std::string& key, uint32_t G) {
    uint64_t h = MurmurHash64A(key.data(), static_cast<int>(key.size()), 0);
    return static_cast<uint32_t>(ReverseBits64(h) >> (64 - G));
}

uint32_t GetBucketIndex_XXH3(const std::string& key, uint32_t G) {
    static const uint64_t kSeed = 0x9e3779b97f4a7c15ULL;
    uint64_t h = XXH3_64bits_withSeed(key.data(), key.size(), kSeed);
    return static_cast<uint32_t>(ReverseBits64(h) >> (64 - G));
}

// 模拟 Index 表 Key 的前缀结构: dbid(4B) + tableid(4B)
const uint32_t kTestDbId = 1001;
const uint32_t kTestTableId = 50000;

// A. 性能测试 (Func Pointer 适配)
typedef uint64_t (*HashFunc)(const void*, int, unsigned int);

void BenchSpeed(const std::string& name, int key_len, size_t count, HashFunc func) {
    std::vector<std::string> keys;
    keys.reserve(count);
    
    // 构造 keys: 模拟 Index 表格式
    for (size_t i = 0; i < count; ++i) {
        std::string k;
        k.resize(key_len, 'x'); // 预分配并填充默认值
        
        // 只有当 Key 长度足够容纳 Header (8字节) 时才填充真实结构
        if (key_len >= 8) {
            // 1. 填入 DBID (0-3 字节)
            memcpy(&k[0], &kTestDbId, sizeof(uint32_t));
            // 2. 填入 TableID (4-7 字节)
            memcpy(&k[4], &kTestTableId, sizeof(uint32_t));
            
            // 3. 填入 PK (8+ 字节)
            if (key_len >= 12) {
                 *(int*)(&k[8]) = i; 
            }
        } else {
             if (key_len >= 4) *(int*)k.data() = i; 
        }
        
        keys.push_back(std::move(k));
    }

    auto start = std::chrono::high_resolution_clock::now();
    
    volatile uint64_t sink = 0; 
    for (size_t i = 0; i < count; ++i) {
        sink = func(keys[i].data(), static_cast<int>(keys[i].size()), 0);
    }

    (void)sink;

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    
    double ops = count / diff.count();
    double mbs = (count * key_len) / diff.count() / 1024 / 1024;

    std::cout << std::left << std::setw(20) << name 
              << " | KeyLen: " << std::setw(3) << key_len 
              << " | " << std::fixed << std::setprecision(2) << ops/1000000.0 << " M ops/s"
              << " | " << mbs << " MB/s" << std::endl;
}

uint64_t WrapXXH3(const void* k, int l, unsigned int s) { 
    return XXH3_64bits_withSeed(k, l, (uint64_t)s); 
}

// B. 分布测试
void BenchDistribution(const std::string& name, uint32_t G, size_t count, 
                       uint32_t (*bucket_func)(const std::string&, uint32_t)) {
    uint32_t num_buckets = 1 << G;
    std::vector<int> buckets(num_buckets, 0);

    // 构造 Index 表 Key: [DBID][TableID][PK_String]
    for (size_t i = 0; i < count; ++i) {
        // 构造 PK 部分 (字符串形式)
        char pk_buf[32];
        snprintf(pk_buf, sizeof(pk_buf), "user_id_%09zu", i);
        size_t pk_len = strlen(pk_buf);

        // 构造完整 Key
        std::string key;
        key.resize(sizeof(uint32_t) * 2 + pk_len);
        
        // 填入 dbid_tableid
        memcpy(&key[0], &kTestDbId, sizeof(uint32_t));
        memcpy(&key[4], &kTestTableId, sizeof(uint32_t));
        // 填入 pk
        memcpy(&key[8], pk_buf, pk_len);
        
        buckets[bucket_func(key, G)]++;
    }

    // 统计
    int min_load = count, max_load = 0;
    double sum = 0;
    for (int c : buckets) {
        if (c < min_load) min_load = c;
        if (c > max_load) max_load = c;
        sum += c;
    }
    double avg = sum / num_buckets;

    double variance = 0;
    for (int c : buckets) {
        variance += (c - avg) * (c - avg);
    }
    variance /= num_buckets;
    double std_dev = std::sqrt(variance);

    std::cout << "\n[" << name << " Distribution] (G=" << G << ", Keys=" << count << ")\n";
    std::cout << "  Key Format: [DBID:4B][TableID:4B][PK_String]\n";
    std::cout << "  Avg Load: " << avg << "\n";
    std::cout << "  Max Load: " << max_load << " (Ideal: ~" << (int)avg << ")\n";
    std::cout << "  Min Load: " << min_load << "\n";
    std::cout << "  Std Dev : " << std_dev << " (Lower is better)\n";
    std::cout << "  Load Factor Skew: " << (max_load / avg) << "x\n";
    
    if (max_load > avg * 2.0) {
        std::cout << "  -> \033[1;31mWARNING: High skew detected!\033[0m\n";
    } else {
        std::cout << "  -> \033[1;32mPASS: Distribution is uniform.\033[0m\n";
    }
}

int main() {
    std::cout << "=== Level-Hash Benchmark Tool (Index Table Key Format) ===\n\n";

    BenchSpeed("MurmurHash64A", 16, 10000000, MurmurHash64A);
    BenchSpeed("XXH3_64bits",   16, 10000000, WrapXXH3);
    std::cout << "------------------------------------------------\n";
    BenchSpeed("MurmurHash64A", 64, 5000000, MurmurHash64A);
    BenchSpeed("XXH3_64bits",   64, 5000000, WrapXXH3);
    std::cout << "------------------------------------------------\n";
    BenchSpeed("MurmurHash64A", 128, 2000000, MurmurHash64A);
    BenchSpeed("XXH3_64bits",   128, 2000000, WrapXXH3);

    BenchDistribution("MurmurHash64A", 3, 10000000, GetBucketIndex_Murmur);
    BenchDistribution("XXH3_64bits",   3, 10000000, GetBucketIndex_XXH3);

    return 0;
}