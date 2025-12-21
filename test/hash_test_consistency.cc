#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <algorithm>
#include <cstring>
#include <random>

// ==========================================
// 1. 引入 XXHash (Header-Only 模式)
// ==========================================
#define XXH_INLINE_ALL 
#include "util/xxhash.h"

// ==========================================
// 2. 引入 RocksDB 原生 MurmurHash
// ==========================================
#include "util/murmurhash.h"
#include "rocksdb/slice.h" 

// ==========================================
// 3. Level-Hash 辅助函数
// ==========================================
inline uint64_t ReverseBits64(uint64_t x) {
    // [修复 1] 移除 __builtin_bitreverse64，使用通用的位运算实现，保证兼容性
    x = ((x & 0x5555555555555555ULL) << 1) | ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
    x = ((x & 0x3333333333333333ULL) << 2) | ((x & 0xCCCCCCCCCCCCCCCCULL) >> 2);
    x = ((x & 0x0F0F0F0F0F0F0F0FULL) << 4) | ((x & 0xF0F0F0F0F0F0F0F0ULL) >> 4);
    x = ((x & 0x00FF00FF00FF00FFULL) << 8) | ((x & 0xFF00FF00FF00FF00ULL) >> 8);
    x = ((x & 0x0000FFFF0000FFFFULL) << 16) | ((x & 0xFFFF0000FFFF0000ULL) >> 16);
    return (x << 32) | (x >> 32);
}

// 模拟你的 GetBucketIndex 逻辑 (使用 rocksdb 原生 MurmurHash)
uint32_t GetBucketIndex_Murmur(const std::string& key, uint32_t G) {
    uint64_t h = MurmurHash64A(key.data(), static_cast<int>(key.size()), 0);
    return static_cast<uint32_t>(ReverseBits64(h) >> (64 - G));
}

uint32_t GetBucketIndex_XXH3(const std::string& key, uint32_t G) {
    static const uint64_t kSeed = 0x9e3779b97f4a7c15ULL;
    
    // [修复 2] 去掉 ROCKSDB_ 前缀，直接使用标准函数名
    // 如果你的 util/xxhash.h 是 RocksDB 修改版，它可能会在内部自动定义 ROCKSDB_
    // 但既然编译器报错提示 "did you mean XXH3...", 说明它看到的是没有前缀的版本
    uint64_t h = XXH3_64bits_withSeed(key.data(), key.size(), kSeed);
    
    // Avalanche Mixer (防御性混淆)
    // h ^= h >> 33;
    // h *= 0xff51afd7ed558ccdULL;
    // h ^= h >> 33;
    // h *= 0xc4ceb9fe1a85ec53ULL;
    // h ^= h >> 33;

    return static_cast<uint32_t>(ReverseBits64(h) >> (64 - G));
}

// ==========================================
// 4. 测试逻辑
// ==========================================

// A. 性能测试 (Func Pointer 适配)
typedef uint64_t (*HashFunc)(const void*, int, unsigned int);

void BenchSpeed(const std::string& name, int key_len, size_t count, HashFunc func) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string k(key_len, 'x');
        if (key_len >= 4) *(int*)k.data() = i; 
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

// 包装器，适配函数签名
// [修复 2] 同样去掉 ROCKSDB_ 前缀
uint64_t WrapXXH3(const void* k, int l, unsigned int s) { 
    return XXH3_64bits_withSeed(k, l, (uint64_t)s); 
}

// B. 分布测试
void BenchDistribution(const std::string& name, uint32_t G, size_t count, 
                       uint32_t (*bucket_func)(const std::string&, uint32_t)) {
    uint32_t num_buckets = 1 << G;
    std::vector<int> buckets(num_buckets, 0);

    // 使用连续整数作为 Key (最容易暴露分布问题)
    for (size_t i = 0; i < count; ++i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "user_id_%09zu", i);
        std::string key = buf;
        
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
    std::cout << "=== Level-Hash Benchmark Tool ===\n\n";

    // 1. 性能测试
    BenchSpeed("MurmurHash64A", 16, 10000000, MurmurHash64A);
    BenchSpeed("XXH3_64bits",   16, 10000000, WrapXXH3);
    std::cout << "------------------------------------------------\n";
    BenchSpeed("MurmurHash64A", 64, 5000000, MurmurHash64A);
    BenchSpeed("XXH3_64bits",   64, 5000000, WrapXXH3);
    std::cout << "------------------------------------------------\n";
    BenchSpeed("MurmurHash64A", 256, 2000000, MurmurHash64A);
    BenchSpeed("XXH3_64bits",   256, 2000000, WrapXXH3);

    // 2. 分布测试
    BenchDistribution("MurmurHash64A", 10, 1000000, GetBucketIndex_Murmur);
    BenchDistribution("XXH3_64bits",   10, 1000000, GetBucketIndex_XXH3);

    return 0;
}