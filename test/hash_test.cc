#include <cassert>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "memtable/level_hash_memtable.h"
#include "table/level_hash/level_hash_table.h"

using namespace ROCKSDB_NAMESPACE;

int main() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    
    // 1. 配置 Level-Hash 组件
    // L0 Bucket 数量 = 2^3 = 8
    options.memtable_factory.reset(new LevelHashMemTableFactory(3, 1000));
    options.table_factory.reset(new LevelHashTableFactory(3));
    
    // 2. 强制 L0 触发 Flush (写少点)
    options.write_buffer_size = 1024 * 1024; // 1MB
    
    Status s = DB::Open(options, "/tmp/rocksdb_level_hash_test", &db);
    assert(s.ok());

    // 3. 写入数据 (Bucket 0 和 Bucket 1 的数据)
    // 假设 MurmurHash 逻辑：
    // "key_b0_1" -> hash ...000
    // "key_b1_1" -> hash ...001
    // 你可能需要预先算好几个 Key
    db->Put(WriteOptions(), "key1", "value1");
    db->Put(WriteOptions(), "key2", "value2");

    // 4. 读取 (MemTable Read)
    std::string value;
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    // 5. 触发 Flush
    s = db->Flush(FlushOptions());
    assert(s.ok());

    // 6. 读取 (SST Read)
    // 这时候数据在 L0 SST 文件中
    value.clear();
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    delete db;
    printf("Stage 1 Passed!\n");
    return 0;
}