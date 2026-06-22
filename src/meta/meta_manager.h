#pragma once
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <cstdint>
#include <functional>

#include "src/common/status.h"

namespace falconkv {

struct KeyRecord {
    std::string key;
    uint32_t store_id = 0;
    uint64_t offset = 0;
    uint32_t size = 0;
    int stat = 0;                 // 0 = start (allocated but not committed),
                                  // 1 = done (committed / visible),
                                  // 2 = evict (marked for eviction)
    uint64_t access_time_ms = 0;
    uint64_t create_time_ms = 0;
    std::string client_id;
    uint32_t node_id = 0;
    std::string data_file;       // same-node DirectIO path
    std::string store_addr;      // remote RPC routing address
    std::string hixl_engine_addr;
};

struct StoreInfo {
    uint32_t store_id = 0;
    uint32_t node_id = 0;
    std::string store_addr;   // remote RPC routing
    std::string data_file;    // same-node DirectIO path
    std::string hixl_engine_addr;
};

class MetaManager {
public:
    explicit MetaManager(size_t shard_count = 64);
    ~MetaManager() = default;

    // Non-copyable
    MetaManager(const MetaManager&) = delete;
    MetaManager& operator=(const MetaManager&) = delete;

    /// Register a new store with the metadata manager.
    Status RegisterStore(const StoreInfo& info);

    /// Check existence of a batch of keys.  Returns records where stat == 1.
    std::vector<KeyRecord> BatchExist(const std::vector<std::string>& keys);

    /// Look up a batch of keys.  Returns records regardless of stat (but not
    /// evicted entries).
    std::vector<KeyRecord> BatchLookup(const std::vector<std::string>& keys);

    /// Accept committed key metadata pushed from a Store via SyncCommit.
    /// store_id identifies the originating store; records contain key metadata
    /// that the Store has already committed locally.
    Status SyncCommit(uint32_t store_id, const std::vector<KeyRecord>& records);

    /// Accept removed/evicted key metadata pushed from a Store via SyncRemove.
    Status SyncRemove(uint32_t store_id, const std::vector<std::string>& keys);

private:
    /// Shard for a given key.
    size_t ShardOf(const std::string& key) const;

    /// Helper to return the current time in milliseconds (steady clock).
    static uint64_t NowMs();

    /// Look up the node_id for a given store_id. Returns 0 if not found.
    uint32_t GetStoreNodeId(uint32_t store_id) const;

    /// Look up the data_file path for a given store_id. Returns "" if not found.
    std::string GetStoreDataFile(uint32_t store_id) const;

    /// Look up the store_addr (RPC address) for a given store_id. Returns "" if not found.
    std::string GetStoreAddr(uint32_t store_id) const;

    /// Look up the HiXL engine address for a given store_id. Returns "" if not found.
    std::string GetStoreHixlEngineAddr(uint32_t store_id) const;

    struct Shard {
        std::shared_mutex rwlock;
        std::unordered_map<std::string, KeyRecord> key_records;
    };

    size_t shard_count_;
    std::vector<Shard> shards_;

    mutable std::shared_mutex stores_rwlock_;
    std::unordered_map<uint32_t, StoreInfo> stores_;
};

} // namespace falconkv
