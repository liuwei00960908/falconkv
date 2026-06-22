#include "src/meta/meta_manager.h"

#include <chrono>
#include <mutex>
#include <algorithm>

#include "src/common/logging.h"

namespace falconkv {

// ---------- helpers ----------

uint64_t MetaManager::NowMs() {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch())
                  .count();
    return static_cast<uint64_t>(ms);
}

size_t MetaManager::ShardOf(const std::string& key) const {
    return std::hash<std::string>{}(key) % shard_count_;
}

uint32_t MetaManager::GetStoreNodeId(uint32_t store_id) const {
    std::shared_lock<std::shared_mutex> lock(stores_rwlock_);
    auto it = stores_.find(store_id);
    return (it != stores_.end()) ? it->second.node_id : 0;
}

std::string MetaManager::GetStoreDataFile(uint32_t store_id) const {
    std::shared_lock<std::shared_mutex> lock(stores_rwlock_);
    auto it = stores_.find(store_id);
    return (it != stores_.end()) ? it->second.data_file : "";
}

std::string MetaManager::GetStoreAddr(uint32_t store_id) const {
    std::shared_lock<std::shared_mutex> lock(stores_rwlock_);
    auto it = stores_.find(store_id);
    return (it != stores_.end()) ? it->second.store_addr : "";
}

std::string MetaManager::GetStoreHixlEngineAddr(uint32_t store_id) const {
    std::shared_lock<std::shared_mutex> lock(stores_rwlock_);
    auto it = stores_.find(store_id);
    return (it != stores_.end()) ? it->second.hixl_engine_addr : "";
}

// ---------- public ----------

MetaManager::MetaManager(size_t shard_count)
    : shard_count_(shard_count),
      shards_(shard_count) {}

Status MetaManager::RegisterStore(const StoreInfo& info) {
    if (info.store_id == 0) {
        return Status::InvalidArg("store_id must be non-zero");
    }
    std::unique_lock<std::shared_mutex> lock(stores_rwlock_);
    stores_[info.store_id] = info;
    LOG(INFO) << "[MetaManager] RegisterStore: store_id=" << info.store_id
              << ", node_id=" << info.node_id << ", addr=" << info.store_addr;
    return Status::OK();
}

std::vector<KeyRecord> MetaManager::BatchExist(
    const std::vector<std::string>& keys) {
    std::vector<KeyRecord> results(keys.size());

    // Group indices by shard for batch processing.
    std::vector<std::vector<size_t>> shard_groups(shard_count_);
    for (size_t i = 0; i < keys.size(); ++i) {
        shard_groups[ShardOf(keys[i])].push_back(i);
    }

    uint64_t now = NowMs();

    for (size_t s = 0; s < shard_count_; ++s) {
        if (shard_groups[s].empty()) continue;
        std::shared_lock<std::shared_mutex> lock(shards_[s].rwlock);
        auto& records = shards_[s].key_records;
        for (size_t idx : shard_groups[s]) {
            auto it = records.find(keys[idx]);
            if (it != records.end() && it->second.stat == 1) {
                it->second.access_time_ms = now;
                results[idx] = it->second;
            }
            // Default-constructed KeyRecord remains for misses.
        }
    }

    // Populate node_id and data_file from registered store info.
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].key.empty()) {
            results[i].node_id = GetStoreNodeId(results[i].store_id);
            results[i].data_file = GetStoreDataFile(results[i].store_id);
            results[i].store_addr = GetStoreAddr(results[i].store_id);
            results[i].hixl_engine_addr = GetStoreHixlEngineAddr(results[i].store_id);
        }
    }

    return results;
}

std::vector<KeyRecord> MetaManager::BatchLookup(
    const std::vector<std::string>& keys) {
    std::vector<KeyRecord> results(keys.size());

    // Group indices by shard for batch processing.
    std::vector<std::vector<size_t>> shard_groups(shard_count_);
    for (size_t i = 0; i < keys.size(); ++i) {
        shard_groups[ShardOf(keys[i])].push_back(i);
    }

    uint64_t now = NowMs();

    for (size_t s = 0; s < shard_count_; ++s) {
        if (shard_groups[s].empty()) continue;
        std::shared_lock<std::shared_mutex> lock(shards_[s].rwlock);
        auto& records = shards_[s].key_records;
        for (size_t idx : shard_groups[s]) {
            auto it = records.find(keys[idx]);
            if (it != records.end() && it->second.stat != 2) {
                it->second.access_time_ms = now;
                results[idx] = it->second;
            }
            // Default-constructed KeyRecord remains for misses.
        }
    }

    // Populate node_id and data_file from registered store info.
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].key.empty()) {
            results[i].node_id = GetStoreNodeId(results[i].store_id);
            results[i].data_file = GetStoreDataFile(results[i].store_id);
            results[i].store_addr = GetStoreAddr(results[i].store_id);
            results[i].hixl_engine_addr = GetStoreHixlEngineAddr(results[i].store_id);
        }
    }

    return results;
}

Status MetaManager::SyncCommit(uint32_t store_id,
                                const std::vector<KeyRecord>& records) {
    // Group keys by shard.
    std::vector<std::vector<size_t>> shard_groups(shard_count_);
    for (size_t i = 0; i < records.size(); ++i) {
        if (!records[i].key.empty()) {
            shard_groups[ShardOf(records[i].key)].push_back(i);
        }
    }

    uint64_t now = NowMs();

    for (size_t s = 0; s < shard_count_; ++s) {
        if (shard_groups[s].empty()) continue;
        std::unique_lock<std::shared_mutex> lock(shards_[s].rwlock);
        auto& shard_records = shards_[s].key_records;
        for (size_t idx : shard_groups[s]) {
            const auto& rec = records[idx];
            KeyRecord& entry = shard_records[rec.key];
            entry.key = rec.key;
            entry.store_id = store_id;
            entry.offset = rec.offset;
            entry.size = rec.size;
            entry.stat = 1; // committed
            entry.access_time_ms = now;
            entry.create_time_ms = (entry.create_time_ms == 0) ? now : entry.create_time_ms;
            entry.client_id = rec.client_id;
        }
    }

    return Status::OK();
}

Status MetaManager::SyncRemove(uint32_t store_id,
                                const std::vector<std::string>& keys) {
    // Group keys by shard.
    std::vector<std::vector<size_t>> shard_groups(shard_count_);
    for (size_t i = 0; i < keys.size(); ++i) {
        shard_groups[ShardOf(keys[i])].push_back(i);
    }

    for (size_t s = 0; s < shard_count_; ++s) {
        if (shard_groups[s].empty()) continue;
        std::unique_lock<std::shared_mutex> lock(shards_[s].rwlock);
        auto& shard_records = shards_[s].key_records;
        for (size_t idx : shard_groups[s]) {
            shard_records.erase(keys[idx]);
        }
    }

    return Status::OK();
}

} // namespace falconkv
