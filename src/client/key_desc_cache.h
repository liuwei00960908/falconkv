#pragma once
#include <string>
#include <vector>
#include <list>
#include <unordered_map>
#include <mutex>
#include <optional>
#include <utility>
#include "src/common/types.h"

namespace falconkv {

struct KeyDescriptor {
    const std::string key;
    uint32_t store_id = 0;
    uint64_t offset = 0;
    uint32_t size = 0;
    uint64_t access_time_ms = 0;
    std::string store_addr;
    std::string hixl_engine_addr;
    AccessType access_type = AccessType::ACCESS_REMOTE_RPC;

    KeyDescriptor() = default;
    explicit KeyDescriptor(std::string k) : key(std::move(k)) {}
};

class KeyDescCache {
public:
    explicit KeyDescCache(size_t capacity = 100000);
    ~KeyDescCache() = default;

    std::optional<KeyDescriptor> Lookup(const std::string& key);
    int BatchLookup(const std::vector<std::string>& keys,
                    std::vector<KeyDescriptor>& hit_descs,
                    std::vector<std::string>& missing_keys);
    void Insert(const std::string& key, const KeyDescriptor& desc);
    void BatchInsert(const std::vector<std::pair<std::string, KeyDescriptor>>& items);
    void Invalidate(const std::string& key);
    void BatchInvalidate(const std::vector<std::string>& keys);
    size_t Size() const;
    void Clear();

private:
    using ListIter = std::list<KeyDescriptor>::iterator;
    void EvictIfNeeded();

    mutable std::mutex mutex_;
    std::list<KeyDescriptor> lru_list_;
    std::unordered_map<std::string_view, ListIter> cache_;
    size_t capacity_;
};

} // namespace falconkv
