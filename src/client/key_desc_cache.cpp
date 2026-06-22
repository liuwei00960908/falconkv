#include "client/key_desc_cache.h"

#include <chrono>
#include <algorithm>

namespace falconkv {

KeyDescCache::KeyDescCache(size_t capacity)
    : capacity_(capacity) {}

std::optional<KeyDescriptor> KeyDescCache::Lookup(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(std::string_view(key));
    if (it == cache_.end()) {
        return std::nullopt;
    }
    // Move to back of LRU list (most recently used) via O(1) splice
    lru_list_.splice(lru_list_.end(), lru_list_, it->second);
    // Update access time
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    it->second->access_time_ms = static_cast<uint64_t>(ms);
    return *(it->second);
}

int KeyDescCache::BatchLookup(const std::vector<std::string>& keys,
                               std::vector<KeyDescriptor>& hit_descs,
                               std::vector<std::string>& missing_keys) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    hit_descs.clear();
    missing_keys.clear();

    for (const auto& key : keys) {
        auto it = cache_.find(std::string_view(key));
        if (it != cache_.end()) {
            // Move to back of LRU list via O(1) splice
            lru_list_.splice(lru_list_.end(), lru_list_, it->second);
            // Update access time
            it->second->access_time_ms = static_cast<uint64_t>(ms);
            hit_descs.push_back(*(it->second));
        } else {
            missing_keys.push_back(key);
        }
    }

    return static_cast<int>(hit_descs.size());
}

void KeyDescCache::Insert(const std::string& key, const KeyDescriptor& desc) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(std::string_view(key));
    if (it != cache_.end()) {
        // Key already exists: update fields (skip const key) + splice to back
        auto& existing = *(it->second);
        existing.store_id = desc.store_id;
        existing.offset = desc.offset;
        existing.size = desc.size;
        existing.access_time_ms = desc.access_time_ms;
        existing.store_addr = desc.store_addr;
        existing.hixl_engine_addr = desc.hixl_engine_addr;
        existing.access_type = desc.access_type;
        lru_list_.splice(lru_list_.end(), lru_list_, it->second);
    } else {
        EvictIfNeeded();
        // Construct with key, then copy remaining fields
        lru_list_.emplace_back(KeyDescriptor(key));
        auto list_it = std::prev(lru_list_.end());
        list_it->store_id = desc.store_id;
        list_it->offset = desc.offset;
        list_it->size = desc.size;
        list_it->access_time_ms = desc.access_time_ms;
        list_it->store_addr = desc.store_addr;
        list_it->hixl_engine_addr = desc.hixl_engine_addr;
        list_it->access_type = desc.access_type;
        cache_.emplace(std::string_view(list_it->key), list_it);
    }
}

void KeyDescCache::BatchInsert(
    const std::vector<std::pair<std::string, KeyDescriptor>>& items) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, desc] : items) {
        auto it = cache_.find(std::string_view(key));
        if (it != cache_.end()) {
            // Update fields (skip const key) + splice to back
            auto& existing = *(it->second);
            existing.store_id = desc.store_id;
            existing.offset = desc.offset;
            existing.size = desc.size;
            existing.access_time_ms = desc.access_time_ms;
            existing.store_addr = desc.store_addr;
            existing.hixl_engine_addr = desc.hixl_engine_addr;
            existing.access_type = desc.access_type;
            lru_list_.splice(lru_list_.end(), lru_list_, it->second);
        } else {
            EvictIfNeeded();
            lru_list_.emplace_back(KeyDescriptor(key));
            auto list_it = std::prev(lru_list_.end());
            list_it->store_id = desc.store_id;
            list_it->offset = desc.offset;
            list_it->size = desc.size;
            list_it->access_time_ms = desc.access_time_ms;
            list_it->store_addr = desc.store_addr;
            list_it->hixl_engine_addr = desc.hixl_engine_addr;
            list_it->access_type = desc.access_type;
            cache_.emplace(std::string_view(list_it->key), list_it);
        }
    }
}

void KeyDescCache::Invalidate(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(std::string_view(key));
    if (it != cache_.end()) {
        lru_list_.erase(it->second);
        cache_.erase(it);
    }
}

void KeyDescCache::BatchInvalidate(const std::vector<std::string>& keys) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& key : keys) {
        auto it = cache_.find(std::string_view(key));
        if (it != cache_.end()) {
            lru_list_.erase(it->second);
            cache_.erase(it);
        }
    }
}

size_t KeyDescCache::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.size();
}

void KeyDescCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
    lru_list_.clear();
}

void KeyDescCache::EvictIfNeeded() {
    // Caller must hold mutex_
    while (cache_.size() >= capacity_ && !lru_list_.empty()) {
        cache_.erase(std::string_view(lru_list_.front().key));
        lru_list_.pop_front();
    }
}

} // namespace falconkv
