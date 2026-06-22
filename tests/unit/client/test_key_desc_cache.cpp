#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <thread>
#include <atomic>

#include "src/client/key_desc_cache.h"

using namespace falconkv;

namespace {

KeyDescriptor MakeDesc(const std::string& key, uint32_t store_id = 1,
                       uint64_t offset = 0, uint32_t size = 4096) {
    KeyDescriptor desc(key);
    desc.store_id = store_id;
    desc.offset = offset;
    desc.size = size;
    desc.access_type = AccessType::ACCESS_LOCAL_DIRECT;
    return desc;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Lookup hit and miss
// ---------------------------------------------------------------------------
TEST(KeyDescCache, LookupHit) {
    KeyDescCache cache(100);
    cache.Insert("key1", MakeDesc("key1", 1, 0, 100));

    auto result = cache.Lookup("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->key, "key1");
    EXPECT_EQ(result->store_id, 1u);
    EXPECT_EQ(result->offset, 0u);
    EXPECT_EQ(result->size, 100u);
}

TEST(KeyDescCache, PreservesHixlEngineAddrOnInsertAndUpdate) {
    KeyDescCache cache(100);
    auto desc = MakeDesc("key1", 1, 0, 100);
    desc.hixl_engine_addr = "7.150.5.81:16000";
    cache.Insert("key1", desc);

    auto result = cache.Lookup("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->hixl_engine_addr, "7.150.5.81:16000");

    desc.hixl_engine_addr = "7.150.5.81:16001";
    cache.Insert("key1", desc);

    auto updated = cache.Lookup("key1");
    ASSERT_TRUE(updated.has_value());
    EXPECT_EQ(updated->hixl_engine_addr, "7.150.5.81:16001");
}

TEST(KeyDescCache, LookupMiss) {
    KeyDescCache cache(100);
    auto result = cache.Lookup("nonexistent");
    EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// BatchLookup with partial hits
// ---------------------------------------------------------------------------
TEST(KeyDescCache, BatchLookupPartialHits) {
    KeyDescCache cache(100);
    cache.Insert("k1", MakeDesc("k1"));
    cache.Insert("k3", MakeDesc("k3"));

    std::vector<std::string> keys = {"k1", "k2", "k3"};
    std::vector<KeyDescriptor> hit_descs;
    std::vector<std::string> missing_keys;

    int hit_count = cache.BatchLookup(keys, hit_descs, missing_keys);

    EXPECT_EQ(hit_count, 2);
    EXPECT_EQ(hit_descs.size(), 2u);
    EXPECT_EQ(missing_keys.size(), 1u);
    EXPECT_EQ(missing_keys[0], "k2");
}

TEST(KeyDescCache, BatchLookupAllHits) {
    KeyDescCache cache(100);
    cache.Insert("a", MakeDesc("a"));
    cache.Insert("b", MakeDesc("b"));

    std::vector<std::string> keys = {"a", "b"};
    std::vector<KeyDescriptor> hit_descs;
    std::vector<std::string> missing_keys;

    int hit_count = cache.BatchLookup(keys, hit_descs, missing_keys);

    EXPECT_EQ(hit_count, 2);
    EXPECT_TRUE(missing_keys.empty());
}

TEST(KeyDescCache, BatchLookupAllMiss) {
    KeyDescCache cache(100);

    std::vector<std::string> keys = {"x", "y", "z"};
    std::vector<KeyDescriptor> hit_descs;
    std::vector<std::string> missing_keys;

    int hit_count = cache.BatchLookup(keys, hit_descs, missing_keys);

    EXPECT_EQ(hit_count, 0);
    EXPECT_EQ(missing_keys.size(), 3u);
    EXPECT_TRUE(hit_descs.empty());
}

// ---------------------------------------------------------------------------
// BatchInsert and verify
// ---------------------------------------------------------------------------
TEST(KeyDescCache, BatchInsertAndVerify) {
    KeyDescCache cache(100);

    std::vector<std::pair<std::string, KeyDescriptor>> items;
    for (int i = 0; i < 10; ++i) {
        std::string key = "batch_key_" + std::to_string(i);
        items.emplace_back(key, MakeDesc(key, i, i * 4096, 4096));
    }

    cache.BatchInsert(items);

    EXPECT_EQ(cache.Size(), 10u);

    for (int i = 0; i < 10; ++i) {
        std::string key = "batch_key_" + std::to_string(i);
        auto result = cache.Lookup(key);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->store_id, static_cast<uint32_t>(i));
        EXPECT_EQ(result->offset, static_cast<uint64_t>(i) * 4096);
    }
}

TEST(KeyDescCache, BatchInsertPreservesHixlEngineAddr) {
    KeyDescCache cache(100);

    auto first = MakeDesc("k1", 1, 0, 4096);
    first.hixl_engine_addr = "7.150.5.81:16000";
    auto second = MakeDesc("k2", 2, 4096, 4096);
    second.hixl_engine_addr = "7.150.5.81:16001";
    cache.BatchInsert({{"k1", first}, {"k2", second}});

    auto result = cache.Lookup("k1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->hixl_engine_addr, "7.150.5.81:16000");

    first.hixl_engine_addr = "7.150.5.81:17000";
    cache.BatchInsert({{"k1", first}});

    auto updated = cache.Lookup("k1");
    ASSERT_TRUE(updated.has_value());
    EXPECT_EQ(updated->hixl_engine_addr, "7.150.5.81:17000");
}

// ---------------------------------------------------------------------------
// Capacity eviction (LRU)
// ---------------------------------------------------------------------------
TEST(KeyDescCache, CapacityEviction) {
    KeyDescCache cache(3);

    cache.Insert("first", MakeDesc("first", 1));
    cache.Insert("second", MakeDesc("second", 2));
    cache.Insert("third", MakeDesc("third", 3));

    EXPECT_EQ(cache.Size(), 3u);

    // Inserting a fourth item should evict the oldest ("first").
    cache.Insert("fourth", MakeDesc("fourth", 4));

    EXPECT_EQ(cache.Size(), 3u);

    // "first" should have been evicted (LRU).
    EXPECT_FALSE(cache.Lookup("first").has_value());
    EXPECT_TRUE(cache.Lookup("second").has_value());
    EXPECT_TRUE(cache.Lookup("third").has_value());
    EXPECT_TRUE(cache.Lookup("fourth").has_value());
}

TEST(KeyDescCache, LRUEvictionUpdatesOnLookup) {
    KeyDescCache cache(3);

    cache.Insert("a", MakeDesc("a", 1));
    cache.Insert("b", MakeDesc("b", 2));
    cache.Insert("c", MakeDesc("c", 3));

    // Access "a" so it becomes most recently used.
    auto val = cache.Lookup("a");
    ASSERT_TRUE(val.has_value());

    // Inserting "d" should evict "b" (now the oldest), not "a".
    cache.Insert("d", MakeDesc("d", 4));

    EXPECT_TRUE(cache.Lookup("a").has_value());
    EXPECT_FALSE(cache.Lookup("b").has_value());
    EXPECT_TRUE(cache.Lookup("c").has_value());
    EXPECT_TRUE(cache.Lookup("d").has_value());
}

// ---------------------------------------------------------------------------
// BatchInvalidate
// ---------------------------------------------------------------------------
TEST(KeyDescCache, BatchInvalidate) {
    KeyDescCache cache(100);
    cache.Insert("k1", MakeDesc("k1"));
    cache.Insert("k2", MakeDesc("k2"));
    cache.Insert("k3", MakeDesc("k3"));
    cache.Insert("k4", MakeDesc("k4"));

    EXPECT_EQ(cache.Size(), 4u);

    std::vector<std::string> to_remove = {"k1", "k3"};
    cache.BatchInvalidate(to_remove);

    EXPECT_EQ(cache.Size(), 2u);
    EXPECT_FALSE(cache.Lookup("k1").has_value());
    EXPECT_TRUE(cache.Lookup("k2").has_value());
    EXPECT_FALSE(cache.Lookup("k3").has_value());
    EXPECT_TRUE(cache.Lookup("k4").has_value());
}

TEST(KeyDescCache, InvalidateNonexistentKeyNoop) {
    KeyDescCache cache(100);
    cache.Insert("exists", MakeDesc("exists"));

    std::vector<std::string> to_remove = {"noexist1", "noexist2"};
    cache.BatchInvalidate(to_remove);

    EXPECT_EQ(cache.Size(), 1u);
    EXPECT_TRUE(cache.Lookup("exists").has_value());
}

// ---------------------------------------------------------------------------
// Concurrent safety (multi-threaded inserts)
// ---------------------------------------------------------------------------
TEST(KeyDescCache, ConcurrentInserts) {
    KeyDescCache cache(100000);
    const int num_threads = 8;
    const int items_per_thread = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&cache, t, items_per_thread]() {
            for (int i = 0; i < items_per_thread; ++i) {
                std::string key = "thread" + std::to_string(t) + "_key" + std::to_string(i);
                cache.Insert(key, MakeDesc(key, t, i * 100));
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(cache.Size(), static_cast<size_t>(num_threads * items_per_thread));

    // Spot-check a few entries.
    for (int t = 0; t < num_threads; t += 2) {
        for (int i = 0; i < items_per_thread; i += 10) {
            std::string key = "thread" + std::to_string(t) + "_key" + std::to_string(i);
            auto result = cache.Lookup(key);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->store_id, static_cast<uint32_t>(t));
        }
    }
}

// ---------------------------------------------------------------------------
// Clear
// ---------------------------------------------------------------------------
TEST(KeyDescCache, Clear) {
    KeyDescCache cache(100);
    cache.Insert("k1", MakeDesc("k1"));
    cache.Insert("k2", MakeDesc("k2"));
    EXPECT_EQ(cache.Size(), 2u);

    cache.Clear();
    EXPECT_EQ(cache.Size(), 0u);
    EXPECT_FALSE(cache.Lookup("k1").has_value());
}
