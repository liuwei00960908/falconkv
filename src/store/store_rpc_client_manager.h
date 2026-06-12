#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>

#include "src/store/store_rpc_client.h"

namespace falconkv {

/// Connection pool for StoreRpcClient instances, cached by address.
class StoreRpcClientManager {
public:
    StoreRpcClientManager() = default;
    ~StoreRpcClientManager();

    // Non-copyable
    StoreRpcClientManager(const StoreRpcClientManager&) = delete;
    StoreRpcClientManager& operator=(const StoreRpcClientManager&) = delete;

    /// Get or create a StoreRpcClient for the given address.
    /// Returns nullptr if connection fails.
    StoreRpcClient* GetOrCreate(const std::string& addr);

    /// Close all cached clients.
    void CloseAll();

    /// Set the BRPC max body size (in bytes) for all future connections.
    void SetMaxBodySize(uint64_t max_body_size_bytes);

    /// Set the maximum number of split BatchRead RPCs to run in parallel.
    void SetMaxParallelSubBatches(uint32_t max_parallel_sub_batches);

    /// Configure streaming remote reads for future connections.
    void SetStreamReadConfig(bool enabled, uint32_t chunk_size_bytes,
                             uint32_t prefetch_chunks,
                             uint32_t queue_chunks);

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<StoreRpcClient>> clients_;
    uint64_t max_body_size_bytes_ = 512ULL * 1024 * 1024;
    uint32_t max_parallel_sub_batches_ = 4;
    bool stream_read_enabled_ = true;
    uint32_t stream_read_chunk_size_bytes_ = 16 * 1024 * 1024;
    uint32_t stream_read_prefetch_chunks_ = 4;
    uint32_t stream_read_queue_chunks_ = 4;
};

} // namespace falconkv
