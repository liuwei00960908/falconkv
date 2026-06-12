#include "src/store/store_rpc_client_manager.h"

#include <algorithm>

#include "src/common/logging.h"

namespace falconkv {

StoreRpcClientManager::~StoreRpcClientManager() {
    CloseAll();
}

StoreRpcClient* StoreRpcClientManager::GetOrCreate(const std::string& addr) {
    if (addr.empty()) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = clients_.find(addr);
    if (it != clients_.end() && it->second->IsConnected()) {
        return it->second.get();
    }

    // Create a new client
    auto client = std::make_unique<StoreRpcClient>();
    Status s = client->Connect(addr, max_body_size_bytes_,
                               max_parallel_sub_batches_,
                               stream_read_enabled_,
                               stream_read_chunk_size_bytes_,
                               stream_read_prefetch_chunks_,
                               stream_read_queue_chunks_);
    if (!s.ok()) {
        return nullptr;
    }

    auto* raw = client.get();
    LOG(INFO) << "[StoreRpcClientManager] Created new StoreRpcClient for " << addr;
    clients_[addr] = std::move(client);
    return raw;
}

void StoreRpcClientManager::SetMaxBodySize(uint64_t max_body_size_bytes) {
    max_body_size_bytes_ = max_body_size_bytes;
}

void StoreRpcClientManager::SetMaxParallelSubBatches(
    uint32_t max_parallel_sub_batches) {
    max_parallel_sub_batches_ = std::max<uint32_t>(1, max_parallel_sub_batches);
}

void StoreRpcClientManager::SetStreamReadConfig(bool enabled,
                                                uint32_t chunk_size_bytes,
                                                uint32_t prefetch_chunks,
                                                uint32_t queue_chunks) {
    stream_read_enabled_ = enabled;
    stream_read_chunk_size_bytes_ = std::max<uint32_t>(1, chunk_size_bytes);
    stream_read_prefetch_chunks_ = std::max<uint32_t>(1, prefetch_chunks);
    stream_read_queue_chunks_ = std::max<uint32_t>(1, queue_chunks);
}

void StoreRpcClientManager::CloseAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    clients_.clear();
}

} // namespace falconkv
