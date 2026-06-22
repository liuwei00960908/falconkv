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
    client->SetHixlReadConfig(hixl_read_enabled_,
                              hixl_transport_config_,
                              hixl_receive_chunk_size_mb_,
                              hixl_receive_chunk_count_,
                              hixl_min_read_size_bytes_,
                              hixl_transfer_timeout_ms_,
                              hixl_fallback_to_brpc_);

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

void StoreRpcClientManager::SetHixlReadConfig(bool enabled,
                                              const HixlTransportConfig& transport_config,
                                              uint32_t receive_chunk_size_mb,
                                              uint32_t receive_chunk_count,
                                              uint32_t min_read_size_bytes,
                                              uint32_t transfer_timeout_ms,
                                              bool fallback_to_brpc) {
    hixl_read_enabled_ = enabled;
    hixl_transport_config_ = transport_config;
    hixl_receive_chunk_size_mb_ = receive_chunk_size_mb;
    hixl_receive_chunk_count_ = receive_chunk_count;
    hixl_min_read_size_bytes_ = min_read_size_bytes;
    hixl_transfer_timeout_ms_ = std::max<uint32_t>(1, transfer_timeout_ms);
    hixl_fallback_to_brpc_ = fallback_to_brpc;
}

void StoreRpcClientManager::CloseAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    clients_.clear();
}

} // namespace falconkv
