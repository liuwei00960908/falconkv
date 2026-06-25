#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>

#include <brpc/channel.h>

#include "falconkv_store.pb.h"
#include "src/common/status.h"
#include "src/store/hixl_buffer_pool.h"
#include "src/store/hixl_transport.h"

namespace falconkv {

/// Client-side RPC wrapper for the Store service.
/// Holds a brpc::Channel and FalconKVStoreService_Stub.
/// Used for remote reads only (writes go through local store).
class StoreRpcClient {
public:
    StoreRpcClient();
    ~StoreRpcClient();

    // Non-copyable
    StoreRpcClient(const StoreRpcClient&) = delete;
    StoreRpcClient& operator=(const StoreRpcClient&) = delete;

    /// Connect to a remote Store server at the given address (host:port).
    /// @param max_body_size_bytes  BRPC max body size in bytes for splitting large batches.
    Status Connect(const std::string& addr,
                   uint64_t max_body_size_bytes = 512ULL * 1024 * 1024,
                   uint32_t max_parallel_sub_batches = 4,
                   bool stream_read_enabled = true,
                   uint32_t stream_read_chunk_size_bytes = 16 * 1024 * 1024,
                   uint32_t stream_read_prefetch_chunks = 4,
                   uint32_t stream_read_queue_chunks = 4);

    /// Configure optional HiXL remote-read probing. BRPC remains the fallback.
    void SetHixlReadConfig(bool enabled,
                           const HixlTransportConfig& transport_config,
                           uint32_t receive_chunk_size_mb,
                           uint32_t receive_chunk_count,
                           uint32_t min_read_size_bytes,
                           uint32_t transfer_timeout_ms,
                           bool fallback_to_brpc);

    /// Whether the client is connected.
    bool IsConnected() const { return connected_; }

    /// Read data from the remote store.
    /// @param client_id  Caller's node ID (forwarded to remote store for scheduler stats).
    /// @param source_node_addr  Caller's store address (forwarded to remote store).
    Status Read(uint64_t offset, void* buffer, uint32_t size,
                uint32_t client_id = 0,
                const std::string& source_node_addr = "");

    /// Batch read multiple segments from the remote store in a single RPC.
    /// @param offsets  Segment offsets.
    /// @param sizes    Segment sizes.
    /// @param buffers  Pre-allocated output buffers (one per segment).
    /// @param results  Output: bytes read per segment (-1 on failure).
    /// @param source_node_addr  Caller's store address (forwarded to remote store).
    /// @return OK if the RPC itself succeeded; individual failures are in results.
    Status BatchRead(const std::vector<uint64_t>& offsets,
                     const std::vector<uint32_t>& sizes,
                     const std::vector<void*>& buffers,
                     std::vector<int32_t>& results,
                     const std::string& source_node_addr = "",
                     const std::string& hixl_engine_addr = "");

    /// Ping the remote store.
    Status Ping();

private:
    struct HixlChunk {
        std::vector<size_t> indices;
        uint64_t total_size = 0;
    };

    struct PreparedHixlChunk {
        HixlChunk chunk;
        std::string token;
        std::string remote_engine;
        std::vector<uint64_t> remote_addrs;
        std::vector<uint32_t> sizes;
    };

    Status BatchReadStream(const std::vector<uint64_t>& offsets,
                             const std::vector<uint32_t>& sizes,
                             const std::vector<void*>& buffers,
                             std::vector<int32_t>& results,
                             const std::string& source_node_addr);

    Status BatchReadHixl(const std::vector<uint64_t>& offsets,
                         const std::vector<uint32_t>& sizes,
                         const std::vector<void*>& buffers,
                         std::vector<int32_t>& results,
                         const std::string& source_node_addr,
                         const std::string& hixl_engine_addr);
    std::vector<HixlChunk> BuildHixlChunks(
        const std::vector<uint32_t>& sizes) const;
    Status PrepareHixlChunk(const HixlChunk& chunk,
                            const std::vector<uint64_t>& offsets,
                            const std::vector<uint32_t>& sizes,
                            const std::string& source_node_addr,
                            const std::string& fallback_remote_engine,
                            PreparedHixlChunk* prepared);
    Status TransferPreparedHixlChunk(const PreparedHixlChunk& prepared,
                                     const std::vector<void*>& buffers,
                                     std::vector<int32_t>& results);
    void ReleaseHixlReadTokenBestEffort(const std::string& token);
    Status EnsureHixlReceivePool();

    brpc::Channel channel_;
    std::unique_ptr<FalconKVStoreService_Stub> stub_;
    bool connected_ = false;
    uint64_t max_body_size_bytes_ = 512ULL * 1024 * 1024;
    uint32_t max_parallel_sub_batches_ = 4;
    bool stream_read_enabled_ = true;
    uint32_t stream_read_chunk_size_bytes_ = 16 * 1024 * 1024;
    uint32_t stream_read_prefetch_chunks_ = 4;
    uint32_t stream_read_queue_chunks_ = 4;
    bool hixl_read_enabled_ = false;
    HixlTransportConfig hixl_transport_config_;
    uint32_t hixl_receive_chunk_size_mb_ = 16;
    uint32_t hixl_receive_chunk_count_ = 16;
    uint32_t hixl_min_read_size_bytes_ = 1024 * 1024;
    uint32_t hixl_transfer_timeout_ms_ = 5000;
    bool hixl_fallback_to_brpc_ = true;
    std::mutex hixl_init_mutex_;
    std::unique_ptr<HixlTransport> hixl_transport_;
    std::unique_ptr<HixlBufferPool> hixl_receive_pool_;
};

} // namespace falconkv
