#pragma once

#include <string>
#include <vector>
#include <memory>

#include <brpc/channel.h>

#include "falconkv_store.pb.h"
#include "src/common/status.h"

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
                   uint32_t stream_read_chunk_size_bytes = 16 * 1024 * 1024);

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
                     const std::string& source_node_addr = "");

    /// Ping the remote store.
    Status Ping();

private:
    Status BatchReadStream(const std::vector<uint64_t>& offsets,
                           const std::vector<uint32_t>& sizes,
                           const std::vector<void*>& buffers,
                           std::vector<int32_t>& results,
                           const std::string& source_node_addr);

    brpc::Channel channel_;
    std::unique_ptr<FalconKVStoreService_Stub> stub_;
    bool connected_ = false;
    uint64_t max_body_size_bytes_ = 512ULL * 1024 * 1024;
    uint32_t max_parallel_sub_batches_ = 4;
    bool stream_read_enabled_ = true;
    uint32_t stream_read_chunk_size_bytes_ = 16 * 1024 * 1024;
};

} // namespace falconkv
