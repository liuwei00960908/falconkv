#include "src/store/store_rpc_client.h"

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <future>
#include <mutex>

#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>

#include "src/common/logging.h"

namespace falconkv {

namespace {

constexpr uint32_t kStreamChunkMagic = 0x46525331;  // "FRS1"
constexpr uint32_t kStreamChunkVersion = 1;

struct StreamChunkHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t segment_index;
    uint32_t status;
    uint64_t offset_in_segment;
    uint32_t payload_size;
    uint32_t reserved;
};

class BatchReadStreamHandler : public brpc::StreamInputHandler {
public:
    BatchReadStreamHandler(const std::vector<uint32_t>& sizes,
                           const std::vector<void*>& buffers,
                           std::vector<int32_t>& results)
        : sizes_(sizes), buffers_(buffers), results_(results) {}

    int on_received_messages(brpc::StreamId,
                             butil::IOBuf* const messages[],
                             size_t size) override {
        for (size_t i = 0; i < size; ++i) {
            butil::IOBuf* msg = messages[i];
            if (!msg) {
                MarkFailed("empty stream message");
                return -1;
            }

            size_t pos = 0;
            while (pos < msg->size()) {
                if (msg->size() - pos < sizeof(StreamChunkHeader)) {
                    MarkFailed("stream chunk too small");
                    return -1;
                }

                StreamChunkHeader header;
                msg->copy_to(&header, sizeof(header), pos);
                pos += sizeof(header);

                if (header.magic != kStreamChunkMagic ||
                    header.version != kStreamChunkVersion ||
                    header.segment_index >= sizes_.size()) {
                    MarkFailed("invalid stream chunk header");
                    return -1;
                }
                if (header.status != 0) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    results_[header.segment_index] = -1;
                    failed_ = true;
                    error_msg_ = "remote stream chunk read failed";
                    return -1;
                }
                if (msg->size() - pos < header.payload_size ||
                    header.offset_in_segment + header.payload_size >
                        sizes_[header.segment_index]) {
                    MarkFailed("invalid stream chunk payload");
                    return -1;
                }

                char* dst = static_cast<char*>(buffers_[header.segment_index]) +
                            header.offset_in_segment;
                msg->copy_to(dst, header.payload_size, pos);
                pos += header.payload_size;

                std::lock_guard<std::mutex> lock(mutex_);
                if (results_[header.segment_index] >= 0) {
                    results_[header.segment_index] +=
                        static_cast<int32_t>(header.payload_size);
                }
            }
        }
        return 0;
    }

    void on_idle_timeout(brpc::StreamId) override {
        MarkFailed("stream idle timeout");
        MarkDone();
    }

    void on_failed(brpc::StreamId, int, const std::string& error_text) override {
        MarkFailed(error_text);
    }

    void on_closed(brpc::StreamId) override {
        MarkDone();
    }

    Status Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return done_; });
        if (failed_) {
            return Status::RpcError(error_msg_.empty() ? "stream read failed"
                                                      : error_msg_);
        }
        for (size_t i = 0; i < sizes_.size(); ++i) {
            if (results_[i] != static_cast<int32_t>(sizes_[i])) {
                return Status::RpcError("stream read incomplete");
            }
        }
        return Status::OK();
    }

private:
    void MarkFailed(const std::string& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        failed_ = true;
        error_msg_ = msg;
    }

    void MarkDone() {
        std::lock_guard<std::mutex> lock(mutex_);
        done_ = true;
        cv_.notify_all();
    }

    const std::vector<uint32_t>& sizes_;
    const std::vector<void*>& buffers_;
    std::vector<int32_t>& results_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_ = false;
    bool failed_ = false;
    std::string error_msg_;
};

} // namespace

StoreRpcClient::StoreRpcClient() = default;

StoreRpcClient::~StoreRpcClient() = default;

void StoreRpcClient::SetHixlReadConfig(bool enabled,
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

Status StoreRpcClient::Connect(const std::string& addr,
                               uint64_t max_body_size_bytes,
                               uint32_t max_parallel_sub_batches,
                               bool stream_read_enabled,
                               uint32_t stream_read_chunk_size_bytes,
                               uint32_t stream_read_prefetch_chunks,
                               uint32_t stream_read_queue_chunks) {
    if (addr.empty()) {
        LOG(ERROR) << "[StoreRpcClient] Connect: empty store address";
        return Status::InvalidArg("empty store address");
    }

    max_body_size_bytes_ = max_body_size_bytes;
    max_parallel_sub_batches_ = std::max<uint32_t>(1, max_parallel_sub_batches);
    stream_read_enabled_ = stream_read_enabled;
    stream_read_chunk_size_bytes_ = std::max<uint32_t>(1, stream_read_chunk_size_bytes);
    stream_read_prefetch_chunks_ = std::max<uint32_t>(1, stream_read_prefetch_chunks);
    stream_read_queue_chunks_ = std::max<uint32_t>(1, stream_read_queue_chunks);

    brpc::ChannelOptions options;
    options.connect_timeout_ms = 3000;
    options.timeout_ms = 30000;       // 30s for large batch reads
    options.max_retry = 1;

    int rc = channel_.Init(addr.c_str(), &options);
    if (rc != 0) {
        LOG(ERROR) << "[StoreRpcClient] Connect: failed to init brpc channel to store at "
                   << addr << ", rc=" << rc;
        return Status::RpcError("failed to init brpc channel to store at " +
                                addr);
    }

    stub_ = std::make_unique<FalconKVStoreService_Stub>(&channel_);
    connected_ = true;
    LOG(INFO) << "[StoreRpcClient] Connected to store at " << addr;
    return Status::OK();
}

Status StoreRpcClient::Read(uint64_t offset, void* buffer, uint32_t size,
                             uint32_t client_id,
                             const std::string& source_node_addr) {
    if (!connected_) {
        LOG(ERROR) << "[StoreRpcClient] Read: not connected";
        return Status::RpcError("not connected");
    }

    ReadRequest request;
    request.set_offset(offset);
    request.set_size(size);
    request.set_client_id(client_id);
    request.set_source_node_addr(source_node_addr);

    ReadResponse response;
    brpc::Controller cntl;

    stub_->Read(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "[StoreRpcClient] Read RPC failed at offset " << offset
                   << ", size=" << size << ": " << cntl.ErrorText();
        return Status::RpcError("Store Read RPC failed: " +
                                std::string(cntl.ErrorText()));
    }

    if (response.status() != 0) {
        LOG(ERROR) << "[StoreRpcClient] Read failed at offset " << offset
                   << " with status=" << response.status();
        return Status::IoError("Store Read failed with status: " +
                               std::to_string(response.status()));
    }

    uint32_t bytes_read = response.bytes_read();
    if (bytes_read > size) {
        bytes_read = size;
    }

    // Read data from brpc attachment (zero-copy path from server)
    if (bytes_read > 0) {
        cntl.response_attachment().copy_to(buffer, bytes_read);
    }
    return Status::OK();
}

Status StoreRpcClient::BatchReadStream(const std::vector<uint64_t>& offsets,
                                       const std::vector<uint32_t>& sizes,
                                       const std::vector<void*>& buffers,
                                       std::vector<int32_t>& results,
                                       const std::string& source_node_addr) {
    results.assign(offsets.size(), 0);

    BatchReadStreamRequest request;
    for (size_t i = 0; i < offsets.size(); ++i) {
        auto* seg = request.add_segments();
        seg->set_offset(offsets[i]);
        seg->set_size(sizes[i]);
    }
    if (!source_node_addr.empty()) {
        request.set_source_node_addr(source_node_addr);
    }
    request.set_chunk_size_bytes(stream_read_chunk_size_bytes_);
    request.set_prefetch_chunks(stream_read_prefetch_chunks_);
    request.set_queue_chunks(stream_read_queue_chunks_);

    BatchReadStreamResponse response;
    brpc::Controller cntl;
    BatchReadStreamHandler handler(sizes, buffers, results);

    brpc::StreamOptions stream_options;
    stream_options.max_buf_size = std::max<size_t>(32 * 1024 * 1024,
                                                   stream_read_chunk_size_bytes_ * 2ULL);
    stream_options.handler = &handler;

    brpc::StreamId stream_id = brpc::INVALID_STREAM_ID;
    if (brpc::StreamCreate(&stream_id, cntl, &stream_options) != 0) {
        return Status::RpcError("Store BatchReadStream: StreamCreate failed");
    }

    stub_->BatchReadStream(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        brpc::StreamClose(stream_id);
        return Status::RpcError("Store BatchReadStream RPC failed: " +
                                std::string(cntl.ErrorText()));
    }
    if (response.status() != 0 ||
        response.segment_count() != offsets.size()) {
        brpc::StreamClose(stream_id);
        return Status::RpcError("Store BatchReadStream setup failed");
    }

    return handler.Wait();
}

Status StoreRpcClient::BatchRead(const std::vector<uint64_t>& offsets,
                                  const std::vector<uint32_t>& sizes,
                                  const std::vector<void*>& buffers,
                                  std::vector<int32_t>& results,
                                  const std::string& source_node_addr,
                                  const std::string& hixl_engine_addr) {
    if (!connected_) {
        LOG(ERROR) << "[StoreRpcClient] BatchRead: not connected";
        return Status::RpcError("not connected");
    }

    size_t n = offsets.size();
    if (n != sizes.size() || n != buffers.size()) {
        LOG(ERROR) << "[StoreRpcClient] BatchRead: size mismatch, offsets="
                   << n << " sizes=" << sizes.size()
                   << " buffers=" << buffers.size();
        return Status::InvalidArg("BatchRead parameter size mismatch");
    }

    results.assign(n, 0);

    if (n == 0) {
        return Status::OK();
    }

    uint64_t total_read_size = 0;
    for (uint32_t size : sizes) {
        total_read_size += size;
    }

    if (hixl_read_enabled_ && !hixl_engine_addr.empty() &&
        total_read_size >= hixl_min_read_size_bytes_) {
        Status hixl_status = BatchReadHixl(offsets, sizes, buffers, results,
                                           source_node_addr, hixl_engine_addr);
        if (hixl_status.ok()) {
            return Status::OK();
        }
        if (!hixl_fallback_to_brpc_) {
            return hixl_status;
        }
        LOG(WARNING) << "[StoreRpcClient] HiXL BatchRead failed, falling back to brpc: "
                     << hixl_status.ToString();
        results.assign(n, 0);
    }

    if (stream_read_enabled_) {
        Status stream_status = BatchReadStream(offsets, sizes, buffers, results,
                                               source_node_addr);
        if (stream_status.ok()) {
            return Status::OK();
        }
        LOG(WARNING) << "[StoreRpcClient] BatchReadStream failed, falling back: "
                     << stream_status.ToString();
        results.assign(n, 0);
    }

    // Effective limit: reserve 10% for protobuf overhead
    const uint64_t effective_limit =
        max_body_size_bytes_ * 9 / 10;

    // Greedy packing: split segments into sub-batches by accumulated data size
    struct SubBatch {
        std::vector<size_t> original_indices;
        uint64_t total_data_size = 0;
    };

    std::vector<SubBatch> sub_batches;
    SubBatch current;
    for (size_t i = 0; i < n; ++i) {
        uint64_t seg_size = sizes[i];
        // If adding this segment exceeds the limit and current batch is non-empty,
        // finalize current batch and start a new one.
        if (!current.original_indices.empty() &&
            current.total_data_size + seg_size > effective_limit) {
            sub_batches.push_back(std::move(current));
            current = SubBatch();
        }
        // Single oversized segment still goes into its own batch (may fail)
        current.original_indices.push_back(i);
        current.total_data_size += seg_size;
    }
    if (!current.original_indices.empty()) {
        sub_batches.push_back(std::move(current));
    }

    if (sub_batches.size() > 1) {
        LOG(INFO) << "[StoreRpcClient] BatchRead: splitting " << n
                  << " segments into " << sub_batches.size()
                  << " sub-batches (effective_limit="
                  << effective_limit << " bytes)";
    }

    auto run_sub_batch = [&](const SubBatch& batch) -> Status {
        const auto& indices = batch.original_indices;
        size_t batch_n = indices.size();

        BatchReadRequest request;
        for (size_t j = 0; j < batch_n; ++j) {
            auto* seg = request.add_segments();
            seg->set_offset(offsets[indices[j]]);
            seg->set_size(sizes[indices[j]]);
        }
        if (!source_node_addr.empty()) {
            request.set_source_node_addr(source_node_addr);
        }

        BatchReadResponse response;
        brpc::Controller cntl;

        FalconKVStoreService_Stub stub(&channel_);
        stub.BatchRead(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(ERROR) << "[StoreRpcClient] BatchRead RPC failed (sub-batch of "
                       << batch_n << " segments): " << cntl.ErrorText();
            for (size_t j = 0; j < batch_n; ++j) {
                results[indices[j]] = -1;
            }
            return Status::RpcError("Store BatchRead RPC failed: " +
                                    std::string(cntl.ErrorText()));
        }

        if (response.status() != 0) {
            LOG(ERROR) << "[StoreRpcClient] BatchRead failed with status="
                       << response.status();
            for (size_t j = 0; j < batch_n; ++j) {
                results[indices[j]] = -1;
            }
            return Status::IoError("Store BatchRead failed with status: " +
                                   std::to_string(response.status()));
        }

        int seg_count = response.bytes_read_size();
        if (static_cast<size_t>(seg_count) != batch_n) {
            LOG(ERROR) << "[StoreRpcClient] BatchRead: sub-batch expected "
                       << batch_n << " segments, got " << seg_count;
            for (size_t j = 0; j < batch_n; ++j) {
                results[indices[j]] = -1;
            }
            return Status::RpcError("Store BatchRead: response segment count mismatch");
        }

        // Read data from brpc attachment
        size_t att_offset = 0;
        for (int j = 0; j < seg_count; ++j) {
            size_t orig_idx = indices[j];
            uint32_t bytes_read = response.bytes_read(j);
            if (bytes_read == 0) {
                results[orig_idx] = -1;
                continue;
            }
            uint32_t to_copy = std::min(bytes_read, sizes[orig_idx]);
            cntl.response_attachment().copy_to(buffers[orig_idx], to_copy,
                                               att_offset);
            att_offset += bytes_read;
            results[orig_idx] = static_cast<int32_t>(to_copy);
        }
        return Status::OK();
    };

    // Execute split RPCs with bounded parallelism so later sub-batches can
    // start remote SSD reads before earlier responses finish copying back.
    bool any_rpc_failed = false;
    const size_t parallelism = std::max<size_t>(1, max_parallel_sub_batches_);
    for (size_t start = 0; start < sub_batches.size(); start += parallelism) {
        size_t end = std::min(start + parallelism, sub_batches.size());
        std::vector<std::future<Status>> futures;
        futures.reserve(end - start);

        for (size_t i = start; i < end; ++i) {
            futures.emplace_back(std::async(std::launch::async,
                [&, batch = sub_batches[i]]() {
                    return run_sub_batch(batch);
                }));
        }

        for (auto& future : futures) {
            Status s = future.get();
            if (!s.ok()) {
                any_rpc_failed = true;
            }
        }
    }

    if (any_rpc_failed) {
        return Status::RpcError("Store BatchRead: one or more sub-batches failed");
    }

    return Status::OK();
}

Status StoreRpcClient::BatchReadHixl(const std::vector<uint64_t>& offsets,
                                     const std::vector<uint32_t>& sizes,
                                     const std::vector<void*>& buffers,
                                     std::vector<int32_t>& results,
                                     const std::string& source_node_addr,
                                     const std::string& hixl_engine_addr) {
    results.assign(offsets.size(), 0);

    Status pool_status = EnsureHixlReceivePool();
    if (!pool_status.ok()) {
        return pool_status;
    }

    PrepareHixlBatchReadRequest request;
    for (size_t i = 0; i < offsets.size(); ++i) {
        auto* seg = request.add_segments();
        seg->set_offset(offsets[i]);
        seg->set_size(sizes[i]);
    }
    if (!source_node_addr.empty()) {
        request.set_source_node_addr(source_node_addr);
    }

    PrepareHixlBatchReadResponse response;
    brpc::Controller cntl;
    stub_->PrepareHixlBatchRead(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("PrepareHixlBatchRead RPC failed: " +
                                std::string(cntl.ErrorText()));
    }
    if (response.status() != 0) {
        std::string msg = response.has_error_msg()
            ? response.error_msg()
            : "PrepareHixlBatchRead failed";
        return Status(static_cast<Status::Code>(response.status()), msg);
    }

    if (response.segments_size() != static_cast<int>(offsets.size())) {
        return Status::RpcError("PrepareHixlBatchRead response segment count mismatch");
    }

    auto release_token = [&]() {
        if (!response.has_token() || response.token().empty()) {
            return;
        }
        ReleaseHixlReadTokenRequest release_request;
        release_request.set_token(response.token());
        ReleaseHixlReadTokenResponse release_response;
        brpc::Controller release_cntl;
        stub_->ReleaseHixlReadToken(&release_cntl, &release_request,
                                    &release_response, nullptr);
        if (release_cntl.Failed()) {
            LOG(WARNING) << "[StoreRpcClient] ReleaseHixlReadToken failed: "
                         << release_cntl.ErrorText();
        }
    };

    std::vector<HixlBufferPool::Lease> leases;
    std::vector<HixlReadOp> ops;
    leases.reserve(offsets.size());
    ops.reserve(offsets.size());

    for (int i = 0; i < response.segments_size(); ++i) {
        const auto& seg = response.segments(i);
        if (seg.status() != 0 || seg.segment_index() >= offsets.size() ||
            seg.size() != sizes[seg.segment_index()] || seg.remote_addr() == 0) {
            for (const auto& lease : leases) {
                hixl_receive_pool_->Release(lease);
            }
            release_token();
            return Status::RpcError("invalid PrepareHixlBatchRead segment");
        }

        HixlBufferPool::Lease lease;
        Status s = hixl_receive_pool_->Acquire(seg.size(), &lease);
        if (!s.ok()) {
            for (const auto& held : leases) {
                hixl_receive_pool_->Release(held);
            }
            release_token();
            return s;
        }
        leases.push_back(lease);
        ops.push_back({reinterpret_cast<uintptr_t>(lease.addr),
                       static_cast<uintptr_t>(seg.remote_addr()),
                       seg.size()});
    }

    std::string remote_engine = response.has_remote_engine() &&
                                !response.remote_engine().empty()
        ? response.remote_engine()
        : hixl_engine_addr;
    Status s = hixl_transport_->Connect(remote_engine);
    if (s.ok()) {
        s = hixl_transport_->Read(remote_engine, ops, hixl_transfer_timeout_ms_);
    }
    if (!s.ok()) {
        for (const auto& lease : leases) {
            hixl_receive_pool_->Release(lease);
        }
        release_token();
        return s;
    }

    for (int i = 0; i < response.segments_size(); ++i) {
        const auto& seg = response.segments(i);
        size_t idx = seg.segment_index();
        std::memcpy(buffers[idx], leases[i].addr, seg.size());
        results[idx] = static_cast<int32_t>(seg.size());
    }

    for (const auto& lease : leases) {
        hixl_receive_pool_->Release(lease);
    }
    release_token();
    return Status::OK();
}

Status StoreRpcClient::EnsureHixlReceivePool() {
    std::lock_guard<std::mutex> lock(hixl_init_mutex_);
    if (hixl_receive_pool_ && hixl_transport_ && hixl_transport_->initialized()) {
        return Status::OK();
    }
    if (hixl_transport_config_.local_engine.empty()) {
        return Status::InvalidArg("HiXL local engine is empty");
    }

    hixl_transport_ = std::make_unique<HixlTransport>();
    Status s = hixl_transport_->Init(hixl_transport_config_);
    if (!s.ok()) {
        hixl_transport_.reset();
        return s;
    }

    hixl_receive_pool_ = std::make_unique<HixlBufferPool>();
    size_t chunk_size = static_cast<size_t>(hixl_receive_chunk_size_mb_) *
                        1024ULL * 1024ULL;
    s = hixl_receive_pool_->Init(chunk_size,
                                 hixl_receive_chunk_count_,
                                 4096,
                                 hixl_transport_.get());
    if (!s.ok()) {
        hixl_receive_pool_.reset();
        hixl_transport_->Close();
        hixl_transport_.reset();
        return s;
    }
    return Status::OK();
}

Status StoreRpcClient::Ping() {
    if (!connected_) {
        LOG(ERROR) << "[StoreRpcClient] Ping: not connected";
        return Status::RpcError("not connected");
    }

    PingRequest request;
    PongResponse response;
    brpc::Controller cntl;

    stub_->Ping(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "[StoreRpcClient] Ping RPC failed: " << cntl.ErrorText();
        return Status::RpcError("Store Ping RPC failed: " +
                                std::string(cntl.ErrorText()));
    }

    return Status::OK();
}

} // namespace falconkv
