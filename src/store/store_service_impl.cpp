#include "src/store/store_service_impl.h"
#include "src/store/store_meta_index.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "src/common/time_util.h"

namespace falconkv {

namespace {

constexpr uint32_t kStreamChunkMagic = 0x46525331;  // "FRS1"
constexpr uint32_t kStreamChunkVersion = 1;
constexpr uint32_t kDefaultStreamChunkSize = 16 * 1024 * 1024;
constexpr uint32_t kDefaultStreamPrefetchChunks = 4;
constexpr uint32_t kDefaultStreamQueueChunks = 4;

struct StreamChunkHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t segment_index;
    uint32_t status;
    uint64_t offset_in_segment;
    uint32_t payload_size;
    uint32_t reserved;
};

struct StreamReadSlice {
    uint32_t segment_index;
    uint64_t offset;
    uint64_t offset_in_segment;
    uint32_t size;
};

struct StreamReadChunk {
    std::vector<StreamReadSlice> slices;
    uint64_t total_size = 0;
};

class StreamMessageQueue {
public:
    explicit StreamMessageQueue(size_t capacity)
        : capacity_(std::max<size_t>(1, capacity)) {}

    bool Push(std::unique_ptr<butil::IOBuf> msg) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] {
            return closed_ || queue_.size() < capacity_;
        });
        if (closed_) {
            return false;
        }
        queue_.push_back(std::move(msg));
        not_empty_.notify_one();
        return true;
    }

    bool Pop(std::unique_ptr<butil::IOBuf>& msg) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] {
            return closed_ || !queue_.empty();
        });
        if (queue_.empty()) {
            return false;
        }
        msg = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return true;
    }

    void Close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

private:
    size_t capacity_;
    std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<std::unique_ptr<butil::IOBuf>> queue_;
    bool closed_ = false;
};

int StreamWriteWithBackpressure(brpc::StreamId stream_id, butil::IOBuf& msg) {
    brpc::StreamWriteOptions options;
    options.write_in_background = true;
    while (true) {
        int rc = brpc::StreamWrite(stream_id, msg, &options);
        if (rc == 0) {
            return 0;
        }
        if (rc == EAGAIN) {
            int wait_rc = brpc::StreamWait(stream_id, nullptr);
            if (wait_rc == 0) {
                continue;
            }
            return wait_rc;
        }
        return rc;
    }
}

} // namespace

StoreServiceImpl::StoreServiceImpl(FalconKVStore* store)
    : store_(store) {}

// -----------------------------------------------------------------
// Read (offset-based, for remote client backward compatibility)
// -----------------------------------------------------------------
void StoreServiceImpl::Read(::google::protobuf::RpcController* controller,
                             const ReadRequest* request,
                             ReadResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    uint64_t offset = request->offset();
    uint32_t size = request->size();

    // Allocate aligned buffer for DirectIO read
    void* buffer = AlignedAllocator::Allocate(512, size);
    if (!buffer) {
        response->set_status(-1);
        return;
    }

    uint64_t request_ts_ns = GetCurrentTimeNs();
    Status s = store_->Read(offset, buffer, size);
    uint64_t done_ts_ns = GetCurrentTimeNs();

    if (!s.ok()) {
        AlignedAllocator::Free(buffer);
        response->set_status(static_cast<int>(s.code()));
        return;
    }

    // Report to Scheduler (Store perspective: NET_RX_READ)
    if (store_->scheduler_proxy()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            request->client_id(),
            size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(0);
    response->set_bytes_read(size);

    // Zero-copy: transfer buffer ownership to brpc attachment.
    // The deleter frees the buffer after brpc finishes sending.
    auto* cntl = static_cast<brpc::Controller*>(controller);
    cntl->response_attachment().append_user_data(
        buffer, size, AlignedAllocator::Free);
}

// -----------------------------------------------------------------
// BatchRead (offset-based)
// -----------------------------------------------------------------
void StoreServiceImpl::BatchRead(::google::protobuf::RpcController* controller,
                                  const BatchReadRequest* request,
                                  BatchReadResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    std::vector<ReadItem> items;
    std::vector<std::unique_ptr<void, void(*)(void*)>> buffers;

    items.reserve(request->segments_size());

    for (int i = 0; i < request->segments_size(); ++i) {
        const auto& seg = request->segments(i);
        uint32_t size = seg.size();

        void* buf = AlignedAllocator::Allocate(512, size);
        if (!buf) {
            response->set_status(-1);
            return;
        }
        buffers.emplace_back(buf, &AlignedAllocator::Free);
        items.push_back({seg.offset(), buf, size});
    }

    uint64_t total_io_size = 0;
    for (const auto& item : items) total_io_size += item.size;

    uint64_t request_ts_ns = GetCurrentTimeNs();
    Status s = store_->BatchRead(items);
    uint64_t done_ts_ns = GetCurrentTimeNs();

    // Report to Scheduler (Store perspective: NET_RX_READ)
    if (store_->scheduler_proxy() && s.ok()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            0,  // client_id not available in BatchReadRequest
            total_io_size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(s.ok() ? 0 : static_cast<int>(s.code()));
    if (!s.ok()) {
        for (size_t i = 0; i < items.size(); ++i) {
            response->add_bytes_read(0);
        }
        return;
    }

    // Fill bytes_read metadata.
    for (size_t i = 0; i < items.size(); ++i) {
        response->add_bytes_read(items[i].size);
    }

    // Zero-copy: transfer each buffer's ownership to brpc attachment.
    // Layout: [seg0_data][seg1_data]...[segN_data] concatenated sequentially.
    // Client uses bytes_read[] to locate each segment's offset.
    auto* cntl = static_cast<brpc::Controller*>(controller);
    for (size_t i = 0; i < items.size(); ++i) {
        cntl->response_attachment().append_user_data(
            buffers[i].release(), items[i].size, AlignedAllocator::Free);
    }
}

// -----------------------------------------------------------------
// BatchReadStream (offset-based, stream response chunks)
// -----------------------------------------------------------------
void StoreServiceImpl::BatchReadStream(::google::protobuf::RpcController* controller,
                                       const BatchReadStreamRequest* request,
                                       BatchReadStreamResponse* response,
                                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);

    brpc::StreamId stream_id = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions stream_options;
    stream_options.max_buf_size = 32 * 1024 * 1024;
    if (brpc::StreamAccept(&stream_id, *cntl, &stream_options) != 0) {
        response->set_status(-1);
        response->set_segment_count(0);
        return;
    }

    std::vector<StreamReadSlice> slices;
    slices.reserve(request->segments_size());
    for (int i = 0; i < request->segments_size(); ++i) {
        const auto& seg = request->segments(i);
        if (seg.size() == 0) {
            continue;
        }
        slices.push_back({static_cast<uint32_t>(i), seg.offset(), 0, seg.size()});
    }

    uint32_t chunk_size = request->chunk_size_bytes() > 0
        ? request->chunk_size_bytes()
        : kDefaultStreamChunkSize;
    chunk_size = std::max<uint32_t>(1, chunk_size);
    uint32_t prefetch_chunks = request->prefetch_chunks() > 0
        ? request->prefetch_chunks()
        : kDefaultStreamPrefetchChunks;
    prefetch_chunks = std::max<uint32_t>(1, prefetch_chunks);
    uint32_t queue_chunks = request->queue_chunks() > 0
        ? request->queue_chunks()
        : kDefaultStreamQueueChunks;
    queue_chunks = std::max<uint32_t>(1, queue_chunks);
    std::string source_node_addr = request->source_node_addr();
    FalconKVStore* store = store_;

    std::vector<StreamReadChunk> chunks;
    size_t next = 0;
    while (next < slices.size()) {
        StreamReadChunk chunk;
        while (next < slices.size() && chunk.total_size < chunk_size) {
            StreamReadSlice slice = slices[next];
            uint32_t take = static_cast<uint32_t>(
                std::min<uint64_t>(slice.size, chunk_size - chunk.total_size));
            slice.size = take;
            chunk.slices.push_back(slice);
            chunk.total_size += take;

            slices[next].offset += take;
            slices[next].offset_in_segment += take;
            slices[next].size -= take;
            if (slices[next].size == 0) {
                ++next;
            }
        }
        if (!chunk.slices.empty()) {
            chunks.push_back(std::move(chunk));
        }
    }

    response->set_status(0);
    response->set_segment_count(request->segments_size());

    std::thread([store, stream_id, chunks = std::move(chunks), prefetch_chunks,
                 queue_chunks, source_node_addr = std::move(source_node_addr)]() mutable {
        auto queue = std::make_shared<StreamMessageQueue>(queue_chunks);
        std::atomic<size_t> next_chunk{0};
        std::atomic<bool> failed{false};

        std::thread sender([stream_id, queue, &failed]() {
            std::unique_ptr<butil::IOBuf> msg;
            while (queue->Pop(msg)) {
                if (StreamWriteWithBackpressure(stream_id, *msg) != 0) {
                    failed.store(true, std::memory_order_release);
                    queue->Close();
                    break;
                }
            }
            brpc::StreamClose(stream_id);
        });

        auto reader = [&]() {
            while (!failed.load(std::memory_order_acquire)) {
                size_t idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                if (idx >= chunks.size()) {
                    return;
                }
                const auto& chunk = chunks[idx];

                std::vector<ReadItem> items;
                std::vector<std::unique_ptr<void, void(*)(void*)>> buffers;
                items.reserve(chunk.slices.size());
                buffers.reserve(chunk.slices.size());

                bool alloc_failed = false;
                for (const auto& slice : chunk.slices) {
                    void* buf = AlignedAllocator::Allocate(512, slice.size);
                    if (!buf) {
                        alloc_failed = true;
                        break;
                    }
                    buffers.emplace_back(buf, &AlignedAllocator::Free);
                    items.push_back({slice.offset, buf, slice.size});
                }

                uint64_t request_ts_ns = GetCurrentTimeNs();
                Status s = alloc_failed
                    ? Status::NoSpace("stream chunk allocation failed")
                    : store->BatchRead(items);
                uint64_t done_ts_ns = GetCurrentTimeNs();

                if (store->scheduler_proxy() && s.ok()) {
                    store->scheduler_proxy()->StoreReportIOAsync(
                        store->store_id(),
                        3,  // NET_RX_READ
                        0,
                        chunk.total_size,
                        request_ts_ns,
                        done_ts_ns,
                        source_node_addr);
                }

                auto msg = std::make_unique<butil::IOBuf>();
                for (size_t i = 0; i < chunk.slices.size(); ++i) {
                    const auto& slice = chunk.slices[i];
                    StreamChunkHeader header;
                    header.magic = kStreamChunkMagic;
                    header.version = kStreamChunkVersion;
                    header.segment_index = slice.segment_index;
                    header.status = s.ok() ? 0 : static_cast<uint32_t>(s.code());
                    header.offset_in_segment = slice.offset_in_segment;
                    header.payload_size = s.ok() ? slice.size : 0;
                    header.reserved = 0;

                    msg->append(&header, sizeof(header));
                    if (s.ok()) {
                        msg->append_user_data(buffers[i].release(), slice.size,
                                              AlignedAllocator::Free);
                    }
                }
                if (!queue->Push(std::move(msg))) {
                    return;
                }

                if (!s.ok()) {
                    failed.store(true, std::memory_order_release);
                    queue->Close();
                    return;
                }
            }
        };

        std::vector<std::thread> readers;
        size_t reader_count = std::min<size_t>(prefetch_chunks,
                                               std::max<size_t>(1, chunks.size()));
        readers.reserve(reader_count);
        for (size_t i = 0; i < reader_count; ++i) {
            readers.emplace_back(reader);
        }
        for (auto& t : readers) {
            if (t.joinable()) {
                t.join();
            }
        }
        queue->Close();
        if (sender.joinable()) {
            sender.join();
        }
    }).detach();
}

// -----------------------------------------------------------------
// GetByKey (key-based read)
// -----------------------------------------------------------------
void StoreServiceImpl::GetByKey(::google::protobuf::RpcController*,
                                 const GetByKeyRequest* request,
                                 GetByKeyResponse* response,
                                 ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    const std::string& key = request->key();

    // First look up the key to get its actual size.
    std::vector<StoreKeyRecord> hits;
    std::vector<std::string> misses;
    store_->BatchContains({key}, hits, misses);
    if (hits.empty()) {
        response->set_status(static_cast<int>(Status::kNotFound));
        return;
    }

    uint32_t data_size = hits[0].size;
    void* buffer = AlignedAllocator::Allocate(512, data_size);
    if (!buffer) {
        response->set_status(-1);
        return;
    }

    uint64_t request_ts_ns = GetCurrentTimeNs();
    auto result = store_->Get(key, buffer, data_size);
    uint64_t done_ts_ns = GetCurrentTimeNs();

    if (!result.status.ok()) {
        AlignedAllocator::Free(buffer);
        response->set_status(static_cast<int>(result.status.code()));
        return;
    }

    // Report to Scheduler (Store perspective: NET_RX_READ)
    if (store_->scheduler_proxy()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            request->client_id(),
            result.size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(0);
    response->set_data(buffer, result.size);
    response->set_size(result.size);
    AlignedAllocator::Free(buffer);
}

// -----------------------------------------------------------------
// BatchGetByKey (key-based batch read)
// -----------------------------------------------------------------
void StoreServiceImpl::BatchGetByKey(::google::protobuf::RpcController*,
                                      const BatchGetByKeyRequest* request,
                                      BatchGetByKeyResponse* response,
                                      ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    // Batch look up all keys to get their actual sizes.
    std::vector<std::string> keys;
    keys.reserve(request->keys_size());
    for (int i = 0; i < request->keys_size(); ++i) {
        keys.push_back(request->keys(i));
    }

    std::vector<StoreKeyRecord> hits;
    std::vector<std::string> misses;
    store_->BatchContains(keys, hits, misses);

    // Build a set of found keys with their sizes for quick lookup.
    std::unordered_map<std::string, uint32_t> key_sizes;
    for (const auto& rec : hits) {
        key_sizes[rec.key] = rec.size;
    }

    bool all_ok = true;

    for (int i = 0; i < request->keys_size(); ++i) {
        const std::string& key = request->keys(i);

        auto it = key_sizes.find(key);
        if (it == key_sizes.end()) {
            response->add_data_segments();
            response->add_sizes(0);
            response->add_statuses(static_cast<int>(Status::kNotFound));
            all_ok = false;
            continue;
        }

        uint32_t data_size = it->second;
        void* buffer = AlignedAllocator::Allocate(512, data_size);
        if (!buffer) {
            response->add_data_segments();
            response->add_sizes(0);
            response->add_statuses(-1);
            all_ok = false;
            continue;
        }

        uint64_t request_ts_ns = GetCurrentTimeNs();
        auto result = store_->Get(key, buffer, data_size);
        uint64_t done_ts_ns = GetCurrentTimeNs();

        if (!result.status.ok()) {
            AlignedAllocator::Free(buffer);
            response->add_data_segments();
            response->add_sizes(0);
            response->add_statuses(static_cast<int>(result.status.code()));
            all_ok = false;
            continue;
        }

        // Report to Scheduler (Store perspective: NET_RX_READ)
        if (store_->scheduler_proxy()) {
            store_->scheduler_proxy()->StoreReportIOAsync(
                store_->store_id(),
                3,  // NET_RX_READ
                request->client_id(),
                result.size,
                request_ts_ns,
                done_ts_ns,
                request->source_node_addr());
        }

        response->add_data_segments(buffer, result.size);
        response->add_sizes(result.size);
        response->add_statuses(0);
        AlignedAllocator::Free(buffer);
    }

    response->set_status(all_ok ? 0 : 1);
}

// -----------------------------------------------------------------
// PrepareHixlBatchRead (control-plane stub; data plane comes in HiXL phase)
// -----------------------------------------------------------------
void StoreServiceImpl::PrepareHixlBatchRead(
    ::google::protobuf::RpcController*,
    const PrepareHixlBatchReadRequest* request,
    PrepareHixlBatchReadResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    std::vector<HixlReadRequest> requests;
    requests.reserve(request->segments_size());
    for (int i = 0; i < request->segments_size(); ++i) {
        const auto& seg = request->segments(i);
        requests.push_back({seg.offset(), seg.size()});
    }

    HixlPrepareResult result;
    uint64_t request_ts_ns = GetCurrentTimeNs();
    Status s = store_->PrepareHixlBatchRead(requests, &result);
    uint64_t done_ts_ns = GetCurrentTimeNs();
    if (!s.ok()) {
        response->set_status(static_cast<int>(s.code()));
        response->set_error_msg(s.msg());
        return;
    }

    uint64_t total_io_size = 0;
    for (const auto& req : requests) {
        total_io_size += req.size;
    }
    if (store_->scheduler_proxy()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            0,
            total_io_size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(0);
    response->set_token(result.token);
    response->set_remote_engine(result.remote_engine);
    for (const auto& segment : result.segments) {
        auto* out = response->add_segments();
        out->set_segment_index(segment.segment_index);
        out->set_remote_addr(segment.remote_addr);
        out->set_size(segment.size);
        out->set_status(segment.status);
    }
}

// -----------------------------------------------------------------
// ReleaseHixlReadToken (control-plane stub)
// -----------------------------------------------------------------
void StoreServiceImpl::ReleaseHixlReadToken(
    ::google::protobuf::RpcController*,
    const ReleaseHixlReadTokenRequest* request,
    ReleaseHixlReadTokenResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    Status s = store_->ReleaseHixlReadToken(request->token());
    response->set_status(s.ok() ? 0 : static_cast<int>(s.code()));
}

// -----------------------------------------------------------------
// Ping
// -----------------------------------------------------------------
void StoreServiceImpl::Ping(::google::protobuf::RpcController*,
                             const PingRequest* request,
                             PongResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    response->set_status(0);
    response->set_timestamp_ns(request->timestamp_ns());
}

} // namespace falconkv
