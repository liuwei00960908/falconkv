#include "src/store/store_service_impl.h"
#include "src/store/store_meta_index.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <vector>
#include "src/common/time_util.h"

namespace falconkv {

namespace {

constexpr uint32_t kStreamChunkMagic = 0x46525331;  // "FRS1"
constexpr uint32_t kStreamChunkVersion = 1;
constexpr uint32_t kDefaultStreamChunkSize = 16 * 1024 * 1024;

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
    std::string source_node_addr = request->source_node_addr();
    FalconKVStore* store = store_;

    response->set_status(0);
    response->set_segment_count(request->segments_size());

    std::thread([store, stream_id, slices = std::move(slices), chunk_size,
                 source_node_addr = std::move(source_node_addr)]() mutable {
        size_t next = 0;
        while (next < slices.size()) {
            std::vector<StreamReadSlice> chunk;
            uint64_t chunk_bytes = 0;

            while (next < slices.size() && chunk_bytes < chunk_size) {
                StreamReadSlice slice = slices[next];
                uint32_t take = static_cast<uint32_t>(
                    std::min<uint64_t>(slice.size, chunk_size - chunk_bytes));
                slice.size = take;
                chunk.push_back(slice);
                chunk_bytes += take;

                slices[next].offset += take;
                slices[next].offset_in_segment += take;
                slices[next].size -= take;
                if (slices[next].size == 0) {
                    ++next;
                }
            }

            std::vector<ReadItem> items;
            std::vector<std::unique_ptr<void, void(*)(void*)>> buffers;
            items.reserve(chunk.size());
            buffers.reserve(chunk.size());

            bool alloc_failed = false;
            for (const auto& slice : chunk) {
                void* buf = AlignedAllocator::Allocate(512, slice.size);
                if (!buf) {
                    alloc_failed = true;
                    break;
                }
                buffers.emplace_back(buf, &AlignedAllocator::Free);
                items.push_back({slice.offset, buf, slice.size});
            }

            uint64_t request_ts_ns = GetCurrentTimeNs();
            Status s = alloc_failed ? Status::NoSpace("stream chunk allocation failed")
                                    : store->BatchRead(items);
            uint64_t done_ts_ns = GetCurrentTimeNs();

            if (store->scheduler_proxy() && s.ok()) {
                store->scheduler_proxy()->StoreReportIOAsync(
                    store->store_id(),
                    3,  // NET_RX_READ
                    0,
                    chunk_bytes,
                    request_ts_ns,
                    done_ts_ns,
                    source_node_addr);
            }

            for (size_t i = 0; i < chunk.size(); ++i) {
                const auto& slice = chunk[i];
                StreamChunkHeader header;
                header.magic = kStreamChunkMagic;
                header.version = kStreamChunkVersion;
                header.segment_index = slice.segment_index;
                header.status = s.ok() ? 0 : static_cast<uint32_t>(s.code());
                header.offset_in_segment = slice.offset_in_segment;
                header.payload_size = s.ok() ? slice.size : 0;
                header.reserved = 0;

                butil::IOBuf msg;
                msg.append(&header, sizeof(header));
                if (s.ok()) {
                    msg.append_user_data(buffers[i].release(), slice.size,
                                         AlignedAllocator::Free);
                }
                if (StreamWriteWithBackpressure(stream_id, msg) != 0) {
                    brpc::StreamClose(stream_id);
                    return;
                }
            }

            if (!s.ok()) {
                brpc::StreamClose(stream_id);
                return;
            }
        }

        brpc::StreamClose(stream_id);
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
