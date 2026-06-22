#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "src/common/status.h"
#include "src/store/hixl_transport.h"

namespace falconkv {

class HixlBufferPool {
public:
    struct Lease {
        size_t index = 0;
        void* addr = nullptr;
        size_t size = 0;
    };

    HixlBufferPool() = default;
    ~HixlBufferPool();

    HixlBufferPool(const HixlBufferPool&) = delete;
    HixlBufferPool& operator=(const HixlBufferPool&) = delete;

    Status Init(size_t chunk_size, size_t chunk_count,
                size_t alignment, HixlTransport* transport);
    Status Acquire(size_t size, Lease* lease);
    void Release(const Lease& lease);
    void Close();
    size_t chunk_size() const { return chunk_size_; }

private:
    struct Chunk {
        void* addr = nullptr;
        bool in_use = false;
    };

    std::mutex mutex_;
    std::vector<Chunk> chunks_;
    void* base_addr_ = nullptr;
    void* mem_handle_ = nullptr;
    bool acl_host_allocated_ = false;
    size_t chunk_size_ = 0;
    size_t total_size_ = 0;
    size_t alignment_ = 4096;
    HixlTransport* transport_ = nullptr;
};

} // namespace falconkv
