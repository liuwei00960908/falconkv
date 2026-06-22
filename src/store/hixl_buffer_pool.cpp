#include "src/store/hixl_buffer_pool.h"

#include "src/common/aligned_allocator.h"

#ifdef FALCONKV_HAS_HIXL
#include <acl/acl.h>
#endif

namespace falconkv {

HixlBufferPool::~HixlBufferPool() {
    Close();
}

Status HixlBufferPool::Init(size_t chunk_size, size_t chunk_count,
                            size_t alignment, HixlTransport* transport) {
    if (!transport || !transport->initialized()) {
        return Status::InvalidArg("HiXL buffer pool requires initialized transport");
    }
    if (chunk_size == 0 || chunk_count == 0) {
        return Status::InvalidArg("invalid HiXL buffer pool sizing");
    }
    if (transport->mem_type() != "host") {
        return Status::NotSupported("FalconKV HiXL buffer pool only supports host memory");
    }
    Close();
    chunk_size_ = chunk_size;
    alignment_ = alignment == 0 ? 4096 : alignment;
    transport_ = transport;
    chunks_.resize(chunk_count);
#ifdef FALCONKV_HAS_HIXL
    bool use_acl_host = transport_->mem_type() == "host";
#endif

    for (auto& chunk : chunks_) {
#ifdef FALCONKV_HAS_HIXL
        if (use_acl_host) {
            aclError acl_rc = aclrtMallocHost(&chunk.addr, chunk_size_);
            if (acl_rc != ACL_ERROR_NONE) {
                Close();
                return Status::NoSpace("aclrtMallocHost failed: " +
                                       std::to_string(acl_rc));
            }
            chunk.acl_host_allocated = true;
        } else
#endif
        {
            chunk.addr = AlignedAllocator::Allocate(alignment_, chunk_size_);
        }
        if (!chunk.addr) {
            Close();
            return Status::NoSpace("failed to allocate HiXL buffer pool chunk");
        }
        Status s = transport_->RegisterMemory(chunk.addr, chunk_size_,
                                              &chunk.mem_handle);
        if (!s.ok()) {
            Close();
            return s;
        }
    }
    return Status::OK();
}

Status HixlBufferPool::Acquire(size_t size, Lease* lease) {
    if (!lease) {
        return Status::InvalidArg("null HiXL pool lease");
    }
    if (size == 0 || size > chunk_size_) {
        return Status::NoSpace("HiXL buffer request exceeds chunk size");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < chunks_.size(); ++i) {
        if (!chunks_[i].in_use) {
            chunks_[i].in_use = true;
            lease->index = i;
            lease->addr = chunks_[i].addr;
            lease->size = size;
            return Status::OK();
        }
    }
    return Status::NoSpace("HiXL buffer pool exhausted");
}

void HixlBufferPool::Release(const Lease& lease) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lease.index < chunks_.size()) {
        chunks_[lease.index].in_use = false;
    }
}

void HixlBufferPool::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& chunk : chunks_) {
        if (transport_ && chunk.mem_handle) {
            transport_->DeregisterMemory(chunk.mem_handle);
        }
        if (chunk.addr) {
#ifdef FALCONKV_HAS_HIXL
            if (chunk.acl_host_allocated) {
                aclrtFreeHost(chunk.addr);
            } else
#endif
            {
                AlignedAllocator::Free(chunk.addr);
            }
        }
    }
    chunks_.clear();
    chunk_size_ = 0;
    transport_ = nullptr;
}

} // namespace falconkv
