#include "src/store/hixl_buffer_pool.h"

#include <limits>

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
    if (chunk_count > std::numeric_limits<size_t>::max() / chunk_size) {
        return Status::InvalidArg("HiXL buffer pool size overflow");
    }
    Close();
    chunk_size_ = chunk_size;
    total_size_ = chunk_size * chunk_count;
    alignment_ = alignment == 0 ? 4096 : alignment;
    transport_ = transport;
    chunks_.resize(chunk_count);
#ifdef FALCONKV_HAS_HIXL
    bool use_acl_host = transport_->mem_type() == "host";
#endif

#ifdef FALCONKV_HAS_HIXL
    if (use_acl_host) {
        aclError acl_rc = aclrtMallocHost(&base_addr_, total_size_);
        if (acl_rc != ACL_ERROR_NONE) {
            Close();
            return Status::NoSpace("aclrtMallocHost failed: " +
                                   std::to_string(acl_rc));
        }
        acl_host_allocated_ = true;
    } else
#endif
    {
        base_addr_ = AlignedAllocator::Allocate(alignment_, total_size_);
    }
    if (!base_addr_) {
        Close();
        return Status::NoSpace("failed to allocate HiXL buffer pool");
    }

    Status s = transport_->RegisterMemory(base_addr_, total_size_,
                                          &mem_handle_);
    if (!s.ok()) {
        Close();
        return s;
    }

    auto* base = static_cast<unsigned char*>(base_addr_);
    for (size_t i = 0; i < chunks_.size(); ++i) {
        chunks_[i].addr = base + i * chunk_size_;
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
    if (transport_ && mem_handle_) {
        transport_->DeregisterMemory(mem_handle_);
    }
    if (base_addr_) {
#ifdef FALCONKV_HAS_HIXL
        if (acl_host_allocated_) {
            aclrtFreeHost(base_addr_);
        } else
#endif
        {
            AlignedAllocator::Free(base_addr_);
        }
    }
    chunks_.clear();
    base_addr_ = nullptr;
    mem_handle_ = nullptr;
    acl_host_allocated_ = false;
    chunk_size_ = 0;
    total_size_ = 0;
    transport_ = nullptr;
}

} // namespace falconkv
