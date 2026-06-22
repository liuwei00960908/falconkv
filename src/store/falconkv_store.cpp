#include "src/store/falconkv_store.h"
#include "src/store/aligned_buffer_pool.h"
#include "src/store/store_meta_index.h"
#include "src/store/meta_sync_client.h"
#include "src/store/pending_evict_queue.h"
#include "src/store/evict_manager.h"
#include "src/store/io_thread_pool.h"
#include "src/store/io_uring_engine.h"
#include "src/common/slot_allocator.h"
#include "src/common/logging.h"
#include "src/common/time_util.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <linux/fs.h>
#include <sys/ioctl.h>

#include <cstring>
#include <cstdio>
#include <cerrno>
#include <algorithm>
#include <future>
#include <chrono>
#include <sstream>

namespace falconkv {

// ---------------------------------------------------------------------------
// Local helpers that delegate to AlignedAllocator
// ---------------------------------------------------------------------------
static inline uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return AlignedAllocator::AlignUp(value, alignment);
}

static inline bool IsAligned(uint64_t value, uint64_t alignment) {
    return (value & (alignment - 1)) == 0;
}

static inline bool IsPtrAligned(const void* ptr, size_t alignment) {
    return AlignedAllocator::IsAligned(ptr, alignment);
}

// ---------------------------------------------------------------------------
// Config::FromStoreConfig
// ---------------------------------------------------------------------------
FalconKVStore::Config FalconKVStore::Config::FromStoreConfig(const StoreConfig& sc) {
    Config cfg;
    cfg.ssd_path = sc.ssd_path;
    cfg.store_id = sc.store_id;
    cfg.node_id = sc.node_id;
    cfg.capacity_bytes = sc.capacity_bytes;
    cfg.page_size = sc.page_size;
    cfg.io_threads = sc.io_threads;
    cfg.scheduler_enabled = sc.scheduler_enabled;
    cfg.scheduler_uds_path = sc.scheduler_uds_path;
    cfg.scheduler_rpc_timeout_us = sc.scheduler_rpc_timeout_us;
    cfg.max_consecutive_failures = sc.max_consecutive_failures;
    cfg.reconnect_interval_sec = sc.reconnect_interval_sec;
    cfg.store_rpc_host = sc.store_rpc_host;
    cfg.listen_port = sc.listen_port;
    cfg.meta_addr = sc.meta_addr;
    cfg.evict_grace_period_ms = sc.evict_grace_period_ms;
    cfg.evict_check_interval_sec = sc.evict_check_interval_sec;
    cfg.evict_high_watermark = sc.evict_high_watermark;
    cfg.evict_low_watermark = sc.evict_low_watermark;
    cfg.evict_cold_threshold_ms = sc.evict_cold_threshold_ms;
    cfg.io_uring_enabled = sc.io_uring_enabled;
    cfg.direct_io_enabled = sc.direct_io_enabled;
    cfg.io_uring_queue_depth = sc.io_uring_queue_depth;
    cfg.slot_size_bytes = sc.slot_size_bytes;
    cfg.remote_read_transport = sc.remote_read_transport;
    cfg.hixl_engine_addr = sc.hixl_engine_addr;
    cfg.hixl_protocol_desc = sc.hixl_protocol_desc;
    cfg.hixl_local_comm_res = sc.hixl_local_comm_res;
    cfg.hixl_mem_type = sc.hixl_mem_type;
    cfg.hixl_device_id = sc.hixl_device_id;
    cfg.hixl_staging_chunk_size_mb = sc.hixl_staging_chunk_size_mb;
    cfg.hixl_staging_chunk_count = sc.hixl_staging_chunk_count;
    cfg.hixl_connect_timeout_ms = sc.hixl_connect_timeout_ms;
    return cfg;
}

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------
FalconKVStore::FalconKVStore(const Config& config)
    : config_(config), store_id_(config.store_id) {
    data_file_ = config_.ssd_path + "/kv_data_" + std::to_string(store_id_);
    store_rpc_addr_ = config_.store_rpc_host + ":" + std::to_string(config_.listen_port);
}

FalconKVStore::~FalconKVStore() {
    Close();
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
Status FalconKVStore::Init(const std::string& meta_addr) {
    // Create the SSD directory if it does not exist.
    struct stat st;
    if (stat(config_.ssd_path.c_str(), &st) != 0) {
        if (mkdir(config_.ssd_path.c_str(), 0755) != 0) {
            LOG(ERROR) << "[FalconKVStore] Failed to create SSD directory: "
                       << config_.ssd_path << ": " << strerror(errno);
            return Status::IoError("Failed to create SSD directory: " +
                                   config_.ssd_path + ": " + strerror(errno));
        }
    }

    Status s = InitDataFile();
    if (!s.ok()) {
        LOG(ERROR) << "[FalconKVStore] InitDataFile failed: " << s.ToString();
        return s;
    }

    // Create the aligned buffer pool for DirectIO operations.
    constexpr uint32_t kDefaultBufferSize = 2 * 1024 * 1024; // 2MB
    buffer_pool_ = std::make_unique<AlignedBufferPool>(
        kDefaultBufferSize, config_.page_size, config_.io_threads * 2);

    // Initialize SlotAllocator for space management.
    allocator_ = std::make_unique<SlotAllocator>(
        config_.capacity_bytes, config_.slot_size_bytes);

    // Initialize local metadata index.
    meta_index_ = std::make_unique<StoreMetaIndex>();

    // Initialize IO thread pool (always needed as fallback).
    io_pool_ = std::make_unique<IOThreadPool>(config_.io_threads);
    io_pool_->Start();

    // Initialize io_uring engine (optional, gracefully degrades).
    if (config_.io_uring_enabled) {
        io_uring_engine_ = std::make_unique<IOUringEngine>();
        io_uring_enabled_ = io_uring_engine_->Init(
            IOUringEngine::Config{config_.io_uring_queue_depth});
        if (io_uring_enabled_) {
            LOG(INFO) << "[FalconKVStore] io_uring enabled (queue_depth="
                      << config_.io_uring_queue_depth << ")";
        } else {
            LOG(INFO) << "[FalconKVStore] io_uring not available, using thread pool fallback";
        }
    } else {
        io_uring_enabled_ = false;
        LOG(INFO) << "[FalconKVStore] io_uring disabled by config";
    }

    if (config_.remote_read_transport == "hixl") {
        Status hixl_status = InitHixlRemoteRead();
        if (!hixl_status.ok()) {
            LOG(WARNING) << "[FalconKVStore] HiXL remote read disabled: "
                         << hixl_status.ToString();
        }
    }

    // Initialize MetaSyncClient.
    meta_sync_client_ = std::make_unique<MetaSyncClient>();
    std::string addr = meta_addr.empty() ? config_.meta_addr : meta_addr;
    meta_sync_client_->SetStoreInfo(store_id_, config_.node_id, data_file_,
                                     config_.capacity_bytes, hixl_engine_addr_);
    meta_sync_client_->SetMetaIndex(meta_index_.get());
    meta_sync_client_->SetStoreRpcAddr(config_.store_rpc_host, config_.listen_port);
    meta_sync_client_->Connect(addr);  // Connect triggers FullResync on success.
    meta_sync_client_->StartReconnectLoop(5);

    // Initialize PendingEvictQueue for deferred space reclamation.
    pending_evict_queue_ = std::make_unique<PendingEvictQueue>(
        config_.evict_grace_period_ms, allocator_.get());

    // Initialize EvictManager for automatic eviction.
    EvictManager::Config evict_cfg;
    evict_cfg.check_interval_sec = config_.evict_check_interval_sec;
    evict_cfg.high_watermark = config_.evict_high_watermark;
    evict_cfg.low_watermark = config_.evict_low_watermark;
    evict_cfg.cold_threshold_ms = config_.evict_cold_threshold_ms;
    evict_cfg.store_id = store_id_;
    evict_manager_ = std::make_unique<EvictManager>(
        evict_cfg, meta_index_.get(), meta_sync_client_.get(),
        pending_evict_queue_.get(), allocator_.get());

    running_.store(true);

    // Initialize SchedulerProxy if enabled
    if (config_.scheduler_enabled && !config_.scheduler_uds_path.empty()) {
        scheduler_proxy_ = std::make_unique<SchedulerProxy>(config_.scheduler_uds_path,
                                                             config_.scheduler_rpc_timeout_us / 1000,
                                                             config_.max_consecutive_failures,
                                                             config_.reconnect_interval_sec);
    }

    // Start background threads after running_ is set.
    pending_evict_queue_->Start();
    evict_manager_->Start();

    return Status::OK();
}

Status FalconKVStore::InitHixlRemoteRead() {
    hixl_read_ready_ = false;
    hixl_engine_addr_.clear();
    hixl_transport_ = std::make_unique<HixlTransport>();

    HixlTransportConfig hixl_cfg;
    hixl_cfg.local_engine = config_.hixl_engine_addr;
    hixl_cfg.protocol_desc = config_.hixl_protocol_desc;
    hixl_cfg.local_comm_res = config_.hixl_local_comm_res;
    hixl_cfg.mem_type = config_.hixl_mem_type;
    hixl_cfg.device_id = config_.hixl_device_id;
    hixl_cfg.connect_timeout_ms = config_.hixl_connect_timeout_ms;

    Status s = hixl_transport_->Init(hixl_cfg);
    if (!s.ok()) {
        return s;
    }

    hixl_staging_pool_ = std::make_unique<HixlBufferPool>();
    size_t chunk_size = static_cast<size_t>(config_.hixl_staging_chunk_size_mb) *
                        1024ULL * 1024ULL;
    s = hixl_staging_pool_->Init(chunk_size,
                                 config_.hixl_staging_chunk_count,
                                 config_.page_size,
                                 hixl_transport_.get());
    if (!s.ok()) {
        hixl_staging_pool_.reset();
        hixl_transport_->Close();
        return s;
    }

    hixl_engine_addr_ = config_.hixl_engine_addr;
    hixl_read_ready_ = true;
    LOG(INFO) << "[FalconKVStore] HiXL remote read enabled, engine="
              << hixl_engine_addr_ << ", staging_chunk_size=" << chunk_size
              << ", staging_chunk_count=" << config_.hixl_staging_chunk_count;
    return Status::OK();
}

Status FalconKVStore::PrepareHixlBatchRead(
    const std::vector<HixlReadRequest>& requests,
    HixlPrepareResult* result) {
    if (!result) {
        return Status::InvalidArg("null HiXL prepare result");
    }
    result->segments.clear();
    result->token.clear();
    result->remote_engine = hixl_engine_addr_;
    if (!hixl_read_ready_ || !hixl_staging_pool_) {
        return Status::NotSupported("HiXL remote read is not enabled");
    }
    if (requests.empty()) {
        return Status::OK();
    }

    std::vector<HixlBufferPool::Lease> leases;
    std::vector<ReadItem> read_items;
    leases.reserve(requests.size());
    read_items.reserve(requests.size());

    for (const auto& req : requests) {
        HixlBufferPool::Lease lease;
        Status s = hixl_staging_pool_->Acquire(req.size, &lease);
        if (!s.ok()) {
            for (const auto& held : leases) {
                hixl_staging_pool_->Release(held);
            }
            return s;
        }
        read_items.push_back({req.offset, lease.addr, req.size});
        leases.push_back(lease);
    }

    Status s = BatchRead(read_items);
    if (!s.ok()) {
        for (const auto& lease : leases) {
            hixl_staging_pool_->Release(lease);
        }
        return s;
    }

    static std::atomic<uint64_t> token_seq{1};
    uint64_t seq = token_seq.fetch_add(1, std::memory_order_relaxed);
    std::ostringstream oss;
    oss << store_id_ << "-" << GetCurrentTimeNs() << "-" << seq;
    result->token = oss.str();
    result->segments.reserve(leases.size());
    for (size_t i = 0; i < leases.size(); ++i) {
        result->segments.push_back({static_cast<uint32_t>(i),
                                    reinterpret_cast<uintptr_t>(leases[i].addr),
                                    requests[i].size,
                                    0});
    }

    HixlReadLease read_lease;
    read_lease.leases = std::move(leases);
    read_lease.create_time_ms = GetCurrentTimeMs();
    {
        std::lock_guard<std::mutex> lock(hixl_lease_mutex_);
        hixl_read_leases_[result->token] = std::move(read_lease);
    }
    return Status::OK();
}

Status FalconKVStore::ReleaseHixlReadToken(const std::string& token) {
    if (token.empty()) {
        return Status::InvalidArg("empty HiXL read token");
    }
    if (!hixl_staging_pool_) {
        return Status::OK();
    }

    HixlReadLease lease;
    {
        std::lock_guard<std::mutex> lock(hixl_lease_mutex_);
        auto it = hixl_read_leases_.find(token);
        if (it == hixl_read_leases_.end()) {
            return Status::OK();
        }
        lease = std::move(it->second);
        hixl_read_leases_.erase(it);
    }
    for (const auto& chunk : lease.leases) {
        hixl_staging_pool_->Release(chunk);
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// InitDataFile - open data file with dual-fd: buffered (always) + O_DIRECT (optional)
// ---------------------------------------------------------------------------
Status FalconKVStore::InitDataFile() {
    // 1. Always open a buffered fd (no O_DIRECT).
    int buf_flags = O_CREAT | O_RDWR;
    if (config_.disable_mtime) {
        buf_flags |= O_NOATIME;
    }

    data_fd_buffered_ = open(data_file_.c_str(), buf_flags, 0644);
    if (data_fd_buffered_ < 0) {
        LOG(ERROR) << "[FalconKVStore] Failed to open data file (buffered): " << data_file_
                   << ": " << strerror(errno);
        return Status::IoError("Failed to open data file: " + data_file_ +
                               ": " + strerror(errno));
    }

    // 2. If config enables O_DIRECT, open a second fd with O_DIRECT.
    if (config_.direct_io_enabled) {
        int direct_flags = buf_flags | O_DIRECT;
        data_fd_ = open(data_file_.c_str(), direct_flags, 0644);
        if (data_fd_ < 0) {
            LOG(WARNING) << "[FalconKVStore] O_DIRECT open failed, falling back to buffered only: "
                         << strerror(errno);
            config_.direct_io_enabled = false;
        }
    } else {
        data_fd_ = -1;
        LOG(INFO) << "[FalconKVStore] Direct IO disabled by config";
    }

    // 3. Preallocate the file to the configured capacity (on buffered fd).
    int rc = posix_fallocate(data_fd_buffered_, 0, static_cast<off_t>(config_.capacity_bytes));
    if (rc != 0) {
        // Some filesystems do not support fallocate; try truncate.
        if (ftruncate(data_fd_buffered_, static_cast<off_t>(config_.capacity_bytes)) != 0) {
            LOG(ERROR) << "[FalconKVStore] Failed to preallocate data file: " << data_file_
                       << ": fallocate error=" << strerror(rc)
                       << ", ftruncate error=" << strerror(errno);
            close(data_fd_buffered_);
            data_fd_buffered_ = -1;
            if (data_fd_ >= 0) {
                close(data_fd_);
                data_fd_ = -1;
            }
            return Status::IoError("Failed to preallocate data file: " +
                                   data_file_ + ": " + strerror(rc));
        }
    }

    // 4. Advise the kernel we don't need this data cached.
    posix_fadvise(data_fd_buffered_, 0, 0, POSIX_FADV_DONTNEED);

    return Status::OK();
}

// ---------------------------------------------------------------------------
// Write - single write with dual-fd: use O_DIRECT fd when aligned, buffered fd otherwise
// ---------------------------------------------------------------------------
Status FalconKVStore::Write(uint64_t offset, const void* data, uint32_t size) {
    if (data_fd_buffered_ < 0) {
        LOG(ERROR) << "[FalconKVStore] Write: data file not initialized";
        return Status::IoError("Data file not initialized");
    }
    if (!data || size == 0) {
        LOG(ERROR) << "[FalconKVStore] Write: null data or zero size, offset=" << offset;
        return Status::InvalidArg("Write: null data or zero size");
    }

    uint32_t page_size = config_.page_size;

    // Select fd: use O_DIRECT fd when all alignment conditions are met,
    // otherwise fall back to the buffered fd.
    int fd = data_fd_buffered_;
    if (config_.direct_io_enabled && data_fd_ >= 0 &&
        IsAligned(offset, page_size) &&
        IsAligned(size, page_size) &&
        IsAligned(reinterpret_cast<uintptr_t>(data), page_size)) {
        fd = data_fd_;
    }

    ssize_t written = pwrite(fd, data, size, static_cast<off_t>(offset));
    if (written != static_cast<ssize_t>(size)) {
        LOG(ERROR) << "[FalconKVStore] Write failed at offset " << offset
                   << ", size=" << size << ": " << strerror(errno);
        return Status::IoError("Write failed at offset " + std::to_string(offset) +
                               ": " + strerror(errno));
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Read - single read with dual-fd: use O_DIRECT fd when aligned, buffered fd otherwise
// ---------------------------------------------------------------------------
Status FalconKVStore::Read(uint64_t offset, void* buffer, uint32_t size) {
    if (data_fd_buffered_ < 0) {
        LOG(ERROR) << "[FalconKVStore] Read: data file not initialized";
        return Status::IoError("Data file not initialized");
    }
    if (!buffer || size == 0) {
        LOG(ERROR) << "[FalconKVStore] Read: null buffer or zero size, offset=" << offset;
        return Status::InvalidArg("Read: null buffer or zero size");
    }

    uint32_t page_size = config_.page_size;

    // Select fd: use O_DIRECT fd when all alignment conditions are met,
    // otherwise fall back to the buffered fd.
    int fd = data_fd_buffered_;
    if (config_.direct_io_enabled && data_fd_ >= 0 &&
        IsAligned(offset, page_size) &&
        IsAligned(size, page_size) &&
        IsPtrAligned(buffer, page_size)) {
        fd = data_fd_;
    }

    ssize_t rd = pread(fd, buffer, size, static_cast<off_t>(offset));
    if (rd < 0) {
        LOG(ERROR) << "[FalconKVStore] Read failed at offset " << offset
                   << ", size=" << size << ": " << strerror(errno);
        return Status::IoError("Read failed at offset " + std::to_string(offset) +
                               ": " + strerror(errno));
    }
    if (rd < static_cast<ssize_t>(size)) {
        LOG(ERROR) << "[FalconKVStore] Short read at offset " << offset
                   << ": expected " << size << " got " << rd;
        return Status::IoError("Short read at offset " + std::to_string(offset) +
                               ": expected " + std::to_string(size) +
                               " got " + std::to_string(rd));
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// BatchWrite - parallel writes via io_uring or thread pool
// ---------------------------------------------------------------------------
Status FalconKVStore::BatchWrite(const std::vector<WriteItem>& items) {
    if (items.empty()) {
        return Status::OK();
    }

    if (io_uring_enabled_) {
        std::vector<UringIORequest> reqs(items.size());
        for (size_t i = 0; i < items.size(); ++i) {
            reqs[i].offset = items[i].offset;
            reqs[i].size = items[i].size;
            reqs[i].buffer = const_cast<void*>(items[i].data);
            // Per-request fd selection: use O_DIRECT fd when all alignment
            // conditions are met, otherwise fall back to buffered fd.
            if (config_.direct_io_enabled && data_fd_ >= 0 &&
                IsAligned(items[i].offset, config_.page_size) &&
                IsAligned(items[i].size, config_.page_size) &&
                IsAligned(reinterpret_cast<uintptr_t>(items[i].data), config_.page_size)) {
                reqs[i].fd = data_fd_;
            } else {
                reqs[i].fd = data_fd_buffered_;
            }
        }
        // Pass -1 as batch-level fd; each request carries its own fd.
        auto results = io_uring_engine_->BatchWrite(/*fd=*/-1, reqs);
        Status last_error = Status::OK();
        for (auto& r : results) {
            if (!r.status.ok()) {
                LOG(ERROR) << "[FalconKVStore] BatchWrite (io_uring): write failed at index "
                           << r.index << ": " << r.status.ToString();
                last_error = r.status;
            }
        }
        return last_error;
    }

    // Fallback: thread pool
    std::vector<std::function<Status()>> tasks;
    tasks.reserve(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        tasks.push_back([this, &items, i]() {
            return Write(items[i].offset, items[i].data, items[i].size);
        });
    }
    auto statuses = io_pool_->SubmitAndWait(std::move(tasks));
    Status last_error = Status::OK();
    for (auto& s : statuses) {
        if (!s.ok()) {
            last_error = s;
        }
    }
    return last_error;
}

// ---------------------------------------------------------------------------
// BatchRead - parallel reads via io_uring or thread pool
// ---------------------------------------------------------------------------
Status FalconKVStore::BatchRead(const std::vector<ReadItem>& items) {
    if (items.empty()) {
        return Status::OK();
    }

    if (io_uring_enabled_) {
        std::vector<UringIORequest> reqs(items.size());
        for (size_t i = 0; i < items.size(); ++i) {
            reqs[i].offset = items[i].offset;
            reqs[i].size = items[i].size;
            reqs[i].buffer = items[i].buffer;
            // Per-request fd selection: use O_DIRECT fd when all alignment
            // conditions are met, otherwise fall back to buffered fd.
            if (config_.direct_io_enabled && data_fd_ >= 0 &&
                IsAligned(items[i].offset, config_.page_size) &&
                IsAligned(items[i].size, config_.page_size) &&
                IsPtrAligned(items[i].buffer, config_.page_size)) {
                reqs[i].fd = data_fd_;
            } else {
                reqs[i].fd = data_fd_buffered_;
            }
        }
        // Pass -1 as batch-level fd; each request carries its own fd.
        auto results = io_uring_engine_->BatchRead(/*fd=*/-1, reqs);
        Status last_error = Status::OK();
        for (auto& r : results) {
            if (!r.status.ok()) {
                LOG(ERROR) << "[FalconKVStore] BatchRead (io_uring): read failed at index "
                           << r.index << ": " << r.status.ToString();
                last_error = r.status;
            }
        }
        return last_error;
    }

    // Fallback: thread pool
    std::vector<std::function<Status()>> tasks;
    tasks.reserve(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        tasks.push_back([this, &items, i]() {
            return Read(items[i].offset, items[i].buffer, items[i].size);
        });
    }
    auto statuses = io_pool_->SubmitAndWait(std::move(tasks));
    Status last_error = Status::OK();
    for (auto& s : statuses) {
        if (!s.ok()) {
            last_error = s;
        }
    }
    return last_error;
}

// ---------------------------------------------------------------------------
// Key-aware API: Put
// ---------------------------------------------------------------------------
StorePutResult FalconKVStore::Put(const std::string& key, const void* data,
                                   uint32_t size) {
    StorePutResult result;

    // 0. Skip if key already exists — avoid space leak from overwriting
    //    the old StoreKeyRecord without freeing its allocated offset.
    if (meta_index_->Get(key).has_value()) {
        LOG(INFO) << "[FalconKVStore] Put: key already exists, skipping: " << key;
        result.status = Status::OK();
        return result;
    }

    // 1. Allocate space via SlotAllocator
    uint32_t alloc_size = 0;
    int64_t offset = allocator_->Alloc(size, &alloc_size);
    if (offset < 0) {
        LOG(WARNING) << "[FalconKVStore] Put: out of space for key: " << key
                     << ", triggering forced eviction";

        // Level 1: Flush expired pending evictions and retry.
        pending_evict_queue_->FlushExpired();
        offset = allocator_->Alloc(size, &alloc_size);

        // Level 2: Force evict LRU entries + flush expired and retry.
        // Evict at least 5% of total capacity to amortize the cost.
        if (offset < 0) {
            uint32_t evict_target = std::max(
                size, static_cast<uint32_t>(allocator_->GetTotalBytes() / 20));
            evict_manager_->ForceEvict(evict_target);
            pending_evict_queue_->FlushExpired();
            offset = allocator_->Alloc(size, &alloc_size);
        }

        // Level 3: Flush ALL pending evictions (ignore grace period) and retry.
        if (offset < 0) {
            pending_evict_queue_->FlushAllForced();
            offset = allocator_->Alloc(size, &alloc_size);
        }

        if (offset < 0) {
            LOG(ERROR) << "[FalconKVStore] Put: still out of space after forced eviction for key: "
                       << key;
            result.status = Status::NoSpace("Store out of space for key: " + key);
            return result;
        }
    }

    result.offset = static_cast<uint64_t>(offset);
    result.alloc_size = alloc_size;

    // 2. Write data to SSD
    Status write_status = Write(static_cast<uint64_t>(offset), data, size);
    if (!write_status.ok()) {
        allocator_->Free(offset, alloc_size);
        LOG(ERROR) << "[FalconKVStore] Put: write failed for key: " << key
                   << " at offset " << offset << ": " << write_status.ToString();
        result.status = write_status;
        return result;
    }

    // 3. Record in local metadata index (committed immediately)
    StoreKeyRecord record;
    record.key = key;
    record.offset = result.offset;
    record.size = size;
    record.alloc_size = alloc_size;
    record.stat = 1; // committed
    record.access_time_ms = GetWallTimeMs();
    meta_index_->Put(key, record);

    // 4. Sync to Meta (best effort, non-blocking for now)
    if (meta_sync_client_) {
        meta_sync_client_->SyncCommit(store_id_, {record});
    }

    result.status = Status::OK();
    return result;
}

// ---------------------------------------------------------------------------
// Key-aware API: BatchPut (three-phase pipeline)
// Phase 1: Batch allocate space (sequential, allocator is thread-safe)
// Phase 2: Parallel IO write (io_uring or thread pool)
// Phase 3: Batch metadata update + Meta sync
// ---------------------------------------------------------------------------
std::vector<StorePutResult> FalconKVStore::BatchPut(
    const std::vector<std::string>& keys,
    const std::vector<const void*>& data_ptrs,
    const std::vector<uint32_t>& sizes) {

    std::vector<StorePutResult> results(keys.size());

    if (keys.size() != data_ptrs.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "[FalconKVStore] BatchPut: size mismatch, keys=" << keys.size()
                   << " data_ptrs=" << data_ptrs.size() << " sizes=" << sizes.size();
        for (size_t i = 0; i < keys.size(); ++i) {
            results[i].status = Status::InvalidArg("keys/data_ptrs/sizes size mismatch");
        }
        return results;
    }

    // ---- Phase 1: Batch allocate space ----
    // First, identify which keys need allocation (skip existing ones).
    std::vector<size_t> need_alloc_indices;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto& result = results[i];
        if (meta_index_->Get(keys[i]).has_value()) {
            result.status = Status::OK();
            continue;
        }
        need_alloc_indices.push_back(i);
    }

    // Batch-allocate all needed slots in one lock acquisition.
    if (!need_alloc_indices.empty()) {
        uint32_t alloc_size = 0;
        std::vector<int64_t> batch_offsets;
        uint32_t alloc_count = allocator_->BatchAlloc(
            sizes[need_alloc_indices[0]],
            static_cast<uint32_t>(need_alloc_indices.size()),
            batch_offsets, &alloc_size);

        // Assign successfully allocated offsets
        for (uint32_t j = 0; j < alloc_count; ++j) {
            size_t idx = need_alloc_indices[j];
            results[idx].offset = static_cast<uint64_t>(batch_offsets[j]);
            results[idx].alloc_size = alloc_size;
            results[idx].status = Status::OK();
        }

        // Handle keys that could not be allocated (out of space with eviction)
        for (size_t j = alloc_count; j < need_alloc_indices.size(); ++j) {
            size_t idx = need_alloc_indices[j];
            LOG(WARNING) << "[FalconKVStore] BatchPut: out of space for key: " << keys[idx]
                         << ", triggering forced eviction";

            pending_evict_queue_->FlushExpired();
            int64_t offset = allocator_->Alloc(sizes[idx], &alloc_size);

            if (offset < 0) {
                uint32_t evict_target = std::max(
                    sizes[idx], static_cast<uint32_t>(allocator_->GetTotalBytes() / 20));
                evict_manager_->ForceEvict(evict_target);
                pending_evict_queue_->FlushExpired();
                offset = allocator_->Alloc(sizes[idx], &alloc_size);
            }

            if (offset < 0) {
                pending_evict_queue_->FlushAllForced();
                offset = allocator_->Alloc(sizes[idx], &alloc_size);
            }

            if (offset < 0) {
                LOG(ERROR) << "[FalconKVStore] BatchPut: still out of space after forced eviction for key: "
                           << keys[idx];
                results[idx].status = Status::NoSpace("Store out of space for key: " + keys[idx]);
            } else {
                results[idx].offset = static_cast<uint64_t>(offset);
                results[idx].alloc_size = alloc_size;
                results[idx].status = Status::OK();
            }
        }
    }

    // ---- Phase 2: Parallel IO write ----
    // Collect all items that need to be written.
    std::vector<WriteItem> write_items;
    std::vector<size_t> write_indices; // maps write_items index -> original index
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!results[i].status.ok() ||
            (results[i].offset == 0 && results[i].alloc_size == 0)) {
            // Skip: allocation failed or key already exists (offset=0, alloc_size=0)
            continue;
        }
        write_items.push_back({results[i].offset, data_ptrs[i], sizes[i]});
        write_indices.push_back(i);
    }

    if (!write_items.empty()) {
        Status write_status = BatchWrite(write_items);
        if (!write_status.ok()) {
            // On failure, mark all written items as failed and free space.
            for (size_t j = 0; j < write_items.size(); ++j) {
                size_t orig_idx = write_indices[j];
                results[orig_idx].status = write_status;
                allocator_->Free(static_cast<int64_t>(results[orig_idx].offset),
                                 results[orig_idx].alloc_size);
                results[orig_idx].offset = 0;
                results[orig_idx].alloc_size = 0;
            }
            return results;
        }
    }

    // ---- Phase 3: Batch metadata update + Meta sync ----
    std::vector<StoreKeyRecord> committed_records;
    for (size_t j = 0; j < write_items.size(); ++j) {
        size_t orig_idx = write_indices[j];
        StoreKeyRecord record;
        record.key = keys[orig_idx];
        record.offset = results[orig_idx].offset;
        record.size = sizes[orig_idx];
        record.alloc_size = results[orig_idx].alloc_size;
        record.stat = 1;
        record.access_time_ms = GetWallTimeMs();
        meta_index_->Put(keys[orig_idx], record);
        committed_records.push_back(record);
        results[orig_idx].status = Status::OK();
    }

    // Async sync to Meta (non-blocking)
    if (meta_sync_client_ && !committed_records.empty()) {
        meta_sync_client_->AsyncCommit(store_id_, committed_records);
    }

    return results;
}

// ---------------------------------------------------------------------------
// Key-aware API: Get
// ---------------------------------------------------------------------------
StoreGetResult FalconKVStore::Get(const std::string& key, void* buffer,
                                   uint32_t buffer_size) {
    StoreGetResult result;

    // 1. Look up in local metadata index
    auto record = meta_index_->Get(key);
    if (!record.has_value()) {
        result.status = Status::NotFound("Key not found in store: " + key);
        return result;
    }

    if (buffer_size < record->size) {
        result.status = Status::InvalidArg("Buffer too small for key: " + key);
        return result;
    }

    // 2. Read from SSD
    Status read_status = Read(record->offset, buffer, record->size);
    if (!read_status.ok()) {
        result.status = read_status;
        return result;
    }

    result.size = record->size;
    result.status = Status::OK();

    // Note: StoreMetaIndex::Get() already touches the LRU entry and updates
    // access_time_ms, so no explicit Touch() call is needed here.

    return result;
}

// ---------------------------------------------------------------------------
// Key-aware API: Contains
// ---------------------------------------------------------------------------
bool FalconKVStore::Contains(const std::string& key) {
    return meta_index_->Get(key).has_value();
}

// ---------------------------------------------------------------------------
// Key-aware API: BatchContains
// ---------------------------------------------------------------------------
void FalconKVStore::BatchContains(const std::vector<std::string>& keys,
                                    std::vector<StoreKeyRecord>& hits,
                                    std::vector<std::string>& misses) {
    meta_index_->BatchContains(keys, hits, misses);
}

// ---------------------------------------------------------------------------
// Key-aware API: Remove
// ---------------------------------------------------------------------------
Status FalconKVStore::Remove(const std::string& key) {
    // 1. Sync removal to Meta first (wait for confirmation).
    if (meta_sync_client_) {
        Status s = meta_sync_client_->SyncRemove(store_id_, {key});
        if (!s.ok()) {
            LOG(ERROR) << "[FalconKVStore] Remove: SyncRemove failed for key: " << key
                       << ": " << s.ToString();
            return s;
        }
    }

    // 2. Remove from local index.
    auto record = meta_index_->Remove(key);
    if (!record.has_value()) {
        LOG(WARNING) << "[FalconKVStore] Remove: key not found in local index: " << key;
        return Status::NotFound("Key not found for removal: " + key);
    }

    // 3. Enqueue for deferred space reclamation (grace period).
    pending_evict_queue_->Enqueue(key, record->offset, record->alloc_size);

    return Status::OK();
}

// ---------------------------------------------------------------------------
// Key-aware API: GetUsageRatio
// ---------------------------------------------------------------------------
double FalconKVStore::GetUsageRatio() const {
    if (!allocator_) return 0.0;
    return allocator_->GetUsageRatio();
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------
void FalconKVStore::Close() {
    if (!running_.exchange(false)) {
        return;
    }

    // Stop background threads before releasing resources they depend on.
    evict_manager_.reset();
    pending_evict_queue_.reset();

    {
        std::lock_guard<std::mutex> lock(hixl_lease_mutex_);
        if (hixl_staging_pool_) {
            for (const auto& kv : hixl_read_leases_) {
                for (const auto& lease : kv.second.leases) {
                    hixl_staging_pool_->Release(lease);
                }
            }
        }
        hixl_read_leases_.clear();
    }
    hixl_staging_pool_.reset();
    if (hixl_transport_) {
        hixl_transport_->Close();
        hixl_transport_.reset();
    }
    hixl_read_ready_ = false;
    hixl_engine_addr_.clear();

    // Close io_uring first, then stop thread pool.
    if (io_uring_engine_) {
        io_uring_engine_->Close();
        io_uring_engine_.reset();
    }
    io_uring_enabled_ = false;

    if (io_pool_) {
        io_pool_->Stop();
        io_pool_.reset();
    }

    scheduler_proxy_.reset();
    if (meta_sync_client_) {
        meta_sync_client_->StopReconnectLoop();
    }
    meta_sync_client_.reset();
    meta_index_.reset();
    allocator_.reset();
    buffer_pool_.reset();

    if (data_fd_buffered_ >= 0) {
        fsync(data_fd_buffered_);
        close(data_fd_buffered_);
        data_fd_buffered_ = -1;
    }

    if (data_fd_ >= 0) {
        close(data_fd_);
        data_fd_ = -1;
    }
}

} // namespace falconkv
