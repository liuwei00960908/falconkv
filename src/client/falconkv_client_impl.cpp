#include "client/falconkv_client_impl.h"

#include <algorithm>
#include <unordered_map>

#include "src/common/logging.h"

namespace falconkv {

FalconKVClientImpl::FalconKVClientImpl(const Config& config)
    : config_(config),
      key_desc_cache_(config.cache_capacity) {
    // Load config
    FalconKVConfig cfg;
    if (!config_.config_file.empty()) {
        cfg = ConfigLoader::LoadFromFile(config_.config_file);
    }
    if (!config_.hixl_engine_addr.empty()) {
        cfg.client.hixl_engine_addr = config_.hixl_engine_addr;
    }
    if (config_.hixl_device_id >= 0) {
        cfg.client.hixl_device_id = config_.hixl_device_id;
    }

    // Read node_id from config (client section takes precedence)
    node_id_ = cfg.client.node_id;
    if (node_id_ == 0) {
        node_id_ = cfg.store.node_id;
    }

    // Build self store address for remote reads (store_rpc_host:listen_port)
    // Prefer externally provided address (from Store object) to avoid
    // inconsistency when Bridge modifies the port after loading config.
    if (!config_.store_rpc_addr.empty()) {
        self_store_addr_ = config_.store_rpc_addr;
    } else if (!config_.config_file.empty()) {
        self_store_addr_ = cfg.store.store_rpc_host + ":"
                         + std::to_string(cfg.store.listen_port);
    }

    // Connect to remote Meta server via RPC
    Status meta_status = meta_client_.Connect(cfg.transfer.meta_addr);
    if (meta_status.ok()) {
        LOG(INFO) << "[FalconKVClient] Connected to Meta server at "
                  << cfg.transfer.meta_addr;
    } else {
        LOG(WARNING) << "[FalconKVClient] Failed to connect to Meta server: "
                     << meta_status.ToString()
                     << " (will retry via background reconnect loop)";
    }

    // Initialize SchedulerProxy if enabled.
    // Prefer settings from the loaded JSON config when a config file is given.
    bool sched_enabled = config_.scheduler_enabled;
    std::string sched_uds = config_.scheduler_uds_path;
    if (!config_.config_file.empty()) {
        sched_enabled = cfg.common.scheduler_enabled;
        if (!cfg.common.scheduler_uds_path.empty()) {
            sched_uds = cfg.common.scheduler_uds_path;
        }
    }
    scheduler_enabled_ = sched_enabled;
    if (scheduler_enabled_) {
        scheduler_proxy_ = std::make_unique<SchedulerProxy>(sched_uds,
                                                             cfg.client.scheduler_rpc_timeout_us / 1000,
                                                             cfg.client.max_consecutive_failures,
                                                             cfg.client.reconnect_interval_sec);
        LOG(INFO) << "[FalconKVClient] SchedulerProxy enabled, uds_path=" << sched_uds;
    } else {
        LOG(INFO) << "[FalconKVClient] SchedulerProxy disabled";
    }

    // Start background reconnect loop for Meta RPC client.
    meta_client_.StartReconnectLoop(5);

    // Initialize NodeLocalAccessor IO engines for batch NODE_DIRECT reads
    NodeLocalAccessor::Config nla_cfg;
    nla_cfg.page_size = cfg.store.page_size;
    nla_cfg.io_threads = cfg.store.io_threads;
    nla_cfg.io_uring_enabled = cfg.store.io_uring_enabled;
    nla_cfg.io_uring_queue_depth = cfg.store.io_uring_queue_depth;
    node_accessor_.InitIOEngines(nla_cfg);

    // Pass max_body_size config to StoreRpcClientManager for batch splitting
    store_rpc_mgr_.SetMaxBodySize(cfg.transfer.max_body_size_mb * 1024ULL * 1024ULL);
    // Reuse store_pool_size as the parallelism for split remote BatchRead RPCs.
    store_rpc_mgr_.SetMaxParallelSubBatches(cfg.transfer.store_pool_size);
    store_rpc_mgr_.SetStreamReadConfig(
        cfg.transfer.remote_read_stream_enabled,
        cfg.transfer.remote_read_chunk_size_mb * 1024U * 1024U,
        cfg.transfer.remote_read_prefetch_chunks,
        cfg.transfer.remote_read_queue_chunks);
    HixlTransportConfig hixl_transport_cfg;
    hixl_transport_cfg.local_engine = cfg.client.hixl_engine_addr;
    hixl_transport_cfg.protocol_desc = cfg.transfer.hixl_protocol_desc;
    hixl_transport_cfg.local_comm_res = cfg.transfer.hixl_local_comm_res;
    hixl_transport_cfg.mem_type = cfg.transfer.hixl_mem_type;
    hixl_transport_cfg.device_id = cfg.client.hixl_device_id;
    hixl_transport_cfg.connect_timeout_ms = cfg.transfer.hixl_connect_timeout_ms;
    store_rpc_mgr_.SetHixlReadConfig(
        cfg.transfer.remote_read_transport == "hixl",
        hixl_transport_cfg,
        cfg.transfer.hixl_receive_chunk_size_mb,
        cfg.transfer.hixl_receive_chunk_count,
        cfg.transfer.hixl_min_read_size_bytes,
        cfg.transfer.hixl_transfer_timeout_ms,
        cfg.transfer.hixl_fallback_to_brpc);
}

FalconKVClientImpl::~FalconKVClientImpl() {
    Close();
}

// -----------------------------------------------------------------
// GetStoreRpcClient
// -----------------------------------------------------------------
StoreRpcClient* FalconKVClientImpl::GetStoreRpcClient(uint32_t store_id) {
    auto it = store_addr_map_.find(store_id);
    if (it == store_addr_map_.end()) {
        return nullptr;
    }
    return store_rpc_mgr_.GetOrCreate(it->second);
}

// -----------------------------------------------------------------
// Scheduler helpers
// -----------------------------------------------------------------
int FalconKVClientImpl::ChannelForAccessType(AccessType type) {
    switch (type) {
        case AccessType::ACCESS_LOCAL_DIRECT:   return 0; // LOCAL_SSD_READ
        case AccessType::ACCESS_NODE_DIRECT:    return 0; // LOCAL_SSD_READ
        case AccessType::ACCESS_REMOTE_RPC:     return 2; // NET_TX_READ
        default: return 0;
    }
}

std::string FalconKVClientImpl::RemoteAddrForDesc(const KeyDescriptor& desc) {
    if (desc.access_type == AccessType::ACCESS_REMOTE_RPC) {
        if (!desc.store_addr.empty()) return desc.store_addr;
        auto it = store_addr_map_.find(desc.store_id);
        if (it != store_addr_map_.end()) return it->second;
    }
    return "";
}

// -----------------------------------------------------------------
// BatchExist
// -----------------------------------------------------------------
int FalconKVClientImpl::BatchExist(const std::vector<std::string>& keys,
                                    std::vector<KeyDescriptor>& hit_descs) {
    hit_descs.clear();

    if (keys.empty()) {
        return 0;
    }

    // Step 1: Check local Store metadata directly (no cache, to avoid stale
    // entries from evicted data).
    std::vector<StoreKeyRecord> store_hits;
    std::vector<std::string> missing_keys;
    if (local_store_) {
        local_store_->BatchContains(keys, store_hits, missing_keys);

        for (const auto& rec : store_hits) {
            KeyDescriptor desc(rec.key);
            desc.store_id = local_store_->store_id();
            desc.offset = rec.offset;
            desc.size = rec.size;
            desc.access_type = AccessType::ACCESS_LOCAL_DIRECT;
            hit_descs.push_back(desc);
            key_desc_cache_.Insert(rec.key, desc);
        }
    } else {
        missing_keys = keys;
    }

    // Step 2: For keys still missing, query remote Meta
    if (!missing_keys.empty()) {
        auto records = meta_client_.BatchExist(missing_keys);
        for (size_t i = 0; i < records.size(); ++i) {
            if (!records[i].key.empty() && records[i].stat == 1) {
                KeyDescriptor desc(records[i].key);
                desc.store_id = records[i].store_id;
                desc.offset = records[i].offset;
                desc.size = records[i].size;
                desc.access_time_ms = records[i].access_time_ms;
                // Determine access type: same node -> NODE_DIRECT, else REMOTE_RPC
                if (records[i].node_id != 0 && records[i].node_id == node_id_) {
                    desc.access_type = AccessType::ACCESS_NODE_DIRECT;
                    // Register the store's data file with NodeLocalAccessor
                    if (!records[i].data_file.empty()) {
                        node_accessor_.RegisterStoreFile(
                            records[i].store_id, records[i].data_file);
                    }
                } else {
                    desc.access_type = AccessType::ACCESS_REMOTE_RPC;
                }
                // Populate store_addr_map_ and desc.store_addr for RPC routing
                if (!records[i].store_addr.empty()) {
                    store_addr_map_[records[i].store_id] = records[i].store_addr;
                    desc.store_addr = records[i].store_addr;
                }
                desc.hixl_engine_addr = records[i].hixl_engine_addr;
                hit_descs.push_back(desc);
                key_desc_cache_.Insert(records[i].key, desc);
            }
        }
    }

    return static_cast<int>(hit_descs.size());
}

// -----------------------------------------------------------------
// BatchPut
// -----------------------------------------------------------------
std::vector<Status> FalconKVClientImpl::BatchPut(
    const std::vector<std::string>& keys,
    const std::vector<BufferInfo>& buffers) {

    if (keys.size() != buffers.size()) {
        LOG(ERROR) << "[FalconKVClient] BatchPut: keys.size()=" << keys.size()
                   << " != buffers.size()=" << buffers.size();
        return {Status::InvalidArg("keys and buffers size mismatch")};
    }

    if (!local_store_) {
        LOG(ERROR) << "[FalconKVClient] BatchPut: local store not set";
        return {Status::IoError("local store not set")};
    }

    // Build data_ptrs and sizes for Store.BatchPut
    std::vector<const void*> data_ptrs;
    std::vector<uint32_t> sizes;
    data_ptrs.reserve(keys.size());
    sizes.reserve(keys.size());

    uint64_t total_io_size = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        data_ptrs.push_back(buffers[i].data_ptr);
        sizes.push_back(buffers[i].size);
        total_io_size += buffers[i].size;
    }

    // Scheduler: RequestIO for write
    uint64_t request_ts = GetCurrentTimeNs();
    IOResponseData io_resp;
    if (scheduler_enabled_) {
        IORequestData io_req;
        io_req.io_channel = 1;  // LOCAL_SSD_WRITE
        io_req.store_id = local_store_->store_id();
        io_req.io_size = total_io_size;
        io_req.request_ts_ns = request_ts;
        io_resp = scheduler_proxy_->RequestIO(io_req);
    }

    // Delegate to local store BatchPut
    uint64_t start_ts = GetCurrentTimeNs();
    auto results = local_store_->BatchPut(keys, data_ptrs, sizes);
    uint64_t done_ts = GetCurrentTimeNs();

    // Scheduler: ReportIOCompletion
    if (scheduler_enabled_) {
        IOCompletionData report;
        report.ticket = io_resp.ticket;
        report.io_start_ts_ns = start_ts;
        report.io_done_ts_ns = done_ts;
        report.io_size = total_io_size;
        report.io_channel = 1;  // LOCAL_SSD_WRITE
        report.io_status = 0;
        report.store_id = local_store_->store_id();
        for (const auto& r : results) {
            if (!r.status.ok()) { report.io_status = -1; break; }
        }
        scheduler_proxy_->ReportIOCompletion(report);
    }

    // Convert results and update KeyDescCache
    std::vector<Status> statuses(keys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        statuses[i] = results[i].status;

        // Only update KeyDescCache when a real write happened (alloc_size > 0).
        // When a key already exists, the store returns OK with offset=0,
        // alloc_size=0 — updating the cache would overwrite the correct entry.
        if (results[i].status.ok() && results[i].alloc_size > 0) {
            KeyDescriptor desc(keys[i]);
            desc.store_id = local_store_->store_id();
            desc.offset = results[i].offset;
            desc.size = buffers[i].size;
            desc.access_type = AccessType::ACCESS_LOCAL_DIRECT;
            key_desc_cache_.Insert(keys[i], desc);
        }
    }

    return statuses;
}

// -----------------------------------------------------------------
// BatchGet
// -----------------------------------------------------------------
std::vector<int32_t> FalconKVClientImpl::BatchGet(
    const std::vector<KeyDescriptor>& key_descs,
    const std::vector<BufferInfo>& buffers) {

    if (key_descs.size() != buffers.size()) {
        LOG(ERROR) << "[FalconKVClient] BatchGet: key_descs.size()="
                   << key_descs.size() << " != buffers.size()=" << buffers.size();
        return std::vector<int32_t>(key_descs.size(), -1);
    }

    std::vector<int32_t> bytes_read(key_descs.size(), 0);

    // Phase 1: Group LOCAL_DIRECT keys for batch read via local_store_
    std::vector<size_t> local_indices;
    std::vector<ReadItem> local_reads;

    for (size_t i = 0; i < key_descs.size(); ++i) {
        const auto& desc = key_descs[i];
        if (desc.access_type == AccessType::ACCESS_LOCAL_DIRECT && local_store_) {
            local_indices.push_back(i);
            local_reads.push_back({desc.offset, buffers[i].data_ptr, buffers[i].size});
        }
    }

    // Execute batch read for all LOCAL_DIRECT keys in one call
    if (!local_reads.empty()) {
        uint64_t total_local_io = 0;
        for (const auto& item : local_reads) {
            total_local_io += item.size;
        }

        // Scheduler: RequestIO for batch local read
        uint64_t request_ts = GetCurrentTimeNs();
        IOResponseData io_resp;
        if (scheduler_enabled_) {
            IORequestData io_req;
            io_req.io_channel = 0;  // LOCAL_SSD_READ
            io_req.store_id = local_store_->store_id();
            io_req.io_size = total_local_io;
            io_req.request_ts_ns = request_ts;
            io_resp = scheduler_proxy_->RequestIO(io_req);
        }

        uint64_t start_ts = GetCurrentTimeNs();
        Status batch_status = local_store_->BatchRead(local_reads);
        uint64_t done_ts = GetCurrentTimeNs();

        if (!batch_status.ok()) {
            LOG(WARNING) << "[BatchGet] BatchRead FAILED status="
                         << batch_status.ToString();
        }

        // Scheduler: ReportIOCompletion for batch
        if (scheduler_enabled_) {
            IOCompletionData report;
            report.ticket = io_resp.ticket;
            report.io_start_ts_ns = start_ts;
            report.io_done_ts_ns = done_ts;
            report.io_size = total_local_io;
            report.io_channel = 0;  // LOCAL_SSD_READ
            report.io_status = batch_status.ok() ? 0 : -1;
            report.store_id = local_store_->store_id();
            scheduler_proxy_->ReportIOCompletion(report);
        }

        // Mark results for LOCAL_DIRECT keys
        if (batch_status.ok()) {
            for (size_t idx : local_indices) {
                bytes_read[idx] = static_cast<int32_t>(buffers[idx].size);
            }
        } else {
            for (size_t idx : local_indices) {
                bytes_read[idx] = -1;
            }
        }
    }

    // Phase 2a: Batch NODE_DIRECT reads via NodeLocalAccessor::BatchRead
    {
        std::vector<size_t> node_indices;
        std::vector<NodeLocalReadRequest> node_requests;
        uint64_t total_node_io = 0;

        for (size_t i = 0; i < key_descs.size(); ++i) {
            const auto& desc = key_descs[i];
            if (desc.access_type == AccessType::ACCESS_NODE_DIRECT) {
                node_indices.push_back(i);
                node_requests.push_back({desc.store_id, desc.offset,
                                         buffers[i].data_ptr, buffers[i].size});
                total_node_io += buffers[i].size;
            }
        }

        if (!node_requests.empty()) {
            // Scheduler: RequestIO for batch node-direct read
            uint64_t request_ts = GetCurrentTimeNs();
            IOResponseData io_resp;
            if (scheduler_enabled_) {
                IORequestData io_req;
                io_req.io_channel = 0;  // LOCAL_SSD_READ
                io_req.io_size = total_node_io;
                io_req.request_ts_ns = request_ts;
                io_resp = scheduler_proxy_->RequestIO(io_req);
            }

            uint64_t start_ts = GetCurrentTimeNs();
            auto batch_results = node_accessor_.BatchRead(node_requests);
            uint64_t done_ts = GetCurrentTimeNs();

            // Scheduler: ReportIOCompletion for batch
            if (scheduler_enabled_) {
                IOCompletionData report;
                report.ticket = io_resp.ticket;
                report.io_start_ts_ns = start_ts;
                report.io_done_ts_ns = done_ts;
                report.io_size = total_node_io;
                report.io_channel = 0;  // LOCAL_SSD_READ
                bool all_ok = true;
                for (const auto& s : batch_results) {
                    if (!s.ok()) { all_ok = false; break; }
                }
                report.io_status = all_ok ? 0 : -1;
                scheduler_proxy_->ReportIOCompletion(report);
            }

            // Map results back
            for (size_t j = 0; j < node_indices.size(); ++j) {
                size_t idx = node_indices[j];
                if (batch_results[j].ok()) {
                    bytes_read[idx] = static_cast<int32_t>(buffers[idx].size);
                } else {
                    LOG(WARNING) << "[BatchGet] NodeLocalAccessor::BatchRead FAILED for key="
                                 << key_descs[idx].key
                                 << " status=" << batch_results[j].ToString();
                    bytes_read[idx] = -1;
                }
            }
        }
    }

    // Phase 2b: Batch REMOTE_RPC reads — group by store_id, one BatchRead RPC per store
    {
        std::unordered_map<uint32_t, std::vector<size_t>> rpc_groups;
        for (size_t i = 0; i < key_descs.size(); ++i) {
            if (key_descs[i].access_type == AccessType::ACCESS_REMOTE_RPC) {
                rpc_groups[key_descs[i].store_id].push_back(i);
            }
        }

        for (auto& [sid, indices] : rpc_groups) {
            StoreRpcClient* rpc = GetStoreRpcClient(sid);
            if (!rpc) {
                LOG(ERROR) << "[BatchGet] No RPC client for store " << sid
                           << ", failing " << indices.size() << " keys";
                for (size_t idx : indices) {
                    bytes_read[idx] = -1;
                }
                continue;
            }

            size_t n = indices.size();
            std::vector<uint64_t> offsets(n);
            std::vector<uint32_t> seg_sizes(n);
            std::vector<void*> seg_bufs(n);
            uint64_t total_rpc_io = 0;

            for (size_t j = 0; j < n; ++j) {
                size_t idx = indices[j];
                offsets[j] = key_descs[idx].offset;
                seg_sizes[j] = buffers[idx].size;
                seg_bufs[j] = buffers[idx].data_ptr;
                total_rpc_io += buffers[idx].size;
            }

            // Scheduler: RequestIO for batch RPC read
            uint64_t request_ts = GetCurrentTimeNs();
            IOResponseData io_resp;
            std::string remote_addr = RemoteAddrForDesc(key_descs[indices[0]]);
            if (scheduler_enabled_) {
                IORequestData io_req;
                io_req.io_channel = 2;  // NET_TX_READ
                io_req.store_id = sid;
                io_req.io_size = total_rpc_io;
                io_req.request_ts_ns = request_ts;
                io_req.remote_node_addr = remote_addr;
                io_resp = scheduler_proxy_->RequestIO(io_req);
            }

            uint64_t start_ts = GetCurrentTimeNs();
            std::vector<int32_t> rpc_results;
            Status batch_status = rpc->BatchRead(offsets, seg_sizes, seg_bufs, rpc_results,
                                                  self_store_addr_,
                                                  key_descs[indices[0]].hixl_engine_addr);
            uint64_t done_ts = GetCurrentTimeNs();

            if (!batch_status.ok()) {
                LOG(WARNING) << "[BatchGet] BatchRead RPC FAILED for store " << sid
                             << " (" << n << " keys): " << batch_status.ToString();
            }

            // Scheduler: ReportIOCompletion for batch
            if (scheduler_enabled_) {
                IOCompletionData report;
                report.ticket = io_resp.ticket;
                report.io_start_ts_ns = start_ts;
                report.io_done_ts_ns = done_ts;
                report.io_size = total_rpc_io;
                report.io_channel = 2;  // NET_TX_READ
                report.io_status = batch_status.ok() ? 0 : -1;
                report.store_id = sid;
                report.remote_node_addr = remote_addr;
                scheduler_proxy_->ReportIOCompletion(report);
            }

            // Map results back
            for (size_t j = 0; j < n; ++j) {
                size_t idx = indices[j];
                if (j < rpc_results.size() && rpc_results[j] > 0) {
                    bytes_read[idx] = rpc_results[j];
                } else {
                    bytes_read[idx] = -1;
                }
            }
        }
    }

    return bytes_read;
}

std::vector<int32_t> FalconKVClientImpl::BatchGetSync(
    const std::vector<std::string>& keys,
    const std::vector<BufferInfo>& buffers) {

    if (keys.size() != buffers.size()) {
        LOG(ERROR) << "[FalconKVClient] BatchGetSync: keys.size()=" << keys.size()
                   << " != buffers.size()=" << buffers.size();
        return std::vector<int32_t>(keys.size(), -1);
    }

    // Step 1: Lookup key descriptors from cache
    std::vector<KeyDescriptor> hit_descs;
    std::vector<std::string> missing_keys;
    key_desc_cache_.BatchLookup(keys, hit_descs, missing_keys);

    // Build a map of key -> descriptor for cache hits
    std::unordered_map<std::string, KeyDescriptor> hit_map;
    for (const auto& desc : hit_descs) {
        hit_map.emplace(desc.key, desc);
    }

    // Step 2: For missing keys, query remote MetaManager
    if (!missing_keys.empty()) {
        auto records = meta_client_.BatchLookup(missing_keys);
        for (size_t i = 0; i < records.size(); ++i) {
            if (!records[i].key.empty()) {
                KeyDescriptor desc(records[i].key);
                desc.store_id = records[i].store_id;
                desc.offset = records[i].offset;
                desc.size = records[i].size;
                desc.access_time_ms = records[i].access_time_ms;
                // Determine access type: same node -> NODE_DIRECT, else REMOTE_RPC
                if (records[i].node_id != 0 && records[i].node_id == node_id_) {
                    desc.access_type = AccessType::ACCESS_NODE_DIRECT;
                    // Register the store's data file with NodeLocalAccessor
                    if (!records[i].data_file.empty()) {
                        node_accessor_.RegisterStoreFile(
                            records[i].store_id, records[i].data_file);
                    }
                } else {
                    desc.access_type = AccessType::ACCESS_REMOTE_RPC;
                }
                // Populate store_addr_map_ and desc.store_addr for RPC routing
                if (!records[i].store_addr.empty()) {
                    store_addr_map_[records[i].store_id] = records[i].store_addr;
                    desc.store_addr = records[i].store_addr;
                }
                desc.hixl_engine_addr = records[i].hixl_engine_addr;
                hit_map.emplace(records[i].key, desc);
                key_desc_cache_.Insert(records[i].key, desc);
            }
        }
    }

    // Step 3: Build ordered key_descs matching the input keys order
    std::vector<KeyDescriptor> ordered_descs;
    std::vector<BufferInfo> ordered_bufs;
    std::vector<size_t> valid_indices;

    for (size_t i = 0; i < keys.size(); ++i) {
        auto it = hit_map.find(keys[i]);
        if (it != hit_map.end()) {
            ordered_descs.push_back(it->second);
            ordered_bufs.push_back(buffers[i]);
            valid_indices.push_back(i);
        }
    }

    // Step 4: Batch read
    auto read_results = BatchGet(ordered_descs, ordered_bufs);

    // Step 5: Map results back to original order
    std::vector<int32_t> results(keys.size(), -1);
    for (size_t i = 0; i < valid_indices.size(); ++i) {
        results[valid_indices[i]] = read_results[i];
    }

    return results;
}

void FalconKVClientImpl::Close() {
    meta_client_.StopReconnectLoop();
    node_accessor_.Close();
    key_desc_cache_.Clear();
    store_rpc_mgr_.CloseAll();
}

} // namespace falconkv
