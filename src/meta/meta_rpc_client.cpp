#include "src/meta/meta_rpc_client.h"

#include <brpc/controller.h>

#include "src/common/logging.h"

namespace falconkv {

MetaRpcClient::MetaRpcClient() = default;

MetaRpcClient::~MetaRpcClient() {
    StopReconnectLoop();
}

// -----------------------------------------------------------------
// Connect / TryConnect
// -----------------------------------------------------------------

Status MetaRpcClient::TryConnect() {
    if (meta_addr_.empty()) {
        return Status::OK();
    }

    auto new_channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.timeout_ms = 5000;
    options.connect_timeout_ms = 3000;
    options.max_retry = 3;

    int rc = new_channel->Init(meta_addr_.c_str(), &options);
    if (rc != 0) {
        LOG(ERROR) << "[MetaRpcClient] TryConnect: failed to connect to meta at "
                   << meta_addr_ << ", rc=" << rc;
        return Status::RpcError("failed to connect to meta server at " + meta_addr_);
    }

    channel_ = std::move(new_channel);
    stub_ = std::make_unique<FalconKVMetaService_Stub>(channel_.get());
    connected_.store(true);
    LOG(INFO) << "[MetaRpcClient] Connected to Meta at " << meta_addr_;
    return Status::OK();
}

Status MetaRpcClient::Connect(const std::string& addr) {
    meta_addr_ = addr;
    if (meta_addr_.empty()) {
        return Status::OK();
    }

    Status s = TryConnect();
    if (!s.ok()) {
        LOG(WARNING) << "[MetaRpcClient] Connect failed: " << s.ToString()
                     << " (will retry later)";
    }
    return Status::OK();
}

// -----------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------

KeyRecord MetaRpcClient::ProtoToKeyRecord(const KeyDesc& desc) {
    KeyRecord rec;
    rec.key = desc.key();
    rec.store_id = desc.store_id();
    rec.offset = desc.offset();
    rec.size = desc.size();
    rec.stat = 1; // BatchExist/BatchLookup only return committed records
    if (desc.has_node_id()) {
        rec.node_id = desc.node_id();
    }
    if (desc.has_data_file()) {
        rec.data_file = desc.data_file();
    }
    if (desc.has_store_addr()) {
        rec.store_addr = desc.store_addr();
    }
    if (desc.has_hixl_engine_addr()) {
        rec.hixl_engine_addr = desc.hixl_engine_addr();
    }
    return rec;
}

// -----------------------------------------------------------------
// BatchExist
// -----------------------------------------------------------------

std::vector<KeyRecord> MetaRpcClient::BatchExist(
    const std::vector<std::string>& keys) {
    // Early return when disconnected — avoid RPC timeout.
    if (!connected_.load() || !stub_) {
        return std::vector<KeyRecord>(keys.size());
    }

    BatchExistRequest request;
    for (const auto& key : keys) {
        request.add_keys(key);
    }

    BatchExistResponse response;
    brpc::Controller cntl;

    stub_->BatchExist(&cntl, &request, &response, nullptr);

    std::vector<KeyRecord> results;
    results.reserve(keys.size());

    if (cntl.Failed()) {
        connected_.store(false);
        LOG(ERROR) << "[MetaRpcClient] BatchExist RPC failed for " << keys.size()
                   << " keys: " << cntl.ErrorText();
        // RPC failed — return empty records for all keys
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back();
        }
        return results;
    }

    // The response may contain fewer entries than input keys (only hits).
    // Build a set of hit keys for O(1) lookup.
    std::unordered_map<std::string, KeyRecord> hit_map;
    for (int i = 0; i < response.key_descs_size(); ++i) {
        auto rec = ProtoToKeyRecord(response.key_descs(i));
        if (!rec.key.empty()) {
            hit_map[rec.key] = std::move(rec);
        }
    }

    for (const auto& key : keys) {
        auto it = hit_map.find(key);
        if (it != hit_map.end()) {
            results.push_back(it->second);
        } else {
            results.emplace_back();
        }
    }

    return results;
}

// -----------------------------------------------------------------
// BatchLookup
// -----------------------------------------------------------------

std::vector<KeyRecord> MetaRpcClient::BatchLookup(
    const std::vector<std::string>& keys) {
    // Early return when disconnected — avoid RPC timeout.
    if (!connected_.load() || !stub_) {
        return std::vector<KeyRecord>(keys.size());
    }

    BatchLookupRequest request;
    for (const auto& key : keys) {
        request.add_keys(key);
    }

    BatchLookupResponse response;
    brpc::Controller cntl;

    stub_->BatchLookup(&cntl, &request, &response, nullptr);

    std::vector<KeyRecord> results;
    results.reserve(keys.size());

    if (cntl.Failed()) {
        connected_.store(false);
        LOG(ERROR) << "[MetaRpcClient] BatchLookup RPC failed for " << keys.size()
                   << " keys: " << cntl.ErrorText();
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back();
        }
        return results;
    }

    // Response contains entries for found keys (empty-key entries for misses).
    // Since the service preserves ordering, use direct mapping.
    if (response.key_descs_size() == static_cast<int>(keys.size())) {
        for (int i = 0; i < response.key_descs_size(); ++i) {
            results.push_back(ProtoToKeyRecord(response.key_descs(i)));
        }
    } else {
        // Fallback: build a map
        std::unordered_map<std::string, KeyRecord> hit_map;
        for (int i = 0; i < response.key_descs_size(); ++i) {
            auto rec = ProtoToKeyRecord(response.key_descs(i));
            if (!rec.key.empty()) {
                hit_map[rec.key] = std::move(rec);
            }
        }
        for (const auto& key : keys) {
            auto it = hit_map.find(key);
            if (it != hit_map.end()) {
                results.push_back(it->second);
            } else {
                results.emplace_back();
            }
        }
    }

    return results;
}

// -----------------------------------------------------------------
// Reconnect loop
// -----------------------------------------------------------------

void MetaRpcClient::StartReconnectLoop(int interval_sec) {
    if (reconnect_running_.load()) {
        return;
    }
    reconnect_running_.store(true);
    reconnect_thread_ = std::thread(&MetaRpcClient::ReconnectLoop, this, interval_sec);
}

void MetaRpcClient::StopReconnectLoop() {
    if (!reconnect_running_.load()) {
        return;
    }
    reconnect_running_.store(false);
    reconnect_cv_.notify_all();
    if (reconnect_thread_.joinable()) {
        reconnect_thread_.join();
    }
}

void MetaRpcClient::ReconnectLoop(int interval_sec) {
    while (reconnect_running_.load()) {
        {
            std::unique_lock<std::mutex> lock(reconnect_mutex_);
            reconnect_cv_.wait_for(lock,
                                    std::chrono::seconds(interval_sec),
                                    [this] { return !reconnect_running_.load(); });
        }

        if (!reconnect_running_.load()) {
            break;
        }

        if (connected_.load()) {
            continue;
        }

        Status s = TryConnect();
        if (s.ok()) {
            LOG(INFO) << "[MetaRpcClient] Reconnected to Meta at " << meta_addr_;
        }
    }
}

} // namespace falconkv
