#include "src/store/hixl_transport.h"

#include <mutex>
#include <unordered_set>

#include "src/common/logging.h"

#ifdef FALCONKV_HAS_HIXL
#include <map>

#include <acl/acl.h>
#include <hixl/hixl.h>
#endif

namespace falconkv {

struct HixlTransport::Impl {
    std::mutex mutex;
    std::unordered_set<std::string> connected_engines;
#ifdef FALCONKV_HAS_HIXL
    hixl::Hixl engine;
#endif
};

HixlTransport::HixlTransport()
    : impl_(std::make_unique<Impl>()) {}

HixlTransport::~HixlTransport() {
    Close();
}

Status HixlTransport::Init(const HixlTransportConfig& config) {
    if (config.local_engine.empty()) {
        return Status::InvalidArg("HiXL local engine is empty");
    }
    config_ = config;
#ifndef FALCONKV_HAS_HIXL
    return Status::NotSupported("FalconKV was built without HiXL support");
#else
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (initialized_) {
        return Status::OK();
    }

    if (config_.device_id >= 0) {
        aclError acl_rc = aclrtSetDevice(config_.device_id);
        if (acl_rc != ACL_ERROR_NONE) {
            return Status::RpcError("aclrtSetDevice failed: " +
                                    std::to_string(acl_rc));
        }
    }

    std::map<hixl::AscendString, hixl::AscendString> options;
    if (!config_.protocol_desc.empty()) {
        LOG(WARNING) << "[HixlTransport] hixl_protocol_desc is ignored for "
                     << "CANN 8.5 HiXL compatibility";
    }
    if (!config_.local_comm_res.empty()) {
        LOG(WARNING) << "[HixlTransport] hixl_local_comm_res is ignored for "
                     << "CANN 8.5 HiXL compatibility";
    }

    hixl::Status rc = impl_->engine.Initialize(config_.local_engine.c_str(), options);
    if (rc != hixl::SUCCESS) {
        return Status::RpcError("HiXL Initialize failed: " + std::to_string(rc));
    }
    initialized_ = true;
    LOG(INFO) << "[HixlTransport] Initialized local_engine=" << config_.local_engine;
    return Status::OK();
#endif
}

Status HixlTransport::RegisterMemory(void* addr, size_t len, void** handle) {
    if (!initialized_) {
        return Status::NotSupported("HiXL transport is not initialized");
    }
    if (!addr || len == 0 || !handle) {
        return Status::InvalidArg("invalid HiXL memory registration request");
    }
#ifndef FALCONKV_HAS_HIXL
    return Status::NotSupported("FalconKV was built without HiXL support");
#else
    std::lock_guard<std::mutex> lock(impl_->mutex);
    hixl::MemDesc desc{};
    desc.addr = reinterpret_cast<uintptr_t>(addr);
    desc.len = len;
    hixl::MemType type = (config_.mem_type == "device")
        ? hixl::MEM_DEVICE
        : hixl::MEM_HOST;
    hixl::MemHandle mem_handle = nullptr;
    hixl::Status rc = impl_->engine.RegisterMem(desc, type, mem_handle);
    if (rc != hixl::SUCCESS) {
        return Status::RpcError("HiXL RegisterMem failed: " + std::to_string(rc));
    }
    *handle = mem_handle;
    return Status::OK();
#endif
}

Status HixlTransport::DeregisterMemory(void* handle) {
    if (!initialized_ || !handle) {
        return Status::OK();
    }
#ifndef FALCONKV_HAS_HIXL
    return Status::OK();
#else
    std::lock_guard<std::mutex> lock(impl_->mutex);
    hixl::Status rc = impl_->engine.DeregisterMem(handle);
    if (rc != hixl::SUCCESS) {
        return Status::RpcError("HiXL DeregisterMem failed: " + std::to_string(rc));
    }
    return Status::OK();
#endif
}

Status HixlTransport::Connect(const std::string& remote_engine) {
    if (!initialized_) {
        return Status::NotSupported("HiXL transport is not initialized");
    }
    if (remote_engine.empty()) {
        return Status::InvalidArg("HiXL remote engine is empty");
    }
#ifndef FALCONKV_HAS_HIXL
    return Status::NotSupported("FalconKV was built without HiXL support");
#else
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->connected_engines.count(remote_engine) != 0) {
        return Status::OK();
    }
    hixl::Status rc = impl_->engine.Connect(remote_engine.c_str(),
                                            config_.connect_timeout_ms);
    if (rc != hixl::SUCCESS && rc != hixl::ALREADY_CONNECTED) {
        return Status::RpcError("HiXL Connect failed: " + std::to_string(rc));
    }
    impl_->connected_engines.insert(remote_engine);
    return Status::OK();
#endif
}

Status HixlTransport::Read(const std::string& remote_engine,
                           const std::vector<HixlReadOp>& ops,
                           uint32_t timeout_ms) {
    if (!initialized_) {
        return Status::NotSupported("HiXL transport is not initialized");
    }
    if (remote_engine.empty()) {
        return Status::InvalidArg("HiXL remote engine is empty");
    }
    if (ops.empty()) {
        return Status::OK();
    }
#ifndef FALCONKV_HAS_HIXL
    return Status::NotSupported("FalconKV was built without HiXL support");
#else
    std::vector<hixl::TransferOpDesc> descs;
    descs.reserve(ops.size());
    for (const auto& op : ops) {
        if (op.local_addr == 0 || op.remote_addr == 0 || op.len == 0) {
            return Status::InvalidArg("invalid HiXL read op");
        }
        descs.push_back({op.local_addr, op.remote_addr, op.len});
    }

    std::lock_guard<std::mutex> lock(impl_->mutex);
    hixl::Status rc = impl_->engine.TransferSync(remote_engine.c_str(),
                                                 hixl::READ,
                                                 descs,
                                                 timeout_ms);
    if (rc != hixl::SUCCESS) {
        return Status::RpcError("HiXL TransferSync READ failed: " +
                                std::to_string(rc));
    }
    return Status::OK();
#endif
}

void HixlTransport::Close() {
#ifdef FALCONKV_HAS_HIXL
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (initialized_) {
        impl_->engine.Finalize();
    }
#endif
    initialized_ = false;
}

} // namespace falconkv
