#include "src/meta/meta_service_impl.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "src/common/logging.h"

namespace falconkv {

MetaServiceImpl::MetaServiceImpl(MetaManager* meta_manager)
    : meta_manager_(meta_manager) {}

// -----------------------------------------------------------------
// Helper: convert C++ KeyRecord -> proto KeyDesc
// -----------------------------------------------------------------
static void KeyRecordToProto(const KeyRecord& rec, KeyDesc* desc) {
    desc->set_key(rec.key);
    desc->set_store_id(rec.store_id);
    desc->set_offset(rec.offset);
    desc->set_size(rec.size);
    desc->set_access_type(ACCESS_REMOTE_RPC);
    if (rec.node_id != 0) {
        desc->set_node_id(rec.node_id);
    }
    if (!rec.data_file.empty()) {
        desc->set_data_file(rec.data_file);
    }
    if (!rec.store_addr.empty()) {
        desc->set_store_addr(rec.store_addr);
    }
    if (!rec.hixl_engine_addr.empty()) {
        desc->set_hixl_engine_addr(rec.hixl_engine_addr);
    }
}

// -----------------------------------------------------------------
// BatchExist
// -----------------------------------------------------------------
void MetaServiceImpl::BatchExist(::google::protobuf::RpcController*,
                                 const BatchExistRequest* request,
                                 BatchExistResponse* response,
                                 ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    std::vector<std::string> keys;
    keys.reserve(request->keys_size());
    for (int i = 0; i < request->keys_size(); ++i) {
        keys.push_back(request->keys(i));
    }

    auto records = meta_manager_->BatchExist(keys);

    int hit_count = 0;
    for (const auto& rec : records) {
        auto* desc = response->add_key_descs();
        if (!rec.key.empty() && rec.stat == 1) {
            KeyRecordToProto(rec, desc);
            ++hit_count;
        }
    }
    response->set_hit_count(hit_count);
}

// -----------------------------------------------------------------
// BatchLookup
// -----------------------------------------------------------------
void MetaServiceImpl::BatchLookup(::google::protobuf::RpcController*,
                                  const BatchLookupRequest* request,
                                  BatchLookupResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    std::vector<std::string> keys;
    keys.reserve(request->keys_size());
    for (int i = 0; i < request->keys_size(); ++i) {
        keys.push_back(request->keys(i));
    }

    auto records = meta_manager_->BatchLookup(keys);

    for (const auto& rec : records) {
        auto* desc = response->add_key_descs();
        if (!rec.key.empty()) {
            KeyRecordToProto(rec, desc);
        }
    }
}

// -----------------------------------------------------------------
// StoreRegister
// -----------------------------------------------------------------
void MetaServiceImpl::StoreRegister(::google::protobuf::RpcController*,
                                    const StoreRegisterRequest* request,
                                    StoreRegisterResponse* response,
                                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    StoreInfo info;
    info.store_id = request->store_id();
    info.node_id = request->node_id();
    info.store_addr = request->node_host() + ":" +
                      std::to_string(request->node_port());
    info.data_file = request->data_file();
    if (request->has_hixl_engine_addr()) {
        info.hixl_engine_addr = request->hixl_engine_addr();
    }

    Status status = meta_manager_->RegisterStore(info);
    if (status.ok()) {
        LOG(INFO) << "[MetaServiceImpl] StoreRegister: store_id=" << info.store_id
                  << ", node_id=" << info.node_id << ", addr=" << info.store_addr
                  << ", hixl_engine_addr=" << info.hixl_engine_addr;
    }
    response->set_status(status.ok() ? 0 : static_cast<int>(status.code()));
    if (!status.ok()) {
        response->set_error_msg(status.msg());
    }
}

// -----------------------------------------------------------------
// ClientHeartbeat
// -----------------------------------------------------------------
void MetaServiceImpl::ClientHeartbeat(
    ::google::protobuf::RpcController*,
    const ClientHeartbeatRequest* /*request*/,
    ClientHeartbeatResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // Heartbeat is a no-op for now; just acknowledge.
    response->set_status(0);
}

// -----------------------------------------------------------------
// SyncCommit
// -----------------------------------------------------------------
void MetaServiceImpl::SyncCommit(::google::protobuf::RpcController*,
                                  const SyncCommitRequest* request,
                                  SyncCommitResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    uint32_t store_id = request->store_id();

    std::vector<KeyRecord> records;
    records.reserve(request->key_records_size());

    for (int i = 0; i < request->key_records_size(); ++i) {
        const auto& desc = request->key_records(i);
        KeyRecord rec;
        rec.key = desc.key();
        rec.store_id = store_id;
        rec.offset = desc.offset();
        rec.size = desc.size();
        records.push_back(std::move(rec));
    }

    Status status = meta_manager_->SyncCommit(store_id, records);
    response->set_status(status.ok() ? 0 : static_cast<int>(status.code()));
}

// -----------------------------------------------------------------
// SyncRemove
// -----------------------------------------------------------------
void MetaServiceImpl::SyncRemove(::google::protobuf::RpcController*,
                                  const SyncRemoveRequest* request,
                                  SyncRemoveResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    uint32_t store_id = request->store_id();

    std::vector<std::string> keys;
    keys.reserve(request->keys_size());
    for (int i = 0; i < request->keys_size(); ++i) {
        keys.push_back(request->keys(i));
    }

    Status status = meta_manager_->SyncRemove(store_id, keys);
    response->set_status(status.ok() ? 0 : static_cast<int>(status.code()));
}

} // namespace falconkv
