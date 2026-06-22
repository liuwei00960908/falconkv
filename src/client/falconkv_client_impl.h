#pragma once
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "src/common/status.h"
#include "src/common/types.h"
#include "src/common/config.h"
#include "src/client/key_desc_cache.h"
#include "src/store/falconkv_store.h"
#include "src/store/store_meta_index.h"
#include "src/store/node_local_accessor.h"
#include "src/store/store_rpc_client_manager.h"
#include "src/meta/meta_rpc_client.h"
#include "src/scheduler/scheduler_proxy.h"
#include "src/common/time_util.h"

namespace falconkv {

struct BufferInfo {
    void* data_ptr;
    uint32_t size;
};

class FalconKVClientImpl {
public:
    struct Config {
        std::string store_rpc_addr;
        std::string config_file;
        size_t cache_capacity = 100000;
        int async_batch_size = 16;
        bool fire_and_forget = true;
        bool scheduler_enabled = true;
        std::string scheduler_uds_path = "/tmp/falconkv_scheduler.sock";
        int scheduler_rpc_timeout_us = 2000;
        std::string hixl_engine_addr;
        int hixl_device_id = -1;
    };

    explicit FalconKVClientImpl(const Config& config);
    ~FalconKVClientImpl();

    // Batch operations
    int BatchExist(const std::vector<std::string>& keys,
                   std::vector<KeyDescriptor>& hit_descs);
    std::vector<Status> BatchPut(const std::vector<std::string>& keys,
                                  const std::vector<BufferInfo>& buffers);
    std::vector<int32_t> BatchGet(const std::vector<KeyDescriptor>& key_descs,
                                   const std::vector<BufferInfo>& buffers);

    // Sync wrappers
    int BatchExistSync(const std::vector<std::string>& keys) {
        std::vector<KeyDescriptor> descs;
        return BatchExist(keys, descs);
    }

    std::vector<Status> BatchPutSync(const std::vector<std::string>& keys,
                                       const std::vector<BufferInfo>& buffers) {
        return BatchPut(keys, buffers);
    }

    std::vector<int32_t> BatchGetSync(const std::vector<std::string>& keys,
                                        const std::vector<BufferInfo>& buffers);

    void Close();

    /// Set the local FalconKVStore pointer (for ACCESS_LOCAL_DIRECT).
    /// The client does NOT own this pointer.
    void SetLocalStore(FalconKVStore* store) { local_store_ = store; }

private:
    /// Get or create a StoreRpcClient for the given store_id.
    StoreRpcClient* GetStoreRpcClient(uint32_t store_id);

    /// Map AccessType to IOChannel for scheduler.
    int ChannelForAccessType(AccessType type);

    /// Get remote node address from KeyDescriptor's store_addr field.
    std::string RemoteAddrForDesc(const KeyDescriptor& desc);

    Config config_;
    KeyDescCache key_desc_cache_;
    MetaRpcClient meta_client_;

    // Store module integration
    FalconKVStore* local_store_ = nullptr;       // not owned
    NodeLocalAccessor node_accessor_;
    StoreRpcClientManager store_rpc_mgr_;

    // Scheduler integration
    std::unique_ptr<SchedulerProxy> scheduler_proxy_;
    bool scheduler_enabled_ = false;

    // store_id -> store_addr mapping (learned from Meta responses)
    std::unordered_map<uint32_t, std::string> store_addr_map_;

    // This client's node identity for access type determination.
    uint32_t node_id_ = 0;

    // This client's own store address (store_rpc_host:listen_port),
    // sent to remote stores so they can report rx_read stats to the scheduler.
    std::string self_store_addr_;
};

} // namespace falconkv
