#include "falconkv_bridge.h"

#include <brpc/protocol.h>
#include <gflags/gflags.h>

#include "src/client/falconkv_client_impl.h"
#include "src/store/falconkv_store.h"
#include "src/common/config.h"
#include "src/common/logging.h"

namespace falconkv {

FalconKVBridge::FalconKVBridge(const Config& config) {
    // 加载全局配置
    FalconKVConfig cfg;
    if (!config.config_file.empty()) {
        cfg = ConfigLoader::LoadFromFile(config.config_file);
    }

    // 初始化共享日志（InitSharedLogging 内部保证 google::InitGoogleLogging 只执行一次）
    falconkv::InitSharedLogging(cfg.common.log_dir, "falconkv_client");

    // Increase brpc max body size for large batch RPCs (default 64MB is too
    // small for BatchRead with many 1MB segments).
    uint64_t max_body = cfg.transfer.max_body_size_mb * 1024ULL * 1024ULL;
    gflags::SetCommandLineOption("max_body_size",
                                  std::to_string(max_body).c_str());

    // 当 worker_id >= 0 时，自动计算 store_id 和 listen_port
    // store_id = node_id * 10 + worker_id  (全局唯一)
    // listen_port = base_port + worker_id   (同节点端口唯一)
    if (config.worker_id >= 0) {
        uint32_t old_store_id = cfg.store.store_id;
        uint32_t old_port = cfg.store.listen_port;
        cfg.store.store_id = cfg.store.node_id * 10 + config.worker_id;
        cfg.store.listen_port = cfg.store.listen_port + config.worker_id;

        std::string store_hixl_host = cfg.store.hixl_engine_host.empty()
            ? cfg.store.store_rpc_host
            : cfg.store.hixl_engine_host;
        std::string client_hixl_host = cfg.client.hixl_engine_host.empty()
            ? cfg.store.store_rpc_host
            : cfg.client.hixl_engine_host;
        if (cfg.store.hixl_engine_addr.empty()) {
            cfg.store.hixl_engine_addr = store_hixl_host + ":" +
                std::to_string(cfg.store.hixl_base_port + config.worker_id);
        }
        if (cfg.client.hixl_engine_addr.empty()) {
            cfg.client.hixl_engine_addr = client_hixl_host + ":" +
                std::to_string(cfg.client.hixl_base_port + config.worker_id);
        }
        if (cfg.store.hixl_device_id < 0) {
            cfg.store.hixl_device_id = config.worker_id;
        }
        if (cfg.client.hixl_device_id < 0) {
            cfg.client.hixl_device_id = config.worker_id;
        }

        LOG(INFO) << "[FalconKVBridge] LMCache worker_id=" << config.worker_id
                  << ", node_id=" << cfg.store.node_id
                  << ", computed store_id=" << cfg.store.store_id
                  << ", listen_port=" << cfg.store.listen_port
                  << ", store_hixl_engine=" << cfg.store.hixl_engine_addr
                  << ", client_hixl_engine=" << cfg.client.hixl_engine_addr
                  << " (overrides store_id=" << old_store_id
                  << ", port=" << old_port << ")";
    }

    // 创建并初始化 FalconKVStore
    store_ = std::make_unique<FalconKVStore>(
        FalconKVStore::Config::FromStoreConfig(cfg.store));
    store_->Init(cfg.transfer.meta_addr);

    // Start Store RPC server for cross-node ACCESS_REMOTE_RPC reads
    if (cfg.store.listen_port > 0) {
        store_service_ = std::make_unique<StoreServiceImpl>(store_.get());
        store_rpc_server_ = std::make_unique<brpc::Server>();
        store_rpc_server_->AddService(
            store_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
        std::string listen_addr = cfg.store.store_rpc_host + ":" +
                                  std::to_string(cfg.store.listen_port);
        if (store_rpc_server_->Start(listen_addr.c_str(), nullptr) != 0) {
            LOG(ERROR) << "[FalconKVBridge] Failed to start Store RPC server at "
                       << listen_addr;
        } else {
            LOG(INFO) << "[FalconKVBridge] Store RPC server listening on "
                      << listen_addr;
        }
    }

    // 创建 FalconKVClientImpl
    FalconKVClientImpl::Config impl_config;
    impl_config.config_file = config.config_file;
    impl_config.cache_capacity = config.cache_capacity;
    impl_config.store_rpc_addr = store_->store_rpc_addr();
    impl_config.hixl_engine_addr = cfg.client.hixl_engine_addr;
    impl_config.hixl_device_id = cfg.client.hixl_device_id;
    impl_ = std::make_unique<FalconKVClientImpl>(impl_config);

    // 将 Store 绑定到 Client
    impl_->SetLocalStore(store_.get());

    // 启动 Fire-and-Forget 工作线程
    int num_workers = 2;
    for (int i = 0; i < num_workers; ++i) {
        workers_.emplace_back([this] { WorkerLoop(); });
    }
}

FalconKVBridge::~FalconKVBridge() {
    Close();
    // 不主动 ShutdownSharedLogging，避免其他实例的日志失效
}

int FalconKVBridge::BatchExistSync(const std::vector<std::string>& keys) {
    return impl_->BatchExistSync(keys);
}

int FalconKVBridge::BatchPutSync(const std::vector<std::string>& keys,
                                  const std::vector<BridgeBuffer>& buffers) {
    // BridgeBuffer → BufferInfo 适配
    // BridgeBuffer 和 BufferInfo 布局一致，直接转换
    std::vector<BufferInfo> buf_infos;
    buf_infos.reserve(buffers.size());
    for (const auto& b : buffers) {
        buf_infos.push_back({b.data_ptr, b.size});
    }

    auto statuses = impl_->BatchPutSync(keys, buf_infos);
    for (const auto& s : statuses) {
        if (!s.ok()) return -1;
    }
    return 0;
}

std::vector<int32_t> FalconKVBridge::BatchGetSync(
    const std::vector<std::string>& keys,
    const std::vector<BridgeBuffer>& buffers) {
    std::vector<BufferInfo> buf_infos;
    buf_infos.reserve(buffers.size());
    for (const auto& b : buffers) {
        buf_infos.push_back({b.data_ptr, b.size});
    }
    return impl_->BatchGetSync(keys, buf_infos);
}

void FalconKVBridge::FireAndForgetPut(
    const std::vector<std::string>& keys,
    const std::vector<BridgeBuffer>& buffers,
    void (*callback)(void*),
    void* user_data) {
    // 拷贝 keys（异步任务可能在调用者释放原始数据后执行）
    auto keys_copy = std::make_shared<std::vector<std::string>>(keys);

    // 拷贝 buffer 元信息（data_ptr 指向的内存由 Python INCREF 保护）
    auto bufs_copy = std::make_shared<std::vector<BufferInfo>>();
    bufs_copy->reserve(buffers.size());
    for (const auto& b : buffers) {
        bufs_copy->push_back({b.data_ptr, b.size});
    }

    // 提交到内部线程池
    SubmitTask([this, keys_copy, bufs_copy, callback, user_data]() {
        // 在工作线程中执行同步写入
        auto statuses = impl_->BatchPutSync(*keys_copy, *bufs_copy);

        // 写入完成（无论成功失败），回调通知 C Extension 层
        // C Extension 层会获取 GIL 并执行 ref_count_down
        callback(user_data);
    });
}

void FalconKVBridge::Close() {
    // 先排空异步队列
    DrainQueue();

    // 停止工作线程
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stopped_.load()) return;
        stopped_.store(true);
    }
    queue_cv_.notify_all();

    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
    workers_.clear();

    // 关闭底层 Client（先解除对 Store 的引用）
    if (impl_) {
        impl_->Close();
        impl_->SetLocalStore(nullptr);
        impl_.reset();
    }

    // 停止 Store RPC server
    if (store_rpc_server_) {
        store_rpc_server_->Stop(0);
        store_rpc_server_->Join();
        store_rpc_server_.reset();
        store_service_.reset();
    }

    // 关闭 Store
    if (store_) {
        store_->Close();
        store_.reset();
    }
}

void FalconKVBridge::WorkerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return stopped_.load() || !task_queue_.empty();
            });
            if (stopped_.load() && task_queue_.empty()) return;
            task = std::move(task_queue_.front());
            task_queue_.pop_front();
        }
        task();
    }
}

void FalconKVBridge::SubmitTask(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push_back(std::move(task));
    }
    queue_cv_.notify_one();
}

void FalconKVBridge::DrainQueue() {
    // 等待队列清空
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (task_queue_.empty()) break;
        lock.unlock();
        // 短暂让步，等待工作线程处理
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

}  // namespace falconkv
