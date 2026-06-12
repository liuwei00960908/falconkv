#pragma once
#include <string>
#include <cstdint>

namespace falconkv {

struct CommonConfig {
    std::string meta_addr = "localhost:8900";
    uint32_t node_id = 0;
    bool scheduler_enabled = true;
    std::string scheduler_uds_path = "/tmp/falconkv_scheduler.sock";
    int scheduler_rpc_timeout_us = 2000;
    int max_consecutive_failures = 3;
    int reconnect_interval_sec = 2;
    std::string log_dir = "";  // glog 日志目录，空则输出到 stderr
};

struct MetaConfig {
    int shard_count = 64;
    uint32_t page_size = 4096;
    int heartbeat_timeout_sec = 30;
};

struct StoreConfig {
    std::string ssd_path = "/data/falconkv";
    uint32_t store_id = 0;
    uint32_t node_id = 0;
    uint64_t capacity_bytes = 500ULL * 1024 * 1024 * 1024; // 500GB
    uint32_t page_size = 4096;
    uint32_t io_threads = 4;
    uint32_t alignment = 512;
    uint32_t listen_port = 8901;
    uint32_t heartbeat_sec = 5;
    bool scheduler_enabled = true;
    std::string scheduler_uds_path = "/tmp/falconkv_scheduler.sock";
    int scheduler_rpc_timeout_us = 2000;
    int max_consecutive_failures = 3;
    int reconnect_interval_sec = 2;
    std::string store_rpc_host = "127.0.0.1";
    std::string meta_addr = "localhost:8900";
    uint32_t evict_grace_period_ms = 5000;
    uint32_t evict_check_interval_sec = 60;
    double evict_high_watermark = 0.85;
    double evict_low_watermark = 0.70;
    uint64_t evict_cold_threshold_ms = 300000;
    bool io_uring_enabled = true;
    bool direct_io_enabled = true;
    uint32_t io_uring_queue_depth = 128;
    uint32_t slot_size_bytes = 0;  // 0 = auto-detect from first write, >0 = explicit slot size
};

struct SchedulerConfig {
    std::string uds_path = "/tmp/falconkv_scheduler.sock";
    std::string schedule_policy = "passthrough";
    double ssd_bw_limit_mbps = 7000.0;
    double net_bw_limit_mbps = 12500.0;
    int stats_report_interval_sec = 5;
    int rpc_timeout_us = 2000;
    int max_consecutive_failures = 3;
    int reconnect_interval_sec = 2;
};

struct ClientConfig {
    size_t cache_capacity = 100000;
    int async_batch_size = 16;
    bool fire_and_forget = true;
    bool scheduler_enabled = true;
    std::string scheduler_uds_path = "/tmp/falconkv_scheduler.sock";
    int scheduler_rpc_timeout_us = 2000;
    int max_consecutive_failures = 3;
    int reconnect_interval_sec = 2;
    uint32_t node_id = 0;
};

struct TransferConfig {
    std::string protocol = "brpc";
    std::string meta_addr = "localhost:8900";
    int meta_pool_size = 4;
    int store_pool_size = 4;
    int rpc_timeout_ms = 5000;
    int connect_timeout_ms = 3000;
    int max_retry = 3;
    uint64_t max_body_size_mb = 512;  // BRPC max body size (MB)
    bool remote_read_stream_enabled = true;
    uint32_t remote_read_chunk_size_mb = 16;
    uint32_t remote_read_prefetch_chunks = 4;
    uint32_t remote_read_queue_chunks = 4;
};

struct FalconKVConfig {
    CommonConfig common;
    MetaConfig meta;
    StoreConfig store;
    SchedulerConfig scheduler;
    ClientConfig client;
    TransferConfig transfer;
};

class ConfigLoader {
public:
    static FalconKVConfig LoadFromFile(const std::string& path);
    static FalconKVConfig LoadFromString(const std::string& json_str);
};

} // namespace falconkv
