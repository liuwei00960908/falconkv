#include "common/config.h"

#include <fstream>
#include <sstream>
#include <cstdlib>

#include <json/json.h>

#include "src/common/logging.h"

namespace falconkv {

namespace {

std::string GetEnvOrDefault(const char* key, const std::string& default_val) {
    const char* val = std::getenv(key);
    return val ? std::string(val) : default_val;
}

int GetEnvOrDefaultInt(const char* key, int default_val) {
    const char* val = std::getenv(key);
    return val ? std::atoi(val) : default_val;
}

uint32_t GetEnvOrDefaultUInt(const char* key, uint32_t default_val) {
    const char* val = std::getenv(key);
    return val ? static_cast<uint32_t>(std::strtoul(val, nullptr, 10)) : default_val;
}

double GetEnvOrDefaultDouble(const char* key, double default_val) {
    const char* val = std::getenv(key);
    return val ? std::atof(val) : default_val;
}

uint64_t GetEnvOrDefaultUInt64(const char* key, uint64_t default_val) {
    const char* val = std::getenv(key);
    return val ? std::strtoull(val, nullptr, 10) : default_val;
}

void ApplyEnvOverrides(FalconKVConfig& config) {
    // Common config overrides (shared across modules)
    config.common.meta_addr = GetEnvOrDefault("FALCONKV_META_ADDR", config.common.meta_addr);
    config.common.node_id = GetEnvOrDefaultUInt("FALCONKV_NODE_ID", config.common.node_id);
    config.common.scheduler_enabled = GetEnvOrDefault("FALCONKV_SCHEDULER_ENABLED", config.common.scheduler_enabled ? "1" : "0") == "1";
    config.common.scheduler_uds_path = GetEnvOrDefault("FALCONKV_SCHEDULER_UDS_PATH", config.common.scheduler_uds_path);
    config.common.scheduler_rpc_timeout_us = GetEnvOrDefaultInt("FALCONKV_SCHEDULER_RPC_TIMEOUT_US", config.common.scheduler_rpc_timeout_us);
    config.common.max_consecutive_failures = GetEnvOrDefaultInt("FALCONKV_MAX_CONSECUTIVE_FAILURES", config.common.max_consecutive_failures);
    config.common.reconnect_interval_sec = GetEnvOrDefaultInt("FALCONKV_RECONNECT_INTERVAL_SEC", config.common.reconnect_interval_sec);
    config.common.log_dir = GetEnvOrDefault("FALCONKV_LOG_DIR", config.common.log_dir);

    // Propagate common values to modules (module-level env overrides take precedence below)
    config.store.meta_addr = config.common.meta_addr;
    config.store.node_id = config.common.node_id;
    config.store.scheduler_enabled = config.common.scheduler_enabled;
    config.store.scheduler_uds_path = config.common.scheduler_uds_path;
    config.store.scheduler_rpc_timeout_us = config.common.scheduler_rpc_timeout_us;
    config.store.max_consecutive_failures = config.common.max_consecutive_failures;
    config.store.reconnect_interval_sec = config.common.reconnect_interval_sec;
    config.transfer.meta_addr = config.common.meta_addr;
    config.client.node_id = config.common.node_id;
    config.client.scheduler_enabled = config.common.scheduler_enabled;
    config.client.scheduler_uds_path = config.common.scheduler_uds_path;
    config.client.scheduler_rpc_timeout_us = config.common.scheduler_rpc_timeout_us;
    config.client.max_consecutive_failures = config.common.max_consecutive_failures;
    config.client.reconnect_interval_sec = config.common.reconnect_interval_sec;
    config.scheduler.uds_path = config.common.scheduler_uds_path;
    config.scheduler.rpc_timeout_us = config.common.scheduler_rpc_timeout_us;
    config.scheduler.max_consecutive_failures = config.common.max_consecutive_failures;
    config.scheduler.reconnect_interval_sec = config.common.reconnect_interval_sec;

    // Meta config overrides
    config.meta.shard_count = GetEnvOrDefaultInt("FALCONKV_SHARD_COUNT", config.meta.shard_count);
    config.meta.page_size = GetEnvOrDefaultUInt("FALCONKV_PAGE_SIZE", config.meta.page_size);
    config.meta.heartbeat_timeout_sec = GetEnvOrDefaultInt("FALCONKV_HEARTBEAT_TIMEOUT_SEC", config.meta.heartbeat_timeout_sec);

    // Store config overrides
    config.store.ssd_path = GetEnvOrDefault("FALCONKV_SSD_PATH", config.store.ssd_path);
    config.store.store_id = GetEnvOrDefaultUInt("FALCONKV_STORE_ID", config.store.store_id);
    config.store.node_id = GetEnvOrDefaultUInt("FALCONKV_STORE_NODE_ID", config.store.node_id);
    config.store.page_size = GetEnvOrDefaultUInt("FALCONKV_STORE_PAGE_SIZE", config.store.page_size);
    config.store.io_threads = GetEnvOrDefaultUInt("FALCONKV_IO_THREADS", config.store.io_threads);
    config.store.alignment = GetEnvOrDefaultUInt("FALCONKV_ALIGNMENT", config.store.alignment);
    config.store.listen_port = GetEnvOrDefaultUInt("FALCONKV_STORE_LISTEN_PORT", config.store.listen_port);
    config.store.heartbeat_sec = GetEnvOrDefaultUInt("FALCONKV_STORE_HEARTBEAT_SEC", config.store.heartbeat_sec);
    config.store.evict_grace_period_ms = GetEnvOrDefaultUInt("FALCONKV_EVICT_GRACE_PERIOD_MS", config.store.evict_grace_period_ms);
    config.store.evict_check_interval_sec = GetEnvOrDefaultUInt("FALCONKV_EVICT_CHECK_INTERVAL_SEC", config.store.evict_check_interval_sec);
    config.store.evict_high_watermark = GetEnvOrDefaultDouble("FALCONKV_EVICT_HIGH_WATERMARK", config.store.evict_high_watermark);
    config.store.evict_low_watermark = GetEnvOrDefaultDouble("FALCONKV_EVICT_LOW_WATERMARK", config.store.evict_low_watermark);
    config.store.evict_cold_threshold_ms = GetEnvOrDefaultUInt("FALCONKV_EVICT_COLD_THRESHOLD_MS", static_cast<uint32_t>(config.store.evict_cold_threshold_ms));
    config.store.io_uring_enabled = GetEnvOrDefault("FALCONKV_IO_URING_ENABLED", config.store.io_uring_enabled ? "1" : "0") == "1";
    config.store.direct_io_enabled = GetEnvOrDefault("FALCONKV_DIRECT_IO_ENABLED", config.store.direct_io_enabled ? "1" : "0") == "1";
    config.store.io_uring_queue_depth = GetEnvOrDefaultUInt("FALCONKV_IO_URING_QUEUE_DEPTH", config.store.io_uring_queue_depth);
    config.store.slot_size_bytes = GetEnvOrDefaultUInt("FALCONKV_SLOT_SIZE_BYTES", config.store.slot_size_bytes);

    // Scheduler config overrides
    config.scheduler.uds_path = GetEnvOrDefault("FALCONKV_SCHED_UDS_PATH", config.scheduler.uds_path);
    config.scheduler.schedule_policy = GetEnvOrDefault("FALCONKV_SCHEDULE_POLICY", config.scheduler.schedule_policy);
    config.scheduler.ssd_bw_limit_mbps = GetEnvOrDefaultDouble("FALCONKV_SSD_BW_LIMIT_MBPS", config.scheduler.ssd_bw_limit_mbps);
    config.scheduler.net_bw_limit_mbps = GetEnvOrDefaultDouble("FALCONKV_NET_BW_LIMIT_MBPS", config.scheduler.net_bw_limit_mbps);
    config.scheduler.stats_report_interval_sec = GetEnvOrDefaultInt("FALCONKV_STATS_REPORT_INTERVAL_SEC", config.scheduler.stats_report_interval_sec);

    // Client config overrides
    config.client.cache_capacity = static_cast<size_t>(GetEnvOrDefaultUInt("FALCONKV_CLIENT_CACHE_CAPACITY", static_cast<uint32_t>(config.client.cache_capacity)));
    config.client.async_batch_size = GetEnvOrDefaultInt("FALCONKV_ASYNC_BATCH_SIZE", config.client.async_batch_size);
    config.client.scheduler_uds_path = GetEnvOrDefault("FALCONKV_CLIENT_SCHEDULER_UDS_PATH", config.client.scheduler_uds_path);
    config.client.node_id = GetEnvOrDefaultUInt("FALCONKV_CLIENT_NODE_ID", config.client.node_id);

    // Transfer config overrides
    config.transfer.protocol = GetEnvOrDefault("FALCONKV_TRANSFER_PROTOCOL", config.transfer.protocol);
    config.transfer.meta_addr = GetEnvOrDefault("FALCONKV_TRANSFER_META_ADDR", config.transfer.meta_addr);
    config.transfer.meta_pool_size = GetEnvOrDefaultInt("FALCONKV_META_POOL_SIZE", config.transfer.meta_pool_size);
    config.transfer.store_pool_size = GetEnvOrDefaultInt("FALCONKV_STORE_POOL_SIZE", config.transfer.store_pool_size);
    config.transfer.rpc_timeout_ms = GetEnvOrDefaultInt("FALCONKV_RPC_TIMEOUT_MS", config.transfer.rpc_timeout_ms);
    config.transfer.connect_timeout_ms = GetEnvOrDefaultInt("FALCONKV_CONNECT_TIMEOUT_MS", config.transfer.connect_timeout_ms);
    config.transfer.max_retry = GetEnvOrDefaultInt("FALCONKV_MAX_RETRY", config.transfer.max_retry);
    config.transfer.max_body_size_mb = GetEnvOrDefaultUInt64("FALCONKV_MAX_BODY_SIZE_MB", config.transfer.max_body_size_mb);
    config.transfer.remote_read_stream_enabled = GetEnvOrDefault("FALCONKV_REMOTE_READ_STREAM_ENABLED", config.transfer.remote_read_stream_enabled ? "1" : "0") == "1";
    config.transfer.remote_read_chunk_size_mb = GetEnvOrDefaultUInt("FALCONKV_REMOTE_READ_CHUNK_SIZE_MB", config.transfer.remote_read_chunk_size_mb);
    config.transfer.remote_read_prefetch_chunks = GetEnvOrDefaultUInt("FALCONKV_REMOTE_READ_PREFETCH_CHUNKS", config.transfer.remote_read_prefetch_chunks);
    config.transfer.remote_read_queue_chunks = GetEnvOrDefaultUInt("FALCONKV_REMOTE_READ_QUEUE_CHUNKS", config.transfer.remote_read_queue_chunks);
}

void ParseCommonConfig(const Json::Value& root, CommonConfig& cfg) {
    if (!root.isMember("common")) return;
    const auto& c = root["common"];
    if (c.isMember("meta_addr"))          cfg.meta_addr = c["meta_addr"].asString();
    if (c.isMember("node_id"))            cfg.node_id = c["node_id"].asUInt();
    if (c.isMember("scheduler_enabled"))  cfg.scheduler_enabled = c["scheduler_enabled"].asBool();
    if (c.isMember("scheduler_uds_path")) cfg.scheduler_uds_path = c["scheduler_uds_path"].asString();
    if (c.isMember("scheduler_rpc_timeout_us")) cfg.scheduler_rpc_timeout_us = c["scheduler_rpc_timeout_us"].asInt();
    if (c.isMember("max_consecutive_failures"))  cfg.max_consecutive_failures = c["max_consecutive_failures"].asInt();
    if (c.isMember("reconnect_interval_sec"))    cfg.reconnect_interval_sec = c["reconnect_interval_sec"].asInt();
    if (c.isMember("log_dir"))            cfg.log_dir = c["log_dir"].asString();
}

void PropagateCommonToModules(FalconKVConfig& config) {
    config.store.meta_addr = config.common.meta_addr;
    config.store.node_id = config.common.node_id;
    config.store.scheduler_enabled = config.common.scheduler_enabled;
    config.store.scheduler_uds_path = config.common.scheduler_uds_path;
    config.store.scheduler_rpc_timeout_us = config.common.scheduler_rpc_timeout_us;
    config.store.max_consecutive_failures = config.common.max_consecutive_failures;
    config.store.reconnect_interval_sec = config.common.reconnect_interval_sec;
    config.transfer.meta_addr = config.common.meta_addr;
    config.client.node_id = config.common.node_id;
    config.client.scheduler_enabled = config.common.scheduler_enabled;
    config.client.scheduler_uds_path = config.common.scheduler_uds_path;
    config.client.scheduler_rpc_timeout_us = config.common.scheduler_rpc_timeout_us;
    config.client.max_consecutive_failures = config.common.max_consecutive_failures;
    config.client.reconnect_interval_sec = config.common.reconnect_interval_sec;
    config.scheduler.uds_path = config.common.scheduler_uds_path;
    config.scheduler.rpc_timeout_us = config.common.scheduler_rpc_timeout_us;
    config.scheduler.max_consecutive_failures = config.common.max_consecutive_failures;
    config.scheduler.reconnect_interval_sec = config.common.reconnect_interval_sec;
}

void ParseMetaConfig(const Json::Value& root, MetaConfig& cfg) {
    if (!root.isMember("meta")) return;
    const auto& m = root["meta"];
    if (m.isMember("shard_count"))        cfg.shard_count = m["shard_count"].asInt();
    if (m.isMember("page_size"))          cfg.page_size = m["page_size"].asUInt();
    if (m.isMember("heartbeat_timeout_sec")) cfg.heartbeat_timeout_sec = m["heartbeat_timeout_sec"].asInt();
}

void ParseStoreConfig(const Json::Value& root, StoreConfig& cfg) {
    if (!root.isMember("store")) return;
    const auto& s = root["store"];
    if (s.isMember("ssd_path"))           cfg.ssd_path = s["ssd_path"].asString();
    if (s.isMember("store_id"))           cfg.store_id = s["store_id"].asUInt();
    if (s.isMember("capacity_gb"))        cfg.capacity_bytes = static_cast<uint64_t>(s["capacity_gb"].asUInt64()) * 1024ULL * 1024ULL * 1024ULL;
    if (s.isMember("page_size"))          cfg.page_size = s["page_size"].asUInt();
    if (s.isMember("io_threads"))         cfg.io_threads = s["io_threads"].asUInt();
    if (s.isMember("alignment"))          cfg.alignment = s["alignment"].asUInt();
    if (s.isMember("listen_port"))        cfg.listen_port = s["listen_port"].asUInt();
    if (s.isMember("heartbeat_sec"))      cfg.heartbeat_sec = s["heartbeat_sec"].asUInt();
    if (s.isMember("store_rpc_host"))     cfg.store_rpc_host = s["store_rpc_host"].asString();
    if (s.isMember("evict_grace_period_ms"))   cfg.evict_grace_period_ms = s["evict_grace_period_ms"].asUInt();
    if (s.isMember("evict_check_interval_sec")) cfg.evict_check_interval_sec = s["evict_check_interval_sec"].asUInt();
    if (s.isMember("evict_high_watermark"))    cfg.evict_high_watermark = s["evict_high_watermark"].asDouble();
    if (s.isMember("evict_low_watermark"))     cfg.evict_low_watermark = s["evict_low_watermark"].asDouble();
    if (s.isMember("evict_cold_threshold_ms")) cfg.evict_cold_threshold_ms = static_cast<uint64_t>(s["evict_cold_threshold_ms"].asUInt64());
    if (s.isMember("io_uring_enabled"))       cfg.io_uring_enabled = s["io_uring_enabled"].asBool();
    if (s.isMember("direct_io_enabled"))      cfg.direct_io_enabled = s["direct_io_enabled"].asBool();
    if (s.isMember("io_uring_queue_depth"))   cfg.io_uring_queue_depth = s["io_uring_queue_depth"].asUInt();
    if (s.isMember("slot_size_bytes"))        cfg.slot_size_bytes = s["slot_size_bytes"].asUInt();
}

void ParseSchedulerConfig(const Json::Value& root, SchedulerConfig& cfg) {
    if (!root.isMember("scheduler")) return;
    const auto& sc = root["scheduler"];
    if (sc.isMember("uds_path"))                  cfg.uds_path = sc["uds_path"].asString();
    if (sc.isMember("schedule_policy"))           cfg.schedule_policy = sc["schedule_policy"].asString();
    if (sc.isMember("ssd_bw_limit_mbps"))         cfg.ssd_bw_limit_mbps = sc["ssd_bw_limit_mbps"].asDouble();
    if (sc.isMember("net_bw_limit_mbps"))         cfg.net_bw_limit_mbps = sc["net_bw_limit_mbps"].asDouble();
    if (sc.isMember("stats_report_interval_sec")) cfg.stats_report_interval_sec = sc["stats_report_interval_sec"].asInt();
}

void ParseClientConfig(const Json::Value& root, ClientConfig& cfg) {
    if (!root.isMember("client")) return;
    const auto& c = root["client"];
    if (c.isMember("cache_capacity"))          cfg.cache_capacity = static_cast<size_t>(c["cache_capacity"].asUInt64());
    if (c.isMember("async_batch_size"))        cfg.async_batch_size = c["async_batch_size"].asInt();
    if (c.isMember("fire_and_forget"))         cfg.fire_and_forget = c["fire_and_forget"].asBool();
}

void ParseTransferConfig(const Json::Value& root, TransferConfig& cfg) {
    if (!root.isMember("transfer")) return;
    const auto& t = root["transfer"];
    if (t.isMember("protocol"))           cfg.protocol = t["protocol"].asString();
    if (t.isMember("meta_pool_size"))     cfg.meta_pool_size = t["meta_pool_size"].asInt();
    if (t.isMember("store_pool_size"))    cfg.store_pool_size = t["store_pool_size"].asInt();
    if (t.isMember("rpc_timeout_ms"))     cfg.rpc_timeout_ms = t["rpc_timeout_ms"].asInt();
    if (t.isMember("connect_timeout_ms")) cfg.connect_timeout_ms = t["connect_timeout_ms"].asInt();
    if (t.isMember("max_retry"))          cfg.max_retry = t["max_retry"].asInt();
    if (t.isMember("max_body_size_mb"))   cfg.max_body_size_mb = t["max_body_size_mb"].asUInt64();
    if (t.isMember("remote_read_stream_enabled")) cfg.remote_read_stream_enabled = t["remote_read_stream_enabled"].asBool();
    if (t.isMember("remote_read_chunk_size_mb"))  cfg.remote_read_chunk_size_mb = t["remote_read_chunk_size_mb"].asUInt();
    if (t.isMember("remote_read_prefetch_chunks")) cfg.remote_read_prefetch_chunks = t["remote_read_prefetch_chunks"].asUInt();
    if (t.isMember("remote_read_queue_chunks"))    cfg.remote_read_queue_chunks = t["remote_read_queue_chunks"].asUInt();
}

bool ParseJsonConfig(const std::string& json_str, FalconKVConfig& config) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    Json::CharReader* reader = builder.newCharReader();
    std::string errors;
    bool ok = reader->parse(json_str.data(), json_str.data() + json_str.size(),
                            &root, &errors);
    delete reader;
    if (!ok) {
        LOG(ERROR) << "[ConfigLoader] Failed to parse JSON config: " << errors;
        return false;
    }

    // 1. Parse common section first
    ParseCommonConfig(root, config.common);

    // 2. Propagate common values to modules as defaults
    PropagateCommonToModules(config);

    // 3. Parse module-specific configs (can override common defaults)
    ParseMetaConfig(root, config.meta);
    ParseStoreConfig(root, config.store);
    ParseSchedulerConfig(root, config.scheduler);
    ParseClientConfig(root, config.client);
    ParseTransferConfig(root, config.transfer);
    return true;
}

} // anonymous namespace

FalconKVConfig ConfigLoader::LoadFromFile(const std::string& path) {
    FalconKVConfig config;

    std::ifstream file(path);
    if (file.is_open()) {
        std::stringstream buf;
        buf << file.rdbuf();
        std::string contents = buf.str();
        file.close();

        if (!ParseJsonConfig(contents, config)) {
            // JSON parse failed; fall through to env overrides with defaults.
        }
    }

    ApplyEnvOverrides(config);
    return config;
}

FalconKVConfig ConfigLoader::LoadFromString(const std::string& json_str) {
    FalconKVConfig config;

    ParseJsonConfig(json_str, config);

    ApplyEnvOverrides(config);
    return config;
}

} // namespace falconkv
