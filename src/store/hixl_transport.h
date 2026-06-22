#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "src/common/status.h"

namespace falconkv {

struct HixlTransportConfig {
    std::string local_engine;
    std::string protocol_desc;
    std::string local_comm_res;
    std::string mem_type = "host";
    int device_id = -1;
    uint32_t connect_timeout_ms = 3000;
};

struct HixlReadOp {
    uintptr_t local_addr = 0;
    uintptr_t remote_addr = 0;
    size_t len = 0;
};

class HixlTransport {
public:
    HixlTransport();
    ~HixlTransport();

    HixlTransport(const HixlTransport&) = delete;
    HixlTransport& operator=(const HixlTransport&) = delete;

    Status Init(const HixlTransportConfig& config);
    Status RegisterMemory(void* addr, size_t len, void** handle);
    Status DeregisterMemory(void* handle);
    Status Connect(const std::string& remote_engine);
    Status Read(const std::string& remote_engine,
                const std::vector<HixlReadOp>& ops,
                uint32_t timeout_ms);
    void Close();
    bool initialized() const { return initialized_; }
    const std::string& local_engine() const { return config_.local_engine; }
    const std::string& mem_type() const { return config_.mem_type; }

private:
    struct Impl;

    HixlTransportConfig config_;
    std::unique_ptr<Impl> impl_;
    bool initialized_ = false;
};

} // namespace falconkv
