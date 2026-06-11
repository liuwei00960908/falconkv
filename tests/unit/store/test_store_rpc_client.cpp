#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

#include "src/store/store_server.h"
#include "src/store/store_rpc_client.h"

namespace falconkv {
namespace {

class StoreRpcClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "falconkv_test_rpc";
        std::filesystem::create_directories(test_dir_);

        FalconKVStore::Config config;
        config.ssd_path = test_dir_.string();
        config.store_id = 1;
        config.capacity_bytes = 16 * 1024 * 1024; // 16MB for testing
        config.page_size = 512;

        listen_addr_ = "127.0.0.1:18901";
        server_ = std::make_unique<StoreServer>(config, listen_addr_);

        Status s = server_->Start();
        ASSERT_TRUE(s.ok()) << s.msg();
    }

    void TearDown() override {
        server_->Stop();
        server_.reset();
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::unique_ptr<StoreServer> server_;
    std::string listen_addr_;
};

TEST_F(StoreRpcClientTest, ConnectAndPing) {
    StoreRpcClient client;
    Status s = client.Connect(listen_addr_);
    ASSERT_TRUE(s.ok()) << s.msg();

    s = client.Ping();
    EXPECT_TRUE(s.ok()) << s.msg();
}

TEST_F(StoreRpcClientTest, ReadOffsetBased) {
    StoreRpcClient client;
    ASSERT_TRUE(client.Connect(listen_addr_).ok());

    // Write data directly to store offset 0 (page-aligned)
    // The store's Write() is used internally; we test Read RPC path
    std::string write_data(512, 'Z');
    Status ws = server_->GetStore()->Write(0, write_data.data(), write_data.size());
    ASSERT_TRUE(ws.ok()) << ws.msg();

    // Read back via RPC
    std::string read_buf(write_data.size(), '\0');
    Status rs = client.Read(0, read_buf.data(), read_buf.size());
    ASSERT_TRUE(rs.ok()) << rs.msg();

    EXPECT_EQ(write_data, read_buf);
}

TEST_F(StoreRpcClientTest, BatchReadForcedSplit) {
    StoreRpcClient client;
    ASSERT_TRUE(client.Connect(listen_addr_, 1024, 2, false).ok());

    std::vector<std::string> write_data = {
        std::string(512, 'A'),
        std::string(512, 'B'),
        std::string(512, 'C'),
    };
    std::vector<uint64_t> offsets = {0, 512, 1024};
    std::vector<uint32_t> sizes = {512, 512, 512};

    for (size_t i = 0; i < write_data.size(); ++i) {
        Status ws = server_->GetStore()->Write(offsets[i], write_data[i].data(),
                                               write_data[i].size());
        ASSERT_TRUE(ws.ok()) << ws.msg();
    }

    std::vector<std::string> read_data = {
        std::string(512, '\0'),
        std::string(512, '\0'),
        std::string(512, '\0'),
    };
    std::vector<void*> buffers;
    buffers.reserve(read_data.size());
    for (auto& data : read_data) {
        buffers.push_back(data.data());
    }

    std::vector<int32_t> results;
    Status rs = client.BatchRead(offsets, sizes, buffers, results);
    ASSERT_TRUE(rs.ok()) << rs.msg();
    ASSERT_EQ(write_data.size(), results.size());

    for (size_t i = 0; i < write_data.size(); ++i) {
        EXPECT_EQ(static_cast<int32_t>(sizes[i]), results[i]);
        EXPECT_EQ(write_data[i], read_data[i]);
    }
}

TEST_F(StoreRpcClientTest, BatchReadStreamChunks) {
    StoreRpcClient client;
    ASSERT_TRUE(client.Connect(listen_addr_, 16 * 1024 * 1024, 2,
                               true, 512).ok());

    std::string first(1024, 'X');
    std::string second(512, 'Y');
    std::vector<std::string> write_data = {first, second};
    std::vector<uint64_t> offsets = {0, 1024};
    std::vector<uint32_t> sizes = {1024, 512};

    for (size_t i = 0; i < write_data.size(); ++i) {
        Status ws = server_->GetStore()->Write(offsets[i], write_data[i].data(),
                                               write_data[i].size());
        ASSERT_TRUE(ws.ok()) << ws.msg();
    }

    std::vector<std::string> read_data = {
        std::string(1024, '\0'),
        std::string(512, '\0'),
    };
    std::vector<void*> buffers;
    for (auto& data : read_data) {
        buffers.push_back(data.data());
    }

    std::vector<int32_t> results;
    Status rs = client.BatchRead(offsets, sizes, buffers, results);
    ASSERT_TRUE(rs.ok()) << rs.msg();
    ASSERT_EQ(write_data.size(), results.size());

    for (size_t i = 0; i < write_data.size(); ++i) {
        EXPECT_EQ(static_cast<int32_t>(sizes[i]), results[i]);
        EXPECT_EQ(write_data[i], read_data[i]);
    }
}

TEST_F(StoreRpcClientTest, ConnectToInvalidAddr) {
    StoreRpcClient client;
    Status s = client.Connect("256.256.256.256:9999");
    EXPECT_FALSE(s.ok());
}

} // namespace
} // namespace falconkv
