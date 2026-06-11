#pragma once

#include "falconkv_store.pb.h"
#include "src/store/falconkv_store.h"

namespace falconkv {

/// Server-side implementation of FalconKVStoreService.
/// Each RPC method extracts proto request fields, delegates to FalconKVStore,
/// and converts C++ results back to proto response.
class StoreServiceImpl : public FalconKVStoreService {
public:
    explicit StoreServiceImpl(FalconKVStore* store);
    ~StoreServiceImpl() override = default;

    void Read(::google::protobuf::RpcController* controller,
              const ReadRequest* request,
              ReadResponse* response,
              ::google::protobuf::Closure* done) override;

    void BatchRead(::google::protobuf::RpcController* controller,
                   const BatchReadRequest* request,
                   BatchReadResponse* response,
                   ::google::protobuf::Closure* done) override;

    void BatchReadStream(::google::protobuf::RpcController* controller,
                         const BatchReadStreamRequest* request,
                         BatchReadStreamResponse* response,
                         ::google::protobuf::Closure* done) override;

    void GetByKey(::google::protobuf::RpcController* controller,
                  const GetByKeyRequest* request,
                  GetByKeyResponse* response,
                  ::google::protobuf::Closure* done) override;

    void BatchGetByKey(::google::protobuf::RpcController* controller,
                       const BatchGetByKeyRequest* request,
                       BatchGetByKeyResponse* response,
                       ::google::protobuf::Closure* done) override;

    void Ping(::google::protobuf::RpcController* controller,
              const PingRequest* request,
              PongResponse* response,
              ::google::protobuf::Closure* done) override;

private:
    FalconKVStore* store_; // not owned
};

} // namespace falconkv
