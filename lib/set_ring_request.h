#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
#include "ring_configuration.h"
#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <set>
#include <string>
#include <vector>

namespace lightning {

//! Attempts to establish a new ring.
class SetRingRequest : public MulticastRpcRequest {
public:
    SetRingRequest(const Guid& hostGroupGuid,
                   RingConfiguration::const_ptr ring);

private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr source,
                 const RpcMessageData& reply);

    void onTimeout();

    void wait();

    Status status() const;

    RpcMessageData rpcMessageData_;
    RingConfiguration::const_ptr ring_;
    uint64_t notAckedMask_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
