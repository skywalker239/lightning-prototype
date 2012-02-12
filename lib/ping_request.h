#pragma once

#include "multicast_rpc_request.h"
#include "ping_tracker.h"
#include "ring_configuration.h"
#include <mordor/fibersynchronization.h>
#include <set>
#include <vector>

namespace lightning {

//! A single multicast ping.
class PingRequest : public MulticastRpcRequest {
public:
    PingRequest(RingConfiguration::const_ptr ring,
                uint64_t pingId,
                PingTracker::ptr pingTracker);

private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr sourceAddress,
                 const RpcMessageData& reply);

    void onTimeout();

    void wait();

    Status status() const;

    RpcMessageData rpcMessageData_;
    RingConfiguration::const_ptr ring_;
    uint64_t notAckedMask_;
    PingTracker::ptr pingTracker_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
