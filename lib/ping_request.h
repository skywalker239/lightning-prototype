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
                PingTracker::ptr pingTracker,
                uint64_t timeoutUs);

private:
    const RpcMessageData& request() const;

    std::ostream& output(std::ostream& os) const;

    void applyReply(uint32_t hostId,
                    const RpcMessageData& reply);

    RpcMessageData rpcMessageData_;
    const GroupConfiguration::ptr& group_;
    PingTracker::ptr pingTracker_;
};

}  // namespace lightning
