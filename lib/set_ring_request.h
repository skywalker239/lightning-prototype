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
                   RingConfiguration::const_ptr ring,
                   uint64_t timeoutUs);

private:
    const RpcMessageData& request() const;

    std::ostream& output(std::ostream& os) const;

    void applyReply(uint32_t hostId,
                    const RpcMessageData& reply);

    RpcMessageData rpcMessageData_;
    RingConfiguration::const_ptr ring_;
};

}  // namespace lightning
