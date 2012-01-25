#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
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
                   const std::vector<Mordor::Address::ptr>& ringAddresses,
                   const std::vector<uint32_t>& ringHostIds,
                   uint32_t ringId);

private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr source,
                 const RpcMessageData& reply);

    void onTimeout();

    void wait();

    Status status() const;

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                        const Mordor::Address::ptr& rhs) const
        {
            return *lhs < *rhs;
        }
    };

    RpcMessageData rpcMessageData_;
    std::set<Mordor::Address::ptr, AddressCompare> notAcked_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
