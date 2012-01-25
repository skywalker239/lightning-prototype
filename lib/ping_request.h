#pragma once

#include "multicast_rpc_request.h"
#include "ping_tracker.h"
#include <mordor/fibersynchronization.h>
#include <set>
#include <vector>

namespace lightning {

//! A single multicast ping.
class PingRequest : public MulticastRpcRequest {
public:
    PingRequest(const std::vector<Mordor::Address::ptr>& hosts,
                uint64_t pingId,
                PingTracker::ptr pingTracker);

private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr sourceAddress,
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
    PingTracker::ptr pingTracker_;
    std::set<Mordor::Address::ptr, AddressCompare> notAcked_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
