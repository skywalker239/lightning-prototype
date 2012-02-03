#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
#include "paxos_defs.h"
#include <mordor/fibersynchronization.h>
#include <map>
#include <set>

namespace lightning {

class BatchPhase1Request : public MulticastRpcRequest {
public:
    typedef boost::shared_ptr<BatchPhase1Request> ptr;
    BatchPhase1Request(const Guid& epoch,
                       uint32_t ringId,
                       paxos::BallotId ballotId,
                       paxos::InstanceId instanceRangeBegin,
                       paxos::InstanceId instanceRangeEnd,
                       const std::vector<Mordor::Address::ptr>&
                             requestRing);

    enum Result {
        PENDING,
        IID_TOO_LOW,
        SUCCESS
    };

    Result result() const;

    Status status() const;

    const std::set<paxos::InstanceId>&
        reservedInstances() const;

    paxos::InstanceId retryStartInstanceId() const;
private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr source,
                 const RpcMessageData& reply);

    void onTimeout();

    void wait();

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                        const Mordor::Address::ptr& rhs) const
        {
            return *lhs < *rhs;
        }
    }; 

    RpcMessageData requestData_;
    std::set<Mordor::Address::ptr, AddressCompare> notAcked_;
    Status status_;
    Result result_;

    std::set<paxos::InstanceId> reservedInstances_;
    paxos::InstanceId retryStartInstanceId_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

} // namespace lightning
