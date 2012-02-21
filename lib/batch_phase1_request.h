#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
#include "paxos_defs.h"
#include "ring_configuration.h"
#include <mordor/fibersynchronization.h>
#include <map>
#include <set>

namespace lightning {

class BatchPhase1Request : public MulticastRpcRequest {
public:
    typedef boost::shared_ptr<BatchPhase1Request> ptr;
    BatchPhase1Request(const Guid& epoch,
                       paxos::BallotId ballotId,
                       paxos::InstanceId instanceRangeBegin,
                       paxos::InstanceId instanceRangeEnd,
                       RingConfiguration::const_ptr ring,
                       const uint64_t timeoutUs);

    enum Result {
        PENDING,
        IID_TOO_LOW,
        SUCCESS
    };

    Result result() const;

    const std::set<paxos::InstanceId>&
        reservedInstances() const;

    paxos::InstanceId retryStartInstanceId() const;
private:
    virtual std::ostream& output(std::ostream& os) const;

    virtual void applyReply(uint32_t hostId,
                            const RpcMessageData& reply);

    const GroupConfiguration::ptr& group_;

    Result result_;

    std::set<paxos::InstanceId> reservedInstances_;
    paxos::InstanceId retryStartInstanceId_;
};

} // namespace lightning
