#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
#include "paxos_defs.h"
#include "ring_configuration.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <map>
#include <set>

namespace lightning {

class Phase1Request : public MulticastRpcRequest {
public:
    typedef boost::shared_ptr<Phase1Request> ptr;

    Phase1Request(const Guid& epoch,
                  paxos::BallotId ballot,
                  paxos::InstanceId instance,
                  RingConfiguration::const_ptr ring,
                  uint64_t timeoutUs);

    enum Result {
        PENDING,
        BALLOT_TOO_LOW,
        SUCCESS
    };

    Result result() const;

    paxos::BallotId lastPromisedBallot() const;

    paxos::BallotId lastVotedBallot() const;

    paxos::Value::ptr lastVotedValue() const;

private:
    const RpcMessageData& request() const;

    void applyReply(uint32_t hostId,
                    const RpcMessageData& reply);

    paxos::Value::ptr parseValue(const ValueData& valueData) const;

    RpcMessageData requestData_;
    
    const GroupConfiguration::ptr& group_;
    Result result_;

    paxos::BallotId lastPromisedBallotId_;
    paxos::BallotId lastVotedBallotId_;
    paxos::Value::ptr lastVotedValue_;
};

} // namespace lightning
