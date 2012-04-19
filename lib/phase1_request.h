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
        FORGOTTEN,
        BALLOT_TOO_LOW,
        SUCCESS
    };

    Result result() const;

    paxos::BallotId lastPromisedBallot() const;

    paxos::BallotId lastVotedBallot() const;

    paxos::Value lastVotedValue() const;

private:
    std::ostream& output(std::ostream& os) const;

    void applyReply(uint32_t hostId,
                    const RpcMessageData& reply);

    const GroupConfiguration::ptr& group_;
    Result result_;

    paxos::BallotId lastPromisedBallotId_;
    paxos::BallotId lastVotedBallotId_;
    paxos::Value lastVotedValue_;
};

} // namespace lightning
