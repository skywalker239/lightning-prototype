#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
#include "paxos_defs.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <map>
#include <set>

namespace lightning {

class Phase1Request : public MulticastRpcRequest {
public:
    typedef boost::shared_ptr<Phase1Request> ptr;

    Phase1Request(const Guid& epoch,
                  uint32_t ringId,
                  paxos::BallotId ballot,
                  paxos::InstanceId instance,
                  const std::vector<Mordor::Address::ptr>&
                        requestRing);

    enum Result {
        PENDING,
        BALLOT_TOO_LOW,
        SUCCESS
    };

    Result result() const;

    Status status() const;

    paxos::BallotId lastPromisedBallot() const;

    paxos::BallotId lastVotedBallot() const;

    paxos::Value::ptr lastVotedValue() const;

private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr source,
                 const RpcMessageData& reply);

    void onTimeout();

    void wait();

    paxos::Value::ptr parseValue(const ValueData& valueData) const;

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

    paxos::BallotId lastPromisedBallotId_;
    paxos::BallotId lastVotedBallotId_;
    paxos::Value::ptr lastVotedValue_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

} // namespace lightning
