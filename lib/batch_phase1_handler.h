#pragma once

#include "rpc_handler.h"
#include "acceptor_state.h"
#include "ring_holder.h"

namespace lightning {

class BatchPhase1Handler : public RingHolder, public RpcHandler {
public:
    BatchPhase1Handler(AcceptorState::ptr acceptorState);

private:
    typedef paxos::BallotId   BallotId;
    typedef paxos::InstanceId InstanceId;

    bool handleRequest(Mordor::Address::ptr sourceAddress,
                       const RpcMessageData& request,
                       RpcMessageData* reply);
    
    bool checkRingId(uint32_t requestRingId);
    void markReservedInstances(BallotId ballot,
                               InstanceId startInstance,
                               InstanceId endInstance,
                               PaxosPhase1BatchReplyData* reply);

    Guid currentEpoch_;
    AcceptorState::ptr acceptorState_;
};

}  // namespace lightning
