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
    
    bool checkRingId(const RingConfiguration::const_ptr& ring,
                     uint32_t requestRingId);
    void markReservedInstances(const Guid& epoch,
                               BallotId ballot,
                               InstanceId startInstance,
                               InstanceId endInstance,
                               PaxosPhase1BatchReplyData* reply);

    AcceptorState::ptr acceptorState_;
};

}  // namespace lightning
