#pragma once

#include "rpc_handler.h"
#include "acceptor_state.h"
#include "ring_holder.h"

namespace lightning {

class Phase1Handler : public RingHolder, public RpcHandler {
public:
    Phase1Handler(AcceptorState::ptr acceptorState);

private:
    typedef paxos::BallotId   BallotId;
    typedef paxos::InstanceId InstanceId;

    bool handleRequest(Mordor::Address::ptr sourceAddress,
                       const RpcMessageData& request,
                       RpcMessageData* reply);
    
    bool checkRingId(RingConfiguration::const_ptr ring,
                     uint32_t requestRingId);

    AcceptorState::ptr acceptorState_;
};

}  // namespace lightning
