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
    
    void updateEpoch(const Guid& requestEpoch);
    bool checkRingId(uint32_t requestRingId);

    Guid currentEpoch_;
    AcceptorState::ptr acceptorState_;
};

}  // namespace lightning
