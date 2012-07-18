#pragma once

#include "group_configuration.h"
#include "rpc_handler.h"
#include "ring_voter.h"
#include "acceptor_state.h"
#include "ring_holder.h"

namespace lightning {

class Phase2Handler : public RingHolder, public RpcHandler {
public:
    Phase2Handler(AcceptorState::ptr acceptorState,
                  RingVoter::ptr ringVoter);

private:
    typedef paxos::BallotId   BallotId;
    typedef paxos::InstanceId InstanceId;

    //! Always returns false since only the RingVoter on the
    //  last acceptor in the ring has to reply to the master.
    bool handleRequest(Mordor::Address::ptr sourceAddress,
                       const RpcMessageData& request,
                       RpcMessageData* reply);

    bool canInitiateVote(RingConfiguration::const_ptr) const;

    AcceptorState::ptr acceptorState_;
    RingVoter::ptr     ringVoter_;
};

}  // namespace lightning
