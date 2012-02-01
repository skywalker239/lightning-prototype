#pragma once

#include "acceptor_state.h"
#include "guid.h"
#include "host_configuration.h"
#include "paxos_defs.h"
#include "ring_holder.h"
#include "proto/rpc_messages.pb.h"

namespace lightning {

class RingVoter : public RingHolder { 
public:
    typedef paxos::BallotId   BallotId;
    typedef paxos::InstanceId InstanceId;
    typedef boost::shared_ptr<RingVoter> ptr;

    RingVoter(const GroupConfiguration& groupConfiguration,
              Mordor::Socket::ptr socket,
              AcceptorState::ptr acceptorState);

    void run();

    //! Start a new vote.
    //  Asserts that thisHostId() == 1 (only the first acceptor in the
    //  ring can initiate a ring vote).
    void initiateVote(const Guid& rpcGuid,
                      const Guid& epoch,
                      uint32_t    ringId,
                      InstanceId  instance,
                      BallotId    ballot,
                      const Guid& valueId);
private:
    const GroupConfiguration groupConfiguration_;
    Mordor::Socket::ptr socket_;
    AcceptorState::ptr acceptorState_;
    
    static const size_t kMaxDatagramSize = 8950;

    bool processVote(RingConfiguration::const_ptr ringConfiguration,
                     const RpcMessageData& request,
                     RpcMessageData* reply);

    Mordor::Address::ptr voteDestination(
        RingConfiguration::const_ptr ringConfiguration) const;
};

}  // namespace lightning
