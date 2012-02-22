#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include "proto/rpc_messages.pb.h"
#include <boost/shared_ptr.hpp>
#include <iostream>

namespace lightning {

class RingVoter;

class Vote {
public:
    //! A new vote.
    Vote(const Guid& rpcGuid,
         const Guid& epoch,
         uint32_t ringId,
         paxos::InstanceId instance,
         paxos::BallotId ballot,
         const Guid& valueId,
         boost::shared_ptr<RingVoter> ringVoter);

    //! A pending vote received from someone.
    Vote(boost::shared_ptr<RpcMessageData> pendingVote,
         boost::shared_ptr<RingVoter> ringVoter);

    //! Submits the vote
    void send();

    paxos::InstanceId instance() const;

    paxos::BallotId ballot() const;

    const Guid epoch() const;

    const Guid valueId() const;

    //! For debug logging.
    std::ostream& output(std::ostream& os) const;
private:
    friend class RingVoter;

    boost::shared_ptr<RpcMessageData> message_;
    boost::shared_ptr<RingVoter> ringVoter_;
};

inline
std::ostream& operator<<(std::ostream& os, const Vote& vote) {
    vote.output(os);
    return os;
}

}  // namespace lightning
