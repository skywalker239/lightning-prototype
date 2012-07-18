#pragma once

#include "acceptor_state.h"
#include "guid.h"
#include "group_configuration.h"
#include "paxos_defs.h"
#include "ring_holder.h"
#include "proto/rpc_messages.pb.h"
#include "udp_sender.h"
#include <boost/enable_shared_from_this.hpp>
#include <iostream>

namespace lightning {

class Vote;

class RingVoter : public RingHolder,
                  public boost::enable_shared_from_this<RingVoter>
{
public:
    typedef boost::shared_ptr<RingVoter> ptr;

    RingVoter(Mordor::Socket::ptr socket,
              UdpSender::ptr udpSender,
              AcceptorState::ptr acceptorState);

    void run();

    void send(const Vote& vote);
private:
    Mordor::Socket::ptr socket_;
    UdpSender::ptr udpSender_;
    AcceptorState::ptr acceptorState_;
    
    static const size_t kMaxDatagramSize = 8950;

    bool processVote(RingConfiguration::const_ptr ringConfiguration,
                     const Vote& vote);

    Mordor::Address::ptr voteDestination(
        RingConfiguration::const_ptr ringConfiguration) const;
};

}  // namespace lightning
