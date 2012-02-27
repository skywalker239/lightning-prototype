#include "ring_voter.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <algorithm>

namespace lightning {

const size_t RingVoter::kMaxDatagramSize;

using Mordor::Address;
using Mordor::Socket;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Statistics;
using Mordor::CountStatistic;
using paxos::BallotId;
using paxos::kInvalidBallotId;
using paxos::InstanceId;
using std::vector;
using std::ostream;

static Logger::ptr g_log = Log::lookup("lightning:ring_voter");

static CountStatistic<uint64_t>& g_inPackets =
    Statistics::registerStatistic("ring_voter.in_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_inBytes =
    Statistics::registerStatistic("ring_voter.in_bytes",
                                  CountStatistic<uint64_t>("bytes"));

RingVoter::RingVoter(Socket::ptr socket,
                     UdpSender::ptr udpSender,
                     AcceptorState::ptr acceptorState)
    : socket_(socket),
      udpSender_(udpSender),
      acceptorState_(acceptorState)
{}

void RingVoter::run() {
    Address::ptr remoteAddress = socket_->emptyAddress();
    MORDOR_LOG_TRACE(g_log) << this << " listening at " <<
                               *(socket_->localAddress()); 
    while(true) {
        char buffer[kMaxDatagramSize];
        ssize_t bytes = socket_->receiveFrom((void*)buffer,
                                             sizeof(buffer),
                                             *remoteAddress);
        g_inBytes.add(bytes);
        g_inPackets.increment();
        MORDOR_LOG_TRACE(g_log) << this << " got " << bytes << " bytes from " <<
                                   *remoteAddress;

        boost::shared_ptr<RpcMessageData> requestData(new RpcMessageData);
        if(!requestData->ParseFromArray(buffer, bytes)) {
            MORDOR_LOG_WARNING(g_log) << this << " malformed " << bytes <<
                                         " bytes from " << *remoteAddress;
            continue;
        }
        const Guid requestGuid = Guid::parse(requestData->uuid());
        
        RingConfiguration::const_ptr ringConfiguration =
            tryAcquireRingConfiguration();

        if(!ringConfiguration.get()) {
            MORDOR_LOG_WARNING(g_log) << this << " no ring configuration," <<
                                         " ignoring vote " << requestGuid;
            continue;
        }
        if(!ringConfiguration->isInRing()) {
            MORDOR_LOG_TRACE(g_log) << this <<
                                       " acceptor not in ring, ignoring vote "
                                       << requestGuid;
            continue;
        }
        MORDOR_LOG_TRACE(g_log) << this << " processing vote " << requestGuid;

        Vote vote(requestData, shared_from_this());

        if(processVote(ringConfiguration, vote)) {
            send(vote);
        }
    }
}

void RingVoter::send(const Vote& vote) {
    RingConfiguration::const_ptr ring = tryAcquireRingConfiguration();
    if(!ring.get()) {
        MORDOR_LOG_TRACE(g_log) << this << " no ring, dropping vote " <<
                                   vote;
        return;
    }
    // XXX we don't verify the ring id in the vote message here.
    Address::ptr destination = ring->nextRingAddress();
    MORDOR_LOG_TRACE(g_log) << this << " sending " << vote << " to " <<
                               *destination;
    udpSender_->send(destination, vote.message_);
}

bool RingVoter::processVote(RingConfiguration::const_ptr ringConfiguration,
                            const Vote& vote)
{
    const VoteData& voteData = vote.message_->vote();
    if(voteData.ring_id() != ringConfiguration->ringId()) {
        MORDOR_LOG_TRACE(g_log) << this << " vote ring id=" <<
                                   voteData.ring_id() << ", current=" <<
                                   ringConfiguration->ringId() <<
                                   ", discarding";
        return false;
    }

    const Guid epoch = Guid::parse(voteData.epoch());
    acceptorState_->updateEpoch(epoch);

    BallotId highestPromised;
    AcceptorState::Status status = acceptorState_->vote(vote,
                                                        &highestPromised);
    if(status == AcceptorState::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " " << vote << " ok";
        return true;
    } else {
        MORDOR_LOG_DEBUG(g_log) << this << " " << vote << " = " <<
                                   uint32_t(status) << ", promised=" <<
                                   highestPromised;
        return false;
    }
}

}  // namespace lightning
