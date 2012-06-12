#include "phase1_handler.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::kInvalidBallotId;
using paxos::Value;

static Logger::ptr g_log = Log::lookup("lightning:phase1_handler");

Phase1Handler::Phase1Handler(AcceptorState::ptr acceptorState)
    : acceptorState_(acceptorState)
{}

bool Phase1Handler::handleRequest(Address::ptr,
                                  const RpcMessageData& request,
                                  RpcMessageData* reply)
{
    const PaxosPhase1RequestData& paxosRequest =
        request.phase1_request();
    Guid requestEpoch = Guid::parse(paxosRequest.epoch());
    const uint32_t requestRingId = paxosRequest.ring_id();
    const InstanceId instance = paxosRequest.instance();
    const BallotId ballot = paxosRequest.ballot();
    
    MORDOR_LOG_TRACE(g_log) << this << " phase 1 request epoch=" <<
                               requestEpoch << " ringId=" << requestRingId <<
                               " iid=" << instance << ", ballot=" << ballot;

    RingConfiguration::const_ptr ring = tryAcquireRingConfiguration();
    if(!checkRingId(ring, requestRingId)) {
        MORDOR_LOG_TRACE(g_log) << this << " bad ring id, ignoring request";
        return false;
    }

    reply->set_type(RpcMessageData::PAXOS_PHASE1);
    PaxosPhase1ReplyData* replyData = reply->mutable_phase1_reply();

    BallotId highestPromised;
    BallotId highestVoted;
    Value    lastVote;
    AcceptorState::Status status = acceptorState_->nextBallot(requestEpoch,
                                                              instance,
                                                              ballot,
                                                              &highestPromised,
                                                              &highestVoted,
                                                              &lastVote);
    switch(status) {
        case AcceptorState::REFUSED:
            MORDOR_LOG_TRACE(g_log) << this << " phase1 request (" << instance <<
                                       ", " << ballot << ") refused";
            return false;
            break;
        case AcceptorState::NACKED:
            MORDOR_LOG_TRACE(g_log) << this << " phase1 request (" << instance <<
                                       ", " << ballot << ") has too low ballot, " <<
                                       " promised ballot=" << highestPromised;
            replyData->set_type(PaxosPhase1ReplyData::BALLOT_TOO_LOW);
            replyData->set_last_ballot_id(highestPromised);
            return ring->isInRing();
            break;
        case AcceptorState::TOO_OLD:
            MORDOR_LOG_TRACE(g_log) << this << " iid=" << instance <<
                                       " forgotten";
            replyData->set_type(PaxosPhase1ReplyData::FORGOTTEN);
            return ring->isInRing();
            break;
        case AcceptorState::OK:
            MORDOR_LOG_TRACE(g_log) << this << " phase1 request (" << instance <<
                                       ", " << ballot << ") successful, " <<
                                       "highestVoted=" << highestVoted << ", " <<
                                       "lastVote=" << lastVote;
            replyData->set_type(PaxosPhase1ReplyData::OK);
            if(highestVoted != kInvalidBallotId) {
                replyData->set_last_ballot_id(highestVoted);
                lastVote.serialize(replyData->mutable_value());
            }
            return ring->isInRing();
            break;
        default:
            MORDOR_ASSERT(1==0);
    }
}

bool Phase1Handler::checkRingId(RingConfiguration::const_ptr ring,
                                uint32_t ringId)
{
    if(!ring.get()) {
        MORDOR_LOG_TRACE(g_log) << this << " no valid ring configuration " <<
                                   "to check against " << ringId;
        return false;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " checkRingId(" << ringId <<
                                   ", " << ring->ringId() <<
                                   ")";
        return ringId == ring->ringId();
    }
}

}  // namespace lightning
