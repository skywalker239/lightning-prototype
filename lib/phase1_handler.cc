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
    updateEpoch(requestEpoch);
    if(!checkRingId(requestRingId)) {
        MORDOR_LOG_TRACE(g_log) << this << " bad ring id, ignoring request";
        return false;
    }

    reply->set_type(RpcMessageData::PAXOS_PHASE1);
    PaxosPhase1ReplyData* replyData = reply->mutable_phase1_reply();

    BallotId highestPromised;
    BallotId highestVoted;
    Value    lastVote;
    AcceptorState::Status status = acceptorState_->nextBallot(instance,
                                                              ballot,
                                                              &highestPromised,
                                                              &highestVoted,
                                                              &lastVote);
    if(status == AcceptorState::REFUSED) {
        MORDOR_LOG_TRACE(g_log) << this << " phase1 request (" << instance <<
                                   ", " << ballot << ") refused";
        return false;
    } else if(status == AcceptorState::NACKED) {
        MORDOR_LOG_TRACE(g_log) << this << " phase1 request (" << instance <<
                                   ", " << ballot << ") has too low ballot, " <<
                                   " promised ballot=" << highestPromised;
        replyData->set_type(PaxosPhase1ReplyData::BALLOT_TOO_LOW);
        replyData->set_last_ballot_id(highestPromised);
        return true;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " phase1 request (" << instance <<
                                   ", " << ballot << ") successful, " <<
                                   "highestVoted=" << highestVoted << ", " <<
                                   "lastVote=" << lastVote.valueId;
        if(highestVoted != kInvalidBallotId) {
            replyData->set_last_ballot_id(highestVoted);
            lastVote.valueId.serialize(
                replyData->mutable_value()->mutable_id());
            replyData->mutable_value()->set_data(lastVote.data, lastVote.size);
        }
        return true;
    }
}

bool Phase1Handler::checkRingId(uint32_t ringId) {
    RingConfiguration::const_ptr ringConfiguration =
        tryAcquireRingConfiguration();
    if(!ringConfiguration.get()) {
        MORDOR_LOG_TRACE(g_log) << this << " no valid ring configuration " <<
                                   "to check against " << ringId;
        return false;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " checkRingId(" << ringId <<
                                   ", " << ringConfiguration->ringId() <<
                                   ")";
        return ringId == ringConfiguration->ringId();
    }
}

}  // namespace lightning
