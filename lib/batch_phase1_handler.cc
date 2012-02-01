#include "batch_phase1_handler.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::Value;

static Logger::ptr g_log = Log::lookup("lightning:batch_phase1_handler");

BatchPhase1Handler::BatchPhase1Handler(AcceptorState::ptr acceptorState)
    : acceptorState_(acceptorState)
{}

bool BatchPhase1Handler::handleRequest(Address::ptr,
                                       const RpcMessageData& request,
                                       RpcMessageData* reply)
{
    const PaxosPhase1BatchRequestData& batchRequest =
        request.phase1_batch_request();
    Guid requestEpoch = Guid::parse(batchRequest.epoch());
    const uint32_t requestRingId = batchRequest.ring_id();
    const BallotId requestBallot = batchRequest.ballot_id();
    const InstanceId startInstanceId = batchRequest.start_instance_id();
    const InstanceId endInstanceId   = batchRequest.end_instance_id();
    
    MORDOR_LOG_TRACE(g_log) << this << " batch phase 1 request epoch=" <<
                               requestEpoch << " ringId=" << requestRingId <<
                               " iids=[" << startInstanceId << ", " <<
                               endInstanceId << ") ballot=" << requestBallot;
    updateEpoch(requestEpoch);
    if(!checkRingId(requestRingId)) {
        MORDOR_LOG_TRACE(g_log) << this << " bad ring id, ignoring request";
        return false;
    }

    reply->set_type(RpcMessageData::PAXOS_BATCH_PHASE1);
    PaxosPhase1BatchReplyData* replyData = reply->mutable_phase1_batch_reply();
    InstanceId lowestOpenInstanceId = acceptorState_->lowestInstanceId();
    if(startInstanceId < lowestOpenInstanceId) {
        MORDOR_LOG_TRACE(g_log) << this << " start iid " << startInstanceId <<
                                   " too low, retry with " <<
                                   lowestOpenInstanceId;
        replyData->set_type(PaxosPhase1BatchReplyData::IID_TOO_LOW);
        replyData->set_retry_iid(lowestOpenInstanceId);
        return true;
    } else {
        markReservedInstances(requestBallot,
                              startInstanceId,
                              endInstanceId,
                              replyData);
        return true;
    }
}

void BatchPhase1Handler::updateEpoch(const Guid& requestEpoch) {
    if(requestEpoch != currentEpoch_) {
        MORDOR_LOG_INFO(g_log) << this << " epoch change " << currentEpoch_ <<
                                  " -> " << requestEpoch <<
                                  ", resetting acceptor";
        currentEpoch_ = requestEpoch;
        acceptorState_->reset();
    }
}

bool BatchPhase1Handler::checkRingId(uint32_t ringId) {
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

void BatchPhase1Handler::markReservedInstances(BallotId ballot,
                                               InstanceId startInstance,
                                               InstanceId endInstance,
                                               PaxosPhase1BatchReplyData*
                                                   reply)
{
    reply->set_type(PaxosPhase1BatchReplyData::OK);
    for(InstanceId iid = startInstance;
        iid < endInstance;
        ++iid)
    {
        BallotId highestPromised;
        BallotId highestVoted;
        Value    lastVote;
        if(acceptorState_->nextBallot(iid,
                                      ballot,
                                      &highestPromised,
                                      &highestVoted,
                                      &lastVote) != AcceptorState::OK)
        {
            MORDOR_LOG_TRACE(g_log) << this << " iid " << iid <<
                                       " is reserved";
            reply->add_reserved_instances(iid);
        }
    }
}


}  // namespace lightning
