#include "phase2_handler.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::kInvalidBallotId;
using paxos::Value;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:phase2_handler");

Phase2Handler::Phase2Handler(AcceptorState::ptr acceptorState,
                             RingVoter::ptr     ringVoter)
    : acceptorState_(acceptorState),
      ringVoter_(ringVoter)
{}

bool Phase2Handler::handleRequest(Address::ptr,
                                  const RpcMessageData& request,
                                  RpcMessageData*)
{
    const PaxosPhase2RequestData& paxosRequest =
        request.phase2_request();
    Guid rpcGuid = Guid::parse(request.uuid());
    Guid requestEpoch = Guid::parse(paxosRequest.epoch());
    const uint32_t requestRingId = paxosRequest.ring_id();
    const InstanceId instance = paxosRequest.instance();
    const BallotId ballot = paxosRequest.ballot();
    Value value;
    value.valueId = Guid::parse(paxosRequest.value().id());
    const string& valueData = paxosRequest.value().data();
    MORDOR_ASSERT(valueData.length() <= Value::kMaxValueSize);
    memcpy(value.data, valueData.c_str(), valueData.length());
    value.size = valueData.length();

    RingConfiguration::const_ptr ringConfiguration =
        tryAcquireRingConfiguration();
    if(!ringConfiguration.get()) {
        MORDOR_LOG_TRACE(g_log) << this << " no ring configuration at " <<
                                   "request " << rpcGuid;
        return false;
    }
    if(requestRingId != ringConfiguration->ringId()) {
        MORDOR_LOG_TRACE(g_log) << this << " ring id mismatch, our=" <<
                                   ringConfiguration->ringId() <<
                                   " request=" << requestRingId <<
                                   " at request " << rpcGuid;
        return false;
    }


    acceptorState_->updateEpoch(requestEpoch);
    AcceptorState::Status status = acceptorState_->beginBallot(
                                       instance,
                                       ballot,
                                       value);
    MORDOR_LOG_TRACE(g_log) << this << " phase2(" << instance << ", " <<
                               ballot << ", " << value.valueId << ") = " <<
                               uint32_t(status);
    if(status == AcceptorState::OK && canInitiateVote(ringConfiguration)) {
        MORDOR_LOG_TRACE(g_log) << this << " initiating vote (" <<
                                   instance << ", " << ballot << ", " <<
                                   value.valueId << ")";
        ringVoter_->send(Vote(rpcGuid,
                              requestEpoch,
                              ringConfiguration->ringId(),
                              instance,
                              ballot,
                              value.valueId,
                              ringVoter_));
    }

    for(int i = 0; i < paxosRequest.commits_size(); ++i) {
        const CommitData& commit = paxosRequest.commits(i);
        const InstanceId instance = commit.instance();
        Guid valueId = Guid::parse(commit.value_id());
        AcceptorState::Status status =
            acceptorState_->commit(instance, valueId);
        MORDOR_LOG_TRACE(g_log) << this << " commit(" << instance << ", " <<
                                   valueId << ") = " << uint32_t(status);
    }
    return false;
}

bool Phase2Handler::canInitiateVote(
    RingConfiguration::const_ptr ringConfiguration) const
{
    return ringConfiguration->ringIndex() == 1;
}

}  // namespace lightning
