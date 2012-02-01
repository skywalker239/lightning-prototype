#include "ring_voter.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <algorithm>

namespace lightning {

using Mordor::Address;
using Mordor::Socket;
using Mordor::Logger;
using Mordor::Log;
using paxos::BallotId;
using paxos::kInvalidBallotId;
using paxos::InstanceId;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ring_voter");

RingVoter::RingVoter(const GroupConfiguration& groupConfiguration,
                     Socket::ptr socket,
                     AcceptorState::ptr acceptorState)
    : groupConfiguration_(groupConfiguration),
      socket_(socket),
      acceptorState_(acceptorState)
{}

void RingVoter::run() {
    Address::ptr remoteAddress = socket_->emptyAddress();
    while(true) {
        char buffer[kMaxDatagramSize];
        ssize_t bytes = socket_->receiveFrom((void*)buffer,
                                             sizeof(buffer),
                                             *remoteAddress);
        RpcMessageData requestData;
        if(!requestData.ParseFromArray(buffer, bytes)) {
            MORDOR_LOG_WARNING(g_log) << this << " malformed " << bytes <<
                                         " bytes from " << *remoteAddress;
            continue;
        }
        const Guid requestGuid = Guid::parse(requestData.uuid());
        
        RingConfiguration::const_ptr ringConfiguration =
            tryAcquireRingConfiguration();

        if(!ringConfiguration.get()) {
            MORDOR_LOG_WARNING(g_log) << this << " no ring configuration," <<
                                         " ignoring vote " << requestGuid;
            continue;
        }
        RpcMessageData replyData;

        if(processVote(ringConfiguration, requestData, &replyData)) {
            char replyBuffer[kMaxDatagramSize];
            if(!replyData.SerializeToArray(replyBuffer, kMaxDatagramSize)) {
                MORDOR_LOG_WARNING(g_log) << this << " failed to serialize " <<
                                             "reply for vote " << requestGuid;
                continue;
            }
            socket_->sendTo((const void*) buffer,
                            replyData.ByteSize(),
                            0,
                            voteDestination(ringConfiguration));
        }
    }
}

void RingVoter::initiateVote(const Guid& rpcGuid,
                             const Guid& epoch,
                             uint32_t ringId,
                             InstanceId instance,
                             BallotId ballot,
                             const Guid& valueId)
{
    MORDOR_ASSERT(groupConfiguration_.thisHostId() == 1);

    RingConfiguration::const_ptr ringConfiguration =
        tryAcquireRingConfiguration();
    if(!ringConfiguration.get()) {
        MORDOR_LOG_TRACE(g_log) << this << " no ring configuration, " <<
                                   "can't initiate vote for (" <<
                                   instance << ", " << ballot << ", " <<
                                   valueId << ")";
        return;
    }
    if(ringId != ringConfiguration->ringId()) {
        MORDOR_LOG_TRACE(g_log) << this << " current ring id is " <<
                                   ringConfiguration->ringId() <<
                                   ", cannot initiate vote for (" <<
                                   instance << ", " << ballot << ", " <<
                                   valueId << ") on ring " << ringId;
        return;
    }

    RpcMessageData rpcMessageData;
    rpcMessageData.set_type(RpcMessageData::PAXOS_PHASE2);
    rpcGuid.serialize(rpcMessageData.mutable_uuid());
    VoteData* voteData = rpcMessageData.mutable_vote();
    epoch.serialize(voteData->mutable_epoch());
    voteData->set_ring_id(ringId);
    voteData->set_instance(instance);
    voteData->set_ballot(ballot);
    valueId.serialize(voteData->mutable_value_id());

    char buffer[kMaxDatagramSize];
    if(!rpcMessageData.SerializeToArray(buffer, kMaxDatagramSize)) {
        MORDOR_LOG_WARNING(g_log) << this << " cannot serialize vote (" <<
                                     instance << ", " << ballot << ", " <<
                                     valueId << ")";
        return;
    }
    
    MORDOR_LOG_TRACE(g_log) << this << " sending vote (" <<
                               instance << ", " << ballot << ", " <<
                               valueId << "), ringId=" << ringId <<
                               ", epoch=" << epoch << ", rpc_uuid=" <<
                               rpcGuid;

    socket_->sendTo((const void*)buffer,
                    rpcMessageData.ByteSize(),
                    0,
                    voteDestination(ringConfiguration));
}

bool RingVoter::processVote(RingConfiguration::const_ptr ringConfiguration,
                            const RpcMessageData& request,
                            RpcMessageData* reply)
{
    const VoteData& voteData = request.vote();
    if(voteData.ring_id() != ringConfiguration->ringId()) {
        MORDOR_LOG_TRACE(g_log) << this << " vote ring id=" <<
                                   voteData.ring_id() << ", current=" <<
                                   ringConfiguration->ringId() <<
                                   ", discarding";
        return false;
    }

    // XXX ignore epoch for now
    const InstanceId instance = voteData.instance();
    const BallotId ballot = voteData.ballot();
    const Guid valueId = Guid::parse(voteData.value_id());
    BallotId highestPromised = kInvalidBallotId;

    AcceptorState::Status status = acceptorState_->vote(instance,
                                                        ballot,
                                                        valueId,
                                                        &highestPromised);
    if(status == AcceptorState::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " vote(" << instance << ", " <<
                                   ballot << ", " << valueId << ") ok";
        reply->MergeFrom(request);
        return true;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " vote(" << instance << ", " <<
                                   ballot << ", " << valueId << ") = " <<
                                   uint32_t(status) << ", promised=" <<
                                   highestPromised;
        return false;
    }
}

Address::ptr RingVoter::voteDestination(
    RingConfiguration::const_ptr currentRing) const
{
    const vector<uint32_t> ringHostIds = currentRing->ringHostIds();
    const uint32_t myId = groupConfiguration_.thisHostId();
    size_t myRingIndex = std::find(ringHostIds.begin(),
                                   ringHostIds.end(),
                                   myId) - ringHostIds.begin();
    MORDOR_ASSERT(myRingIndex < ringHostIds.size());
    uint32_t nextHostId = ringHostIds[(myRingIndex + 1) % ringHostIds.size()];

    //! The last vote to master is sent as a reply to multicast rpc,
    //  all others are forwarded to the ring ports
    //  XXX fixed master dependency
    if(nextHostId != 0) {
        return groupConfiguration_.hosts()[nextHostId].ringAddress;
    } else {
        return groupConfiguration_.hosts()[nextHostId].multicastSourceAddress;
    }
}

}  // namespace lightning
