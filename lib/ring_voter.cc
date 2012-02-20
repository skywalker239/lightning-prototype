#include "ring_voter.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <algorithm>

namespace lightning {

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

static Logger::ptr g_log = Log::lookup("lightning:ring_voter");

static CountStatistic<uint64_t>& g_outPackets =
    Statistics::registerStatistic("ring_voter.out_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_inPackets =
    Statistics::registerStatistic("ring_voter.in_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_outBytes =
    Statistics::registerStatistic("ring_voter.out_bytes",
                                  CountStatistic<uint64_t>("bytes"));
static CountStatistic<uint64_t>& g_inBytes =
    Statistics::registerStatistic("ring_voter.in_bytes",
                                  CountStatistic<uint64_t>("bytes"));

RingVoter::RingVoter(Socket::ptr socket,
                     AcceptorState::ptr acceptorState)
    : socket_(socket),
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
        if(!ringConfiguration->isInRing()) {
            MORDOR_LOG_TRACE(g_log) << this <<
                                       " acceptor not in ring, ignoring vote "
                                       << requestGuid;
            continue;
        }
        MORDOR_LOG_TRACE(g_log) << this << " processing vote " << requestGuid;


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
                            ringConfiguration->nextRingAddress());
            g_outBytes.add(replyData.ByteSize());
            g_outPackets.increment();
        }
    }
}

void RingVoter::initiateVote(const Guid& rpcGuid,
                             const Guid& epoch,
                             RingConfiguration::const_ptr ring,
                             InstanceId instance,
                             BallotId ballot,
                             const Guid& valueId)
{
    RpcMessageData rpcMessageData;
    rpcMessageData.set_type(RpcMessageData::PAXOS_PHASE2);
    rpcGuid.serialize(rpcMessageData.mutable_uuid());
    VoteData* voteData = rpcMessageData.mutable_vote();
    epoch.serialize(voteData->mutable_epoch());
    voteData->set_ring_id(ring->ringId());
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
    
    Address::ptr destination = ring->nextRingAddress();
    MORDOR_LOG_TRACE(g_log) << this << " sending vote (" <<
                               instance << ", " << ballot << ", " <<
                               valueId << "), ringId=" << ring->ringId() <<
                               ", epoch=" << epoch << ", rpc_uuid=" <<
                               rpcGuid << " to " << *destination;

    socket_->sendTo((const void*)buffer,
                    rpcMessageData.ByteSize(),
                    0,
                    destination);
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

    const Guid epoch = Guid::parse(voteData.epoch());
    acceptorState_->updateEpoch(epoch);

    const InstanceId instance = voteData.instance();
    const BallotId ballot = voteData.ballot();
    const Guid valueId = Guid::parse(voteData.value_id());
    const Guid rpcGuid = Guid::parse(request.uuid());
    BallotId highestPromised = kInvalidBallotId;

    AcceptorState::Status status = acceptorState_->vote(instance,
                                                        ballot,
                                                        valueId,
                                                        &highestPromised);
    if(status == AcceptorState::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " vote(" << instance << ", " <<
                                   ballot << ", " << valueId << ") ok";
        reply->MergeFrom(request);
        MORDOR_LOG_TRACE(g_log) << this << " sending vote(" << instance <<
                                   ", " << ballot << ", " << valueId <<
                                   "), ringId=" << voteData.ring_id() <<
                                   ", rpc_uuid=" << rpcGuid << " to " <<
                                   *ringConfiguration->nextRingAddress();
        return true;
    } else {
        MORDOR_LOG_DEBUG(g_log) << this << " vote(" << instance << ", " <<
                                   ballot << ", " << valueId << ") = " <<
                                   uint32_t(status) << ", promised=" <<
                                   highestPromised;
        return false;
    }
}

}  // namespace lightning
