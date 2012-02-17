#include "batch_phase1_request.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <algorithm>

namespace lightning {

using Mordor::Log;
using Mordor::Logger;
using paxos::BallotId;
using paxos::InstanceId;
using std::make_pair;
using std::map;
using std::min;
using std::max;
using std::vector;
using std::set;

static Logger::ptr g_log = Log::lookup("lightning:batch_phase1_request");

BatchPhase1Request::BatchPhase1Request(
    const Guid& epoch,
    BallotId ballotId,
    InstanceId instanceRangeBegin,
    InstanceId instanceRangeEnd,
    RingConfiguration::const_ptr ring,
    uint64_t timeoutUs)
    : MulticastRpcRequest(ring, timeoutUs),
      group_(ring->group()),
      result_(PENDING),
      retryStartInstanceId_(0)
{
    requestData_.set_type(RpcMessageData::PAXOS_BATCH_PHASE1);
    PaxosPhase1BatchRequestData* request =
        requestData_.mutable_phase1_batch_request();
    epoch.serialize(request->mutable_epoch());
    request->set_ring_id(ring->ringId());
    request->set_ballot_id(ballotId);
    request->set_start_instance_id(instanceRangeBegin);
    request->set_end_instance_id(instanceRangeEnd);

    MORDOR_LOG_TRACE(g_log) << this << " BatchP1(" << epoch << ", " <<
                               ring->ringId() << ", " << ballotId << ", [" <<
                               instanceRangeBegin << ", " <<
                               instanceRangeEnd << "))";    
}

std::ostream& BatchPhase1Request::output(std::ostream& os) const {
    const PaxosPhase1BatchRequestData& request =
        requestData_.phase1_batch_request();
    os << "BatchP1(" << Guid::parse(request.epoch()) << ", " <<
       request.ring_id() << ", " << request.ballot_id() << ", [" <<
       request.start_instance_id() << ", " << request.end_instance_id() <<
       "))";
   return os;
}

BatchPhase1Request::Result BatchPhase1Request::result() const {
    return result_;
}

const set<InstanceId>& BatchPhase1Request::reservedInstances() const
{
    return reservedInstances_;
}

InstanceId BatchPhase1Request::retryStartInstanceId() const {
    return retryStartInstanceId_;
}

const RpcMessageData& BatchPhase1Request::request() const {
    return requestData_;
}

void BatchPhase1Request::applyReply(uint32_t hostId,
                                    const RpcMessageData& rpcReply)
{
    MORDOR_ASSERT(rpcReply.has_phase1_batch_reply());
    const PaxosPhase1BatchReplyData& reply = rpcReply.phase1_batch_reply();
    switch(reply.type()) {
        case PaxosPhase1BatchReplyData::IID_TOO_LOW:
            result_ = IID_TOO_LOW;
            retryStartInstanceId_ = min(retryStartInstanceId_,
                                        reply.retry_iid());
            MORDOR_LOG_TRACE(g_log) << this << " IID_TOO_LOW(" <<
                                       reply.retry_iid() << ") from " <<
                                       group_->host(hostId) <<
                                       ", new start_iid=" <<
                                       retryStartInstanceId_;
            break;
        case PaxosPhase1BatchReplyData::OK:
            result_ = (result_ == IID_TOO_LOW) ? IID_TOO_LOW : SUCCESS;
            MORDOR_LOG_TRACE(g_log) << this << " OK from " <<
                                       group_->host(hostId);
            for(int i = 0; i < reply.reserved_instances_size(); ++i) {
                const uint64_t& reservedInstance =
                    reply.reserved_instances(i);
                MORDOR_LOG_TRACE(g_log) << this << " instance " <<
                                           reservedInstance << " reserved " <<
                                           "on " << group_->host(hostId);
                reservedInstances_.insert(reservedInstance);
            }
            break;
    }
}

}  // namespace lightning
