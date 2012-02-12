#include "batch_phase1_request.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <algorithm>

namespace lightning {

using Mordor::Address;
using Mordor::FiberEvent;
using Mordor::FiberMutex;
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
    RingConfiguration::const_ptr ring)
    : ring_(ring),
      notAckedMask_(ring_->ringMask()),
      status_(IN_PROGRESS),
      result_(PENDING),
      retryStartInstanceId_(0),
      event_(true)
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

BatchPhase1Request::Result BatchPhase1Request::result() const {
    FiberMutex::ScopedLock lk(mutex_);
    return result_;
}

const set<InstanceId>& BatchPhase1Request::reservedInstances() const
{
    FiberMutex::ScopedLock lk(mutex_);
    return reservedInstances_;
}

InstanceId BatchPhase1Request::retryStartInstanceId() const {
    FiberMutex::ScopedLock lk(mutex_);
    return retryStartInstanceId_;
}

const RpcMessageData& BatchPhase1Request::request() const {
    FiberMutex::ScopedLock lk(mutex_);
    return requestData_;
}

void BatchPhase1Request::onReply(Address::ptr source,
                                 const RpcMessageData& rpcReply)
{
    FiberMutex::ScopedLock lk(mutex_);

    const uint32_t hostId = ring_->replyAddressToId(source);
    if(hostId == GroupConfiguration::kInvalidHostId) {
        MORDOR_LOG_WARNING(g_log) << this << " reply from unknown address " <<
                                     *source;
        return;
    }

    MORDOR_ASSERT(rpcReply.has_phase1_batch_reply());
    const PaxosPhase1BatchReplyData& reply = rpcReply.phase1_batch_reply();
    switch(reply.type()) {
        case PaxosPhase1BatchReplyData::IID_TOO_LOW:
            result_ = IID_TOO_LOW;
            retryStartInstanceId_ = min(retryStartInstanceId_,
                                        reply.retry_iid());
            MORDOR_LOG_TRACE(g_log) << this << " IID_TOO_LOW(" <<
                                       reply.retry_iid() << ") from " <<
                                       *source << "(" << hostId <<
                                       "), new start_iid=" <<
                                       retryStartInstanceId_;
            break;
        case PaxosPhase1BatchReplyData::OK:
            MORDOR_LOG_TRACE(g_log) << this << " OK from " << *source <<
                                               "(" << hostId << ")";
            for(int i = 0; i < reply.reserved_instances_size(); ++i) {
                const uint64_t& reservedInstance =
                    reply.reserved_instances(i);
                MORDOR_LOG_TRACE(g_log) << this << " instance " <<
                                           reservedInstance << " reserved " <<
                                           "on " << *source << "(" <<
                                           hostId << ")";
                reservedInstances_.insert(reservedInstance);
            }
            break;
    }

    notAckedMask_ &= ~(1 << hostId);
    if(notAckedMask_ == 0) {
        MORDOR_LOG_TRACE(g_log) << this << " got all replies";
        result_ = (result_ == PENDING) ? SUCCESS : result_;
        status_ = COMPLETED;
        event_.set();
    }
}

void BatchPhase1Request::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    if(notAckedMask_ == 0) {
        MORDOR_LOG_TRACE(g_log) << this << " onTimeout() with no pending acks";
        status_ = COMPLETED;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " timed out, notAckedMask=" <<
                                   notAckedMask_;
        status_ = TIMED_OUT;
    }
    event_.set();
}

void BatchPhase1Request::wait() {
    event_.wait();
    FiberMutex::ScopedLock lk(mutex_);
}

MulticastRpcRequest::Status BatchPhase1Request::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
