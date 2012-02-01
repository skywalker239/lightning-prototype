#include "phase1_request.h"
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
using paxos::kInvalidBallotId;
using paxos::Value;
using paxos::InstanceId;
using std::make_pair;
using std::map;
using std::min;
using std::max;
using std::vector;
using std::set;

static Logger::ptr g_log = Log::lookup("lightning:phase1_request");

Phase1Request::Phase1Request(
    const Guid& epoch,
    uint32_t ringId,
    BallotId ballot,
    InstanceId instance,
    const vector<Address::ptr>& requestRing)
    : notAcked_(requestRing.begin(), requestRing.end()),
      status_(IN_PROGRESS),
      result_(PENDING),
      lastPromisedBallotId_(kInvalidBallotId),
      lastVotedBallotId_(kInvalidBallotId),
      event_(true)
{
    requestData_.set_type(RpcMessageData::PAXOS_PHASE1);
    PaxosPhase1RequestData* request =
        requestData_.mutable_phase1_request();
    epoch.serialize(request->mutable_epoch());
    request->set_ring_id(ringId);
    request->set_instance(instance);
    request->set_ballot(ballot);

    MORDOR_LOG_TRACE(g_log) << this << "P1(" << epoch << ", " <<
                               ringId << ", " << instance << ", " <<
                               ballot << ")";
 }

Phase1Request::Result Phase1Request::result() const {
    FiberMutex::ScopedLock lk(mutex_);
    return result_;
}

BallotId Phase1Request::lastVotedBallot() const {
    FiberMutex::ScopedLock lk(mutex_);
    return lastVotedBallotId_;
}

BallotId Phase1Request::lastPromisedBallot() const {
    FiberMutex::ScopedLock lk(mutex_);
    return lastPromisedBallotId_;
}

Value::ptr Phase1Request::lastVotedValue() const {
    FiberMutex::ScopedLock lk(mutex_);
    return lastVotedValue_;
}

const RpcMessageData& Phase1Request::request() const {
    FiberMutex::ScopedLock lk(mutex_);
    return requestData_;
}

void Phase1Request::onReply(Address::ptr source,
                            const RpcMessageData& rpcReply)
{
    FiberMutex::ScopedLock lk(mutex_);

    if(notAcked_.find(source) == notAcked_.end()) {
        MORDOR_LOG_WARNING(g_log) << this << " reply from unknown address " <<
                                     *source;
        return;
    }
    
    MORDOR_ASSERT(rpcReply.has_phase1_reply());
    const PaxosPhase1ReplyData& reply = rpcReply.phase1_reply();
    switch(reply.type()) {
        case PaxosPhase1ReplyData::BALLOT_TOO_LOW:
            result_ = BALLOT_TOO_LOW;
            lastPromisedBallotId_ = max(lastPromisedBallotId_,
                                        reply.last_ballot_id());
            MORDOR_LOG_TRACE(g_log) << this << " BALLOT_TOO_LOW(" <<
                                       reply.last_ballot_id() << ", new " <<
                                       "lastPromisedBallot=" <<
                                       lastPromisedBallotId_;
            break;
        case PaxosPhase1ReplyData::OK:
            if(!reply.has_last_ballot_id()) {
                MORDOR_LOG_TRACE(g_log) << this << " OK from " << *source;
            } else {
                MORDOR_LOG_TRACE(g_log) << this << " OK(" <<
                                           reply.last_ballot_id() << ") " <<
                                           "from " << *source;
                if(reply.last_ballot_id() > lastVotedBallotId_) {
                    lastVotedBallotId_ = reply.last_ballot_id();
                    lastVotedValue_ = parseValue(reply.value());
                }
                MORDOR_LOG_TRACE(g_log) << this << " lastVotedBallot=" <<
                                           lastVotedBallotId_ << "," <<
                                           " lastVotedValueId=" <<
                                           lastVotedValue_->valueId;
            }
            break;
    }
    notAcked_.erase(source);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " got all replies";
        result_ = (result_ == PENDING) ? SUCCESS : result_;
        status_ = COMPLETED;
        event_.set();
    }
}

void Phase1Request::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " onTimeout() with no pending acks";
        status_ = COMPLETED;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " timed out";
        status_ = TIMED_OUT;
    }
    event_.set();
}

void Phase1Request::wait() {
    event_.wait();
    FiberMutex::ScopedLock lk(mutex_);
}

Value::ptr Phase1Request::parseValue(const ValueData& valueData) const {
    Value::ptr value(new Value);
    value->valueId.parse(valueData.id());
    value->size = valueData.data().length();
    MORDOR_ASSERT(valueData.data().length() <= Value::kMaxValueSize);
    memcpy(value->data, valueData.data().c_str(), valueData.data().length());
    MORDOR_LOG_TRACE(g_log) << this << " parsed value id=" << value->valueId <<
                               ", size=" << value->size;
    return value;
}

MulticastRpcRequest::Status Phase1Request::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
