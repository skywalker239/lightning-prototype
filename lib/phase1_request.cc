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
    BallotId ballot,
    InstanceId instance,
    RingConfiguration::const_ptr ring,
    uint64_t timeoutUs)
    : MulticastRpcRequest(ring, timeoutUs),
      group_(ring->group()),
      result_(SUCCESS),
      lastPromisedBallotId_(kInvalidBallotId),
      lastVotedBallotId_(kInvalidBallotId)
{
    requestData_.set_type(RpcMessageData::PAXOS_PHASE1);
    PaxosPhase1RequestData* request =
        requestData_.mutable_phase1_request();
    epoch.serialize(request->mutable_epoch());
    request->set_ring_id(ring->ringId());
    request->set_instance(instance);
    request->set_ballot(ballot);

    MORDOR_LOG_TRACE(g_log) << this << " P1(" << epoch << ", " <<
                               ring->ringId() << ", " << instance << ", " <<
                               ballot << ")";
}

std::ostream& Phase1Request::output(std::ostream& os) const {
     const PaxosPhase1RequestData& request = requestData_.phase1_request();
     os << "P1(" << Guid::parse(request.epoch()) << ", " <<
        request.ring_id() << ", " << request.instance() << ", " <<
        request.ballot() << ")";
     return os;
}

Phase1Request::Result Phase1Request::result() const {
    return result_;
}

BallotId Phase1Request::lastVotedBallot() const {
    return lastVotedBallotId_;
}

BallotId Phase1Request::lastPromisedBallot() const {
    return lastPromisedBallotId_;
}

Value::ptr Phase1Request::lastVotedValue() const {
    return lastVotedValue_;
}

void Phase1Request::applyReply(uint32_t hostId,
                               const RpcMessageData& rpcReply)
{
    MORDOR_ASSERT(rpcReply.has_phase1_reply());
    const PaxosPhase1ReplyData& reply = rpcReply.phase1_reply();
    switch(reply.type()) {
        case PaxosPhase1ReplyData::FORGOTTEN:
            result_ = FORGOTTEN;
            MORDOR_LOG_TRACE(g_log) << this << " FORGOTTEN from " <<
                                       group_->host(hostId);
            break;
        case PaxosPhase1ReplyData::BALLOT_TOO_LOW:
            result_ = (result_ == FORGOTTEN) ? result_ : BALLOT_TOO_LOW;
            lastPromisedBallotId_ = max(lastPromisedBallotId_,
                                        reply.last_ballot_id());
            MORDOR_LOG_TRACE(g_log) << this << " BALLOT_TOO_LOW(" <<
                                       reply.last_ballot_id() << ", " <<
                                       "lastPromisedBallot=" <<
                                       lastPromisedBallotId_ << ") from " <<
                                       group_->host(hostId);
            break;
        case PaxosPhase1ReplyData::OK:
            if(!reply.has_last_ballot_id()) {
                MORDOR_LOG_TRACE(g_log) << this << " OK from " <<
                                           group_->host(hostId);
            } else {
                MORDOR_LOG_TRACE(g_log) << this << " OK(" <<
                                           reply.last_ballot_id() << ") " <<
                                           "from " << group_->host(hostId);
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
}

Value::ptr Phase1Request::parseValue(const ValueData& valueData) const {
    Value::ptr value(new Value);
    value->valueId = Guid::parse(valueData.id());
    value->size = valueData.data().length();
    MORDOR_ASSERT(valueData.data().length() <= Value::kMaxValueSize);
    memcpy(value->data, valueData.data().c_str(), valueData.data().length());
    MORDOR_LOG_TRACE(g_log) << this << " parsed value id=" << value->valueId <<
                               ", size=" << value->size;
    return value;
}

}  // namespace lightning
