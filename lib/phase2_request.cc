#include "phase2_request.h"
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
using std::ostream;
using std::pair;
using std::vector;
using std::set;

static Logger::ptr g_log = Log::lookup("lightning:phase2_request");

namespace {

ostream& operator<<(ostream& os,
                    const vector<pair<InstanceId, Guid> >& commits)
{
    os << "[";
    for(size_t i = 0; i < commits.size(); ++i) {
        os << "(" << commits[i].first << ", " << commits[i].second << ")";
        if(i + 1 < commits.size()) {
            os << ", ";
        }
    }
    os << "]";
    return os;
}

}  // anonymous namespace

Phase2Request::Phase2Request(
    const Guid& epoch,
    uint32_t ringId,
    InstanceId instance,
    BallotId ballot,
    Value::ptr value,
    const vector<pair<InstanceId, Guid> >& commits,
    Address::ptr lastRingHost)
    : valueId_(value->valueId),
      lastRingHost_(lastRingHost),
      status_(IN_PROGRESS),
      result_(PENDING),
      event_(true)
{
    requestData_.set_type(RpcMessageData::PAXOS_PHASE2);
    PaxosPhase2RequestData* request =
        requestData_.mutable_phase2_request();
    epoch.serialize(request->mutable_epoch());
    request->set_ring_id(ringId);
    request->set_instance(instance);
    request->set_ballot(ballot);
    serializeValue(value, request->mutable_value());
    serializeCommits(commits, request);

    MORDOR_LOG_TRACE(g_log) << this << " P2(" << epoch << ", " <<
                               ringId << ", " << instance << ", " <<
                               ballot << ", " << value->valueId << ", " <<
                               commits << ") host=" << *lastRingHost_;
 }

Phase2Request::Result Phase2Request::result() const {
    FiberMutex::ScopedLock lk(mutex_);
    return result_;
}

const RpcMessageData& Phase2Request::request() const {
    FiberMutex::ScopedLock lk(mutex_);
    return requestData_;
}

void Phase2Request::onReply(Address::ptr source,
                            const RpcMessageData& rpcReply)
{
    FiberMutex::ScopedLock lk(mutex_);

    if(*source != *lastRingHost_) {
        MORDOR_LOG_WARNING(g_log) << this << " reply from unknown address " <<
                                     *source;
        return;
    }
    
    MORDOR_ASSERT(rpcReply.has_vote());
    const VoteData& vote = rpcReply.vote();
    Guid voteValueId = Guid::parse(vote.value_id());
    MORDOR_ASSERT(valueId_ == voteValueId);
    MORDOR_LOG_TRACE(g_log) << this << " phase2 successful for iid=" <<
                               requestData_.phase2_request().instance() <<
                               ", valueId=" << valueId_;
    status_ = COMPLETED;
    result_ = SUCCESS;
    event_.set();
}

void Phase2Request::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    status_ = TIMED_OUT;
    MORDOR_LOG_TRACE(g_log) << this << " timed out";
    event_.set();
}

void Phase2Request::wait() {
    event_.wait();
    FiberMutex::ScopedLock lk(mutex_);
}

void Phase2Request::serializeValue(Value::ptr value,
                                   ValueData* valueData) const
{
    value->valueId.serialize(valueData->mutable_id());
    valueData->set_data(value->data, value->size);
}

void Phase2Request::serializeCommits(
    const vector<pair<InstanceId, Guid> >& commits,
    PaxosPhase2RequestData* request) const
{
    for(size_t i = 0; i < commits.size(); ++i) {
        CommitData* commitData = request->add_commits();
        commitData->set_instance(commits[i].first);
        commits[i].second.serialize(commitData->mutable_value_id());
    }
}

MulticastRpcRequest::Status Phase2Request::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
