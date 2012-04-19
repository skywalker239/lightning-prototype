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
    const Value& value,
    const vector<pair<InstanceId, Guid> >& commits,
    RingConfiguration::const_ptr ring,
    uint64_t timeoutUs)
    : MulticastRpcRequest(ring, timeoutUs),
      valueId_(value.valueId()),
      group_(ring->group()),
      result_(PENDING)
{
    requestData_.set_type(RpcMessageData::PAXOS_PHASE2);
    PaxosPhase2RequestData* request =
        requestData_.mutable_phase2_request();
    epoch.serialize(request->mutable_epoch());
    request->set_ring_id(ringId);
    request->set_instance(instance);
    request->set_ballot(ballot);
    value.serialize(request->mutable_value());
    serializeCommits(commits, request);

    MORDOR_LOG_TRACE(g_log) << this << " P2(" << epoch << ", " <<
                               ringId << ", " << instance << ", " <<
                               ballot << ", " << valueId_ << ", " <<
                               commits << ") ring=" << *ring;
}

std::ostream& Phase2Request::output(std::ostream& os) const {
    const PaxosPhase2RequestData& request = requestData_.phase2_request();
    os << "P2(" << Guid::parse(request.epoch()) << ", " <<
       request.ring_id() << ", " << request.instance() << ", " <<
       request.ballot() << ", Value(" << Guid::parse(request.value().id()) <<
       ", " << request.value().data().length() << "), [";
    for(int i = 0; i < request.commits_size(); ++i) {
        const CommitData& commit = request.commits(i);
        os << "(" << commit.instance() << ", " <<
           Guid::parse(commit.value_id()) << ")";
        if(i + 1 < request.commits_size()) {
            os << ", ";
        }
    }
    os << "])";
    return os;
}

Phase2Request::Result Phase2Request::result() const {
    return result_;
}

void Phase2Request::applyReply(uint32_t /* hostId */,
                               const RpcMessageData& rpcReply)
{
    MORDOR_ASSERT(rpcReply.has_vote());
    const VoteData& vote = rpcReply.vote();
    Guid voteValueId = Guid::parse(vote.value_id());
    MORDOR_ASSERT(valueId_ == voteValueId);
    MORDOR_LOG_TRACE(g_log) << this << " phase2 successful for iid=" <<
                               requestData_.phase2_request().instance() <<
                               ", valueId=" << valueId_;
    result_ = SUCCESS;
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

}  // namespace lightning
