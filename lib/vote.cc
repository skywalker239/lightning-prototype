#include "vote.h"
#include "ring_voter.h"

namespace lightning {

using paxos::BallotId;
using paxos::InstanceId;
using std::ostream;

Vote::Vote(const Guid& rpcGuid,
           const Guid& epoch,
           uint32_t ringId,
           InstanceId instance,
           BallotId ballot,
           const Guid& valueId,
           RingVoter::ptr ringVoter)
    : message_(new RpcMessageData),
      ringVoter_(ringVoter)
{
    message_->set_type(RpcMessageData::PAXOS_PHASE2);
    rpcGuid.serialize(message_->mutable_uuid());
    VoteData* voteData = message_->mutable_vote();
    epoch.serialize(voteData->mutable_epoch());
    voteData->set_ring_id(ringId);
    voteData->set_instance(instance);
    voteData->set_ballot(ballot);
    valueId.serialize(voteData->mutable_value_id());
}

Vote::Vote(boost::shared_ptr<RpcMessageData> pendingVote,
           RingVoter::ptr ringVoter)
    : message_(pendingVote),
      ringVoter_(ringVoter)
{}

InstanceId Vote::instance() const {
    return message_->vote().instance();
}

BallotId Vote::ballot() const {
    return message_->vote().ballot();
}

const Guid Vote::epoch() const {
    return Guid::parse(message_->vote().epoch());
}

const Guid Vote::valueId() const {
    return Guid::parse(message_->vote().value_id());
}

ostream& Vote::output(ostream& os) const {
    const VoteData& vote = message_->vote();
    os << "Vote(" << Guid::parse(message_->uuid()) << ", " << epoch() <<
          ", " << vote.ring_id() << ", " << instance() << ", " <<
          ballot() << ", " << valueId() << ")";
    return os;
}

void Vote::send() {
    ringVoter_->send(*this);
}

}  // namespace lightning
