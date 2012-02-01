#include "acceptor_instance.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {
namespace paxos {

using Mordor::Logger;
using Mordor::Log;

static Logger::ptr g_log = Log::lookup("lightning:acceptor_instance");

AcceptorInstance::AcceptorInstance() {
    reset();
}

void AcceptorInstance::reset() {
    MORDOR_LOG_TRACE(g_log) << this << " reset";
    highestBallotParticipated_ = kInvalidBallotId;
    highestBallotVoted_ = kInvalidBallotId;
    lastVotedValue_ = Value();
    committedValueId_ = Guid();
    committed_ = false;
}

bool AcceptorInstance::nextBallot(BallotId  ballotId,
                                  BallotId* highestBallotParticipated,
                                  BallotId* highestBallotVoted,
                                  Value*    lastVote)
{
    if(ballotId >= highestBallotParticipated_) {
        MORDOR_LOG_TRACE(g_log) << this << " accepting nextBallot id=" <<
                                   ballotId << ", MaxBallotVoted=" <<
                                   highestBallotVoted_ << ", MaxVotedValue=" <<
                                   lastVotedValue_.valueId;
        highestBallotParticipated_ = ballotId;
        *highestBallotParticipated = highestBallotParticipated_;
        *highestBallotVoted = highestBallotVoted_;
        *lastVote = lastVotedValue_;
        return true;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " rejecting nextBallot id=" <<
                                           ballotId << ", lastBallot=" <<
                                           highestBallotParticipated_;
        *highestBallotParticipated = highestBallotParticipated_;
        return false;
    }
}

bool AcceptorInstance::beginBallot(BallotId ballotId,
                                   const Value& value)
{
    if(ballotId < highestBallotParticipated_) {
        MORDOR_LOG_TRACE(g_log) << this << " rejecting beginBallot id=" <<
                                   ballotId << ", lastBallot=" <<
                                   highestBallotParticipated_;
        return false;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " accepting beginBallot id=" <<
                                   ballotId << " with value=" << value.valueId;
        highestBallotVoted_ = ballotId;
        lastVotedValue_ = value;
        return true;
    }
}

bool AcceptorInstance::vote(BallotId ballotId,
                            const Guid& valueId,
                            BallotId* highestBallotPromised)
{
    if(ballotId < highestBallotParticipated_) {
        MORDOR_LOG_TRACE(g_log) << this << " not voting in ballot " <<
                                   ballotId << ", promised " <<
                                   highestBallotParticipated_;
        *highestBallotPromised = highestBallotParticipated_;
        return false;
    }
    if(lastVotedValue_.valueId != valueId) {
        MORDOR_LOG_TRACE(g_log) << this << " not voting in ballot " <<
                                   ballotId << " for valueId " <<
                                   valueId << ", value unknown";
        *highestBallotPromised = kInvalidBallotId;
        return false;
    }
    MORDOR_LOG_TRACE(g_log) << this << " voting in ballot " << ballotId <<
                            " for" << valueId;
    return true;
}

bool AcceptorInstance::commit(const Guid& valueId) {
    if(lastVotedValue_.valueId != valueId) {
        MORDOR_LOG_TRACE(g_log) << this << " cannot commit " << valueId <<
                                   ", value unknown";
        return false;
    }
    MORDOR_LOG_TRACE(g_log) << this << " committing " << valueId;
    MORDOR_ASSERT(committedValueId_.empty() ||
                  committedValueId_ == valueId);
    committedValueId_ = lastVotedValue_.valueId;
    committed_ = true;
    return true;
}

bool AcceptorInstance::value(Value* value) const {
    MORDOR_LOG_TRACE(g_log) << this << " get value=" <<
                               lastVotedValue_.valueId <<
                               ", committed=" << committed_;
    if(committed_) { 
        *value = lastVotedValue_;
        return true;
    } else {
        return false;
    }
}

}  // namespace paxos
}  // namespace lightning
