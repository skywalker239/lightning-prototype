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
    highestVotedValueId_ = ValueId();
    valueId_ = ValueId();
    committed_ = false;
}

bool AcceptorInstance::nextBallot(BallotId ballotId,
                                  BallotId* highestBallotParticipated,
                                  BallotId* highestBallotVoted,
                                  ValueId*  highestVotedValueId)
{
    if(ballotId > highestBallotParticipated_) {
        MORDOR_LOG_TRACE(g_log) << this << " accepting nextBallot id=" <<
                                   ballotId << ", MaxBallotVoted=" <<
                                   highestBallotVoted_ << ", MaxVotedValue=" <<
                                   highestVotedValueId_;
        highestBallotParticipated_ = ballotId;
        *highestBallotParticipated = highestBallotParticipated_;
        *highestBallotVoted = highestBallotVoted_;
        *highestVotedValueId = highestVotedValueId_;
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
                                   ValueId valueId,
                                   BallotId* highestBallotPromised)
{
    //! XXX Can ballotId in beginBallot really be higher than lastBallot?
    if(ballotId > highestBallotParticipated_) {
        MORDOR_LOG_ERROR(g_log) << this << 
                                   " beginBallot with too high ballotId=" <<
                                   ballotId << ", lastBallot=" <<
                                   highestBallotParticipated_;
        MORDOR_ASSERT(1 == 0);
    } else if(ballotId < highestBallotParticipated_) {
        MORDOR_LOG_TRACE(g_log) << this << " rejecting beginBallot id=" <<
                                   ballotId << ", lastBallot=" <<
                                   highestBallotParticipated_;
        *highestBallotPromised = highestBallotParticipated_;
        return false;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " accepting beginBallot id=" <<
                                   ballotId << " with value=" << valueId;
        highestBallotVoted_ = ballotId;
        highestVotedValueId_ = valueId;
        return true;
    }
}

void AcceptorInstance::commit(ValueId valueId) {
    MORDOR_ASSERT(!committed_ || (valueId == valueId_));
    MORDOR_LOG_TRACE(g_log) << this << " commit value=" << valueId;
    
    valueId_ = valueId;
    committed_ = true;
}

bool AcceptorInstance::value(ValueId* valueId) const {
    MORDOR_LOG_TRACE(g_log) << this << " get value=" << valueId <<
                               ", committed=" << committed_;
    if(committed_) { 
        *valueId = valueId_;
        return true;
    } else {
        return false;
    }
}

}  // namespace paxos
}  // namespace lightning
