#include "ring_voter.h"
#include "acceptor_instance.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {
namespace paxos {

using Mordor::Logger;
using Mordor::Log;
using Mordor::Statistics;
using Mordor::CountStatistic;
using std::max;

static Logger::ptr g_log = Log::lookup("lightning:acceptor_instance");

static CountStatistic<uint64_t>& g_phase1Fails =
    Statistics::registerStatistic("acceptor.phase1_fails",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_phase2Fails =
    Statistics::registerStatistic("acceptor.phase2_fails",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_voteFails =
    Statistics::registerStatistic("acceptor.vote_fails",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_unknownValueVotes =
    Statistics::registerStatistic("acceptor.unknown_value_votes",
                                   CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_recoveredVotes =
    Statistics::registerStatistic("acceptor.recovered_votes",
                                  CountStatistic<uint64_t>());

AcceptorInstance::AcceptorInstance() {
    reset();
}

AcceptorInstance::AcceptorInstance(const Value& value,
                                   BallotId ballot)
    : highestPromisedBallot_(ballot),
      highestVotedBallot_(ballot),
      lastVotedValue_(value),
      pendingVote_(boost::none),
      committed_(true)
{}

void AcceptorInstance::reset() {
    MORDOR_LOG_TRACE(g_log) << this << " reset";
    highestPromisedBallot_ = kInvalidBallotId;
    highestVotedBallot_ = kInvalidBallotId;
    lastVotedValue_ = Value();
    pendingVote_ = boost::none;
    committed_ = false;
}

bool AcceptorInstance::nextBallot(BallotId  ballotId,
                                  BallotId* highestPromisedBallot,
                                  BallotId* highestVotedBallot,
                                  Value*    lastVote)
{
    //! Reject the phase 1 request for the same ballot number as
    //  Paxos is designed for unique ballot numbers.
    if(ballotId > highestPromisedBallot_) {
        MORDOR_LOG_TRACE(g_log) << this << " accepting nextBallot id=" <<
                                   ballotId << ", highestVotedBallot=" <<
                                   highestVotedBallot_ << ", lastVotedValue=" <<
                                   lastVotedValue_.valueId;
        highestPromisedBallot_ = ballotId;
        *highestPromisedBallot = highestPromisedBallot_;
        *highestVotedBallot = highestVotedBallot_;
        *lastVote = lastVotedValue_;
        return true;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " rejecting nextBallot id=" <<
                                           ballotId <<
                                           ", highestPromisedBallot=" <<
                                           highestPromisedBallot_;
        *highestPromisedBallot = highestPromisedBallot_;
        g_phase1Fails.increment();
        return false;
    }
}

bool AcceptorInstance::beginBallot(BallotId ballotId,
                                   const Value& value)
{
    if(ballotId < highestPromisedBallot_) {
        MORDOR_LOG_TRACE(g_log) << this << " rejecting beginBallot id=" <<
                                   ballotId << ", highestPromisedBallot=" <<
                                   highestPromisedBallot_;
        g_phase2Fails.increment();
        return false;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " accepting beginBallot id=" <<
                                   ballotId << " with value=" << value.valueId;
        highestVotedBallot_ = max(ballotId, highestVotedBallot_);
        lastVotedValue_ = value;
        if(!!pendingVote_) {
            if(pendingVote_->ballot() == ballotId &&
               pendingVote_->valueId() == lastVotedValue_.valueId)
            {
                g_recoveredVotes.increment();
                MORDOR_LOG_TRACE(g_log) << this << " recovered " <<
                                           *pendingVote_;
                pendingVote_->send();
            }

            pendingVote_.reset();
        }

        return true;
    }
}

bool AcceptorInstance::vote(const Vote& vote,
                            BallotId* highestBallotPromised)
{
    if(vote.ballot() < highestPromisedBallot_) {
        MORDOR_LOG_TRACE(g_log) << this << " not voting in ballot " <<
                                   vote.ballot() << ", promised " <<
                                   highestPromisedBallot_;
        *highestBallotPromised = highestPromisedBallot_;
        g_voteFails.increment();
        return false;
    }
    if(lastVotedValue_.valueId != vote.valueId()) {
        MORDOR_LOG_TRACE(g_log) << this << " value unknown for " <<
                                   vote;
        pendingVote_ = vote;
        *highestBallotPromised = kInvalidBallotId;
        g_unknownValueVotes.increment();
        return false;
    }
    highestVotedBallot_ = max(highestVotedBallot_, vote.ballot());
    MORDOR_LOG_TRACE(g_log) << this << " " << vote << " successful";
    return true;
}

bool AcceptorInstance::commit(const Guid& valueId) {
    if(lastVotedValue_.valueId != valueId) {
        MORDOR_LOG_TRACE(g_log) << this << " cannot commit " << valueId <<
                                   ", value unknown";
        return false;
    }
    MORDOR_LOG_TRACE(g_log) << this << " committing " << valueId;
    committed_ = true;
    return true;
}

bool AcceptorInstance::value(Value* value, BallotId* ballot) const {
    MORDOR_LOG_TRACE(g_log) << this << " get value=" <<
                               lastVotedValue_.valueId <<
                               ", committed=" << committed_;
    if(committed_) { 
        *value = lastVotedValue_;
        *ballot = highestVotedBallot_;
        return true;
    } else {
        return false;
    }
}

}  // namespace paxos
}  // namespace lightning
