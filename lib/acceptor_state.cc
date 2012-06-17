#include "acceptor_state.h"
#include "recovery_manager.h"
#include "ring_voter.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::AverageMinMaxStatistic;
using Mordor::CountStatistic;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Logger;
using Mordor::Log;
using Mordor::MaxStatistic;
using Mordor::Statistics;
using Mordor::Timer;
using paxos::AcceptorInstance;
using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using std::make_pair;
using std::map;
using std::set;

static Logger::ptr g_log = Log::lookup("lightning:acceptor_state");

CountStatistic<uint64_t>& g_pendingInstances =
    Statistics::registerStatistic("acceptor_state.pending_instances",
                                  CountStatistic<uint64_t>());

AcceptorState::AcceptorState(uint32_t pendingInstancesSpan,
                             IOManager* ioManager,
                             RecoveryManager::ptr recoveryManager,
                             CommitTracker::ptr commitTracker,
                             ValueCache::ptr    valueCache)
    : pendingInstancesSpan_(pendingInstancesSpan),
      ioManager_(ioManager),
      recoveryManager_(recoveryManager),
      commitTracker_(commitTracker),
      valueCache_(valueCache)
{}

AcceptorState::Status AcceptorState::nextBallot(const Guid& epoch,
                                                InstanceId instanceId,
                                                BallotId  ballotId,
                                                BallotId* highestPromised,
                                                BallotId* highestVoted,
                                                Value*    lastVote)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateEpoch(epoch);
    if(valueCache_) { // HACK(skywalker)
        switch(tryNextBallotOnCommitted(instanceId,
                                        ballotId,
                                        highestPromised,
                                        highestVoted,
                                        lastVote))
        {
            case ValueCache::OK:
                return OK;
                break;
            case ValueCache::TOO_OLD: case ValueCache::WRONG_EPOCH:
                MORDOR_LOG_TRACE(g_log) << this << " iid " << instanceId <<
                    " forgotten/epoch wrong";
                return TOO_OLD;
                break;
            default:
                break;
        }
    }
    AcceptorInstance* instance = lookupInstance(instanceId);
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << " nextBallot(" << instanceId <<
                                   ") refused";
        return REFUSED;
    }
    bool result =  instance->nextBallot(ballotId,
                                        highestPromised,
                                        highestVoted,
                                        lastVote);
    MORDOR_LOG_TRACE(g_log) << this << " nextBallot(" << instanceId <<
                               ", " << ballotId << ") = " << result;
    return boolToStatus(result);
}

ValueCache::QueryResult AcceptorState::tryNextBallotOnCommitted(
    InstanceId instanceId,
    BallotId   ballotId,
    BallotId*  highestPromised,
    BallotId*  highestVoted,
    Value*     lastVote)
{
    auto result = valueCache_->query(epoch_, instanceId, lastVote);
    if(result == ValueCache::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " nextBallot(" << instanceId <<
            ", " << ballotId << ") hit committed " << *lastVote;
        *highestPromised = ballotId;
        *highestVoted = ballotId - 1; // HACK(skywalker)
    }
    return result;
}

AcceptorState::Status AcceptorState::beginBallot(const Guid& epoch,
                                                 InstanceId instanceId,
                                                 BallotId ballotId,
                                                 const Value& value)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateEpoch(epoch);
    if(valueCache_) { // HACK(skywalker)
        switch(tryBeginBallotOnCommitted(instanceId, ballotId, value)) {
            case ValueCache::OK:
                return OK;
                break;
            case ValueCache::TOO_OLD: case ValueCache::WRONG_EPOCH:
                MORDOR_LOG_TRACE(g_log) << this << " iid " << instanceId <<
                    " forgotten/epoch wrong";
                return TOO_OLD;
                break;
            default:
                break;
        }
    }

    AcceptorInstance* instance = lookupInstance(instanceId);
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << " beginBallot(" << instanceId <<
                                   ") refused";
        return REFUSED;
    }

    bool result = instance->beginBallot(ballotId,
                                        value);
    MORDOR_LOG_TRACE(g_log) << this << " beginBallot(" << instanceId <<
                               ", " << ballotId << ", " <<
                               value << ") = " << result;
    return boolToStatus(result);
}

ValueCache::QueryResult AcceptorState::tryBeginBallotOnCommitted(
    InstanceId instanceId,
    BallotId   ballotId,
    const Value& value)
{
    Value committedValue;
    auto result = valueCache_->query(epoch_, instanceId, &committedValue);
    if(result == ValueCache::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " beginBallot(" << instanceId <<
            ", " << ballotId << ", " << value << ") hit committed " <<
            committedValue;
        MORDOR_ASSERT(value.valueId() == committedValue.valueId());
    }
    return result;
}

AcceptorState::Status AcceptorState::vote(const Guid& epoch,
                                          const Vote& vote,
                                          BallotId* highestPromised)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateEpoch(epoch);
    if(valueCache_) { // HACK(skywalker)
        switch(tryVoteOnCommitted(vote, highestPromised)) {
            case ValueCache::OK:
                return OK;
                break;
            case ValueCache::TOO_OLD: case ValueCache::WRONG_EPOCH:
                MORDOR_LOG_TRACE(g_log) << this << " iid " << vote.instance() <<
                    " forgotten/epoch wrong";
                return TOO_OLD;
                break;
            default:
                break;
        }
    }
    AcceptorInstance* instance = lookupInstance(vote.instance());
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << " " << vote << " refused";
        return REFUSED;
    }
    bool result = instance->vote(vote, highestPromised);
    MORDOR_LOG_TRACE(g_log) << this << " " << vote << " = " << result;
    return boolToStatus(result);
}

ValueCache::QueryResult AcceptorState::tryVoteOnCommitted(
    const Vote& vote,
    BallotId* highestBallotPromised)
{
    Value committedValue;
    auto result = valueCache_->query(epoch_, vote.instance(), &committedValue);
    if(result == ValueCache::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " vote " << vote <<
            " hit committed value " << committedValue;
        MORDOR_ASSERT(vote.valueId() == committedValue.valueId());
    }
    return result;
}

AcceptorState::Status AcceptorState::commit(const Guid& epoch,
                                            InstanceId instanceId,
                                            const Guid& valueId)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateEpoch(epoch);
    if(valueCache_) { // HACK(skywalker)
        switch(tryCommitOnCommitted(instanceId, valueId)) {
            case ValueCache::OK:
                return OK;
                break;
            case ValueCache::TOO_OLD: case ValueCache::WRONG_EPOCH:
                MORDOR_LOG_TRACE(g_log) << this << " iid " << instanceId <<
                    " forgotten/epoch wrong";
                return TOO_OLD;
                break;
            default:
                break;
        }
    }

    AcceptorInstance* instance = lookupInstance(instanceId);
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << "commit(" << instanceId <<
                                   ") refused";
        return REFUSED;
    }

    MORDOR_ASSERT(!instance->committed());

    bool result = instance->commit(valueId);
    MORDOR_LOG_TRACE(g_log) << this << " commit(" << instanceId << ", " <<
                               valueId << ") = " << result;
    if(result) {
        Value value;
        BallotId ballot;
        if(!instance->value(&value, &ballot)) {
            MORDOR_ASSERT(1 == 0);
        }
        commitTracker_->push(epoch_, instanceId, ballot, value);
        pendingInstances_.erase(instanceId);
        g_pendingInstances.decrement();
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " commit(" << instanceId << ")" <<
                                   " failed, scheduling recovery";
        ioManager_->schedule(boost::bind(&AcceptorState::startRecovery,
                                         this,
                                         epoch_,
                                         instanceId));
    }
    return boolToStatus(result);
}

ValueCache::QueryResult AcceptorState::tryCommitOnCommitted(
    InstanceId instanceId,
    const Guid& valueId)
{
    Value committedValue;
    auto result = valueCache_->query(epoch_, instanceId, &committedValue);
    if(result == ValueCache::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " commit(" << instanceId <<
            ", " << valueId << " is a recommit(" <<
            committedValue.valueId() << ")";
        MORDOR_ASSERT(valueId == committedValue.valueId());
    }
    return result;
}

AcceptorInstance* AcceptorState::lookupInstance(InstanceId instanceId) {
    // A precondition for calling this function is that the value cache
    // query returned NOT_YET.
    // Thus we only need to check whether the pending instance span
    // constraint is violated by inserting a new instance.
    auto instanceIter = pendingInstances_.find(instanceId);
    if(instanceIter != pendingInstances_.end()) {
        return &instanceIter->second;
    } else {
        if(canInsert(instanceId)) {
            MORDOR_LOG_TRACE(g_log) << this << " new pending iid=" <<
                instanceId;
            auto freshIter = pendingInstances_.insert(make_pair(instanceId,
                                                      AcceptorInstance()));
            MORDOR_ASSERT(freshIter.second);
            g_pendingInstances.increment();

            return &(freshIter.first->second);
        } else {
            return NULL;
        }
    }
}

InstanceId AcceptorState::firstNotCommittedInstanceId(const Guid& epoch) {
    FiberMutex::ScopedLock lk(mutex_);
    updateEpoch(epoch);
    return commitTracker_->firstNotCommittedInstanceId();
}

bool AcceptorState::canInsert(InstanceId instanceId) const {
    if(pendingInstances_.empty()) {
        return true;
    } else {
        auto maxPendingIter = --(pendingInstances_.end());
        InstanceId maxPendingInstanceId = maxPendingIter->first;
        InstanceId minPendingInstanceId =
            commitTracker_->firstNotCommittedInstanceId();
        if(instanceId >= minPendingInstanceId &&
           instanceId <= maxPendingInstanceId)
        {
            return true;
        } else if(instanceId < minPendingInstanceId) {
            MORDOR_LOG_TRACE(g_log) << this << " cannot insert iid " <<
                instanceId << ", min uncommitted iid is " <<
                minPendingInstanceId;
            return false;
        } else if(instanceId >=
            minPendingInstanceId + pendingInstancesSpan_)
        {
            MORDOR_LOG_TRACE(g_log) << this << " cannot insert iid " <<
                instanceId << ", pending span is [" <<
                minPendingInstanceId << ", " << maxPendingInstanceId <<
                "], limit is " << pendingInstancesSpan_;
            return false;
        } else {
            return true;
        }
    }
}

void AcceptorState::startRecovery(const Guid epoch, InstanceId instanceId) {
    MORDOR_LOG_TRACE(g_log) << this << " submitting (" << epoch << ", " <<
        instanceId << ") to recovery";
    recoveryManager_->addInstance(epoch, instanceId);
}

AcceptorState::Status AcceptorState::boolToStatus(const bool boolean) const
{
    return boolean ? OK : NACKED;
}

void AcceptorState::updateEpoch(const Guid& epoch) {
    if(epoch != epoch_) {
        MORDOR_LOG_INFO(g_log) << this << " epoch change from " << epoch_ <<
                                  " to " << epoch;
        reset();
        commitTracker_->updateEpoch(epoch);
        epoch_ = epoch;
    }
}

void AcceptorState::reset() {
    pendingInstances_.clear();
    g_pendingInstances.reset();
}

}  // namespace lightning
