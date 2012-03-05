#include "acceptor_state.h"
#include "ring_voter.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::CountStatistic;
using Mordor::FiberMutex;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Statistics;
using paxos::AcceptorInstance;
using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using std::make_pair;
using std::map;
using std::set;

static CountStatistic<uint64_t>& g_pendingInstances =
    Statistics::registerStatistic("acceptor.pending_instances",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_notCommittedInstances =
    Statistics::registerStatistic("acceptor.not_committed_instances",
                                  CountStatistic<uint64_t>());

static Logger::ptr g_log = Log::lookup("lightning:acceptor_state");

AcceptorState::AcceptorState(uint32_t pendingInstancesLimit,
                             uint32_t instanceWindowSize)
    : pendingInstancesLimit_(pendingInstancesLimit),
      instanceWindowSize_(instanceWindowSize),
      firstNotForgottenInstanceId_(0),
      pendingInstanceCount_(0),
      afterLastCommittedInstanceId_(0)
{}

AcceptorState::Status AcceptorState::nextBallot(InstanceId instanceId,
                                                BallotId  ballotId,
                                                BallotId* highestPromised,
                                                BallotId* highestVoted,
                                                Value*    lastVote)
{
    FiberMutex::ScopedLock lk(mutex_);
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

AcceptorState::Status AcceptorState::beginBallot(InstanceId instanceId,
                                                 BallotId ballotId,
                                                 const Value& value)
{
    FiberMutex::ScopedLock lk(mutex_);
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
                               value.valueId << ") = " << result;
    return boolToStatus(result);
}

AcceptorState::Status AcceptorState::vote(const Vote& vote,
                                          BallotId* highestPromised)
{
    FiberMutex::ScopedLock lk(mutex_);
    AcceptorInstance* instance = lookupInstance(vote.instance());
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << " " << vote << " refused";
        return REFUSED;
    }
    bool result = instance->vote(vote, highestPromised);
    MORDOR_LOG_TRACE(g_log) << this << " " << vote << " = " << result;
    return boolToStatus(result);
}

AcceptorState::Status AcceptorState::commit(InstanceId instanceId,
                                            const Guid& valueId)
{
    FiberMutex::ScopedLock lk(mutex_);
    AcceptorInstance* instance = lookupInstance(instanceId);
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << "commit(" << instanceId <<
                                   ") refused";
        return REFUSED;
    }


    bool result = instance->commit(valueId);
    MORDOR_LOG_TRACE(g_log) << this << " commit(" << instanceId << ", " <<
                               valueId << ") = " << result;
    if(result) {
        addCommittedInstanceId(instanceId);
    }
    return boolToStatus(result);
}

AcceptorState::Status AcceptorState::value(
    InstanceId instanceId,
    Value* value) const
{
    FiberMutex::ScopedLock lk(mutex_);

    auto instanceIter = instances_.find(instanceId);
    if(instanceIter == instances_.end()) {
        MORDOR_LOG_TRACE(g_log) << this << " value(" << instanceId <<
                                   ") not found";
        return boolToStatus(false);
    } else {
        return boolToStatus(instanceIter->second.value(value));
    }
}

InstanceId AcceptorState::firstNotForgottenInstance() const {
    FiberMutex::ScopedLock lk(mutex_);

    return firstNotForgottenInstanceId_;
}

InstanceId AcceptorState::firstNotCommittedInstance() const {
    FiberMutex::ScopedLock lk(mutex_);

    return firstNotCommittedInstanceInternal();
}

InstanceId AcceptorState::firstNotCommittedInstanceInternal() const {
    return (notCommittedInstanceIds_.empty()) ?
                afterLastCommittedInstanceId_ :
                *notCommittedInstanceIds_.begin();
}

bool AcceptorState::tryForgetInstances(InstanceId upperLimit) {
    InstanceId firstNotCommittedId = firstNotCommittedInstanceInternal();
    if(firstNotCommittedId < upperLimit) {
        MORDOR_LOG_TRACE(g_log) << this << " forget(" << upperLimit <<
                                   " failed: iid " << firstNotCommittedId <<
                                   " not committed";
        return false;
    }

    while(!instances_.empty() && instances_.begin()->first < upperLimit) {
        MORDOR_LOG_TRACE(g_log) << this << " forgetting iid=" <<
                                   instances_.begin()->first;
        instances_.erase(instances_.begin());
    }
    firstNotForgottenInstanceId_ = upperLimit;
    return true;
}

AcceptorInstance* AcceptorState::lookupInstance(InstanceId instanceId) {
    if(instanceId < firstNotForgottenInstanceId_) {
        MORDOR_LOG_WARNING(g_log) << this << " lookup forgotten iid=" <<
                                     instanceId;
        return NULL;
    }

    auto instanceIter = instances_.find(instanceId);
    if(instanceIter != instances_.end()) {
        return &instanceIter->second;
    } else {
        if(pendingInstanceCount_ >= pendingInstancesLimit_) {
            return NULL;
        }
        if(instanceId - firstNotForgottenInstanceId_ > instanceWindowSize_) {
            const InstanceId newWindowStart = instanceId - instanceWindowSize_;
            if(!tryForgetInstances(newWindowStart)) {
                return NULL;
            }
        }

        MORDOR_LOG_TRACE(g_log) << this << " new pending iid=" <<
                                       instanceId;
        auto freshIter = instances_.insert(make_pair(instanceId,
                                           AcceptorInstance())).first;
        ++pendingInstanceCount_;
        g_pendingInstances.increment();
        return &freshIter->second;
    }
}
                                                    
void AcceptorState::addCommittedInstanceId(InstanceId instanceId) {
    bool freshCommit = false;
    if(instanceId >= afterLastCommittedInstanceId_) {
        for(InstanceId iid = afterLastCommittedInstanceId_;
            iid < instanceId;
            ++iid)
        {
            notCommittedInstanceIds_.insert(iid);
            g_notCommittedInstances.increment();
            MORDOR_ASSERT(g_notCommittedInstances.count ==
                          notCommittedInstanceIds_.size())
        }
        afterLastCommittedInstanceId_ = instanceId + 1;
        freshCommit = true;
    } else {
        freshCommit = (notCommittedInstanceIds_.erase(instanceId) == 1);
        if(freshCommit) {
            g_notCommittedInstances.decrement();
            MORDOR_ASSERT(g_notCommittedInstances.count ==
                          notCommittedInstanceIds_.size());
        }
    }
    if(freshCommit) {
        --pendingInstanceCount_;
        g_pendingInstances.decrement();
    }
}

AcceptorState::Status AcceptorState::boolToStatus(const bool boolean) const
{
    return boolean ? OK : NACKED;
}

void AcceptorState::updateEpoch(const Guid& epoch) {
    FiberMutex::ScopedLock lk(mutex_);
    if(epoch != epoch_) {
        MORDOR_LOG_TRACE(g_log) << this << " epoch change from " << epoch_ <<
                                   " to " << epoch;
        reset();
        epoch_ = epoch;
    }
}

void AcceptorState::reset() {
    instances_.clear();
    firstNotForgottenInstanceId_ = 0;
    pendingInstanceCount_ = 0;
    g_pendingInstances.reset();
    g_notCommittedInstances.reset();
    notCommittedInstanceIds_.clear();
    afterLastCommittedInstanceId_ = 0;
}

}  // namespace lightning
