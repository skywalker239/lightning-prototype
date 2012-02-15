#include "acceptor_state.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::FiberMutex;
using Mordor::Logger;
using Mordor::Log;
using paxos::AcceptorInstance;
using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using std::make_pair;
using std::map;
using std::set;

static Logger::ptr g_log = Log::lookup("lightning:acceptor_state");

AcceptorState::AcceptorState(uint32_t pendingInstancesLimit,
                             uint32_t committedInstancesLimit)
    : pendingInstancesLimit_(pendingInstancesLimit),
      committedInstancesLimit_(committedInstancesLimit),
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

AcceptorState::Status AcceptorState::vote(InstanceId instanceId,
                                          BallotId ballotId,
                                          const Guid& valueId,
                                          BallotId* highestPromised)
{
    FiberMutex::ScopedLock lk(mutex_);
    AcceptorInstance* instance = lookupInstance(instanceId);
    if(!instance) {
        MORDOR_LOG_TRACE(g_log) << this << "vote(" << instanceId <<
                                   ") refused";
        return REFUSED;
    }
    bool result = instance->vote(ballotId,
                                 valueId,
                                 highestPromised);
    MORDOR_LOG_TRACE(g_log) << this << " vote(" << instanceId << ", " <<
                               ballotId << ", " << valueId << ") = " <<
                               result;
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
        if(addCommittedInstanceId(instanceId)) {
            --pendingInstanceCount_;
        }
        if(committedInstanceIds_.size() > committedInstancesLimit_) {
            evictLowestCommittedInstance();
        }
    }
    return boolToStatus(result);
}

AcceptorState::Status AcceptorState::value(InstanceId instanceId, Value* value) const {
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

InstanceId AcceptorState::lowestInstanceId() const {
    FiberMutex::ScopedLock lk(mutex_);
    return notCommittedInstanceIds_.empty() ?
               afterLastCommittedInstanceId_ :
               *notCommittedInstanceIds_.begin();
}

AcceptorInstance* AcceptorState::lookupInstance(InstanceId instanceId) {
    auto instanceIter = instances_.find(instanceId);
    if(instanceIter != instances_.end()) {
        return &instanceIter->second;
    } else {
        if(pendingInstanceCount_ < pendingInstancesLimit_) {
            MORDOR_LOG_TRACE(g_log) << this << " new pending iid=" <<
                                       instanceId;
            auto freshIter = instances_.insert(make_pair(instanceId,
                                              AcceptorInstance())).first;
            ++pendingInstanceCount_;
            return &freshIter->second;
        } else {
            return NULL;
        }
    }
}
                                                    
bool AcceptorState::addCommittedInstanceId(InstanceId instanceId) {
    bool freshCommit = false;
    if(instanceId >= afterLastCommittedInstanceId_) {
        for(InstanceId iid = afterLastCommittedInstanceId_;
            iid < instanceId;
            ++iid)
        {
            notCommittedInstanceIds_.insert(iid);
        }
        afterLastCommittedInstanceId_ = instanceId + 1;
        freshCommit = true;
    } else {
        freshCommit = (notCommittedInstanceIds_.erase(instanceId) == 1);
    }
    if(freshCommit) {
        committedInstanceIds_.push(instanceId);
    }
    return freshCommit;
}

AcceptorState::Status AcceptorState::boolToStatus(const bool boolean) const
{
    return boolean ? OK : NACKED;
}

void AcceptorState::evictLowestCommittedInstance() {
    MORDOR_ASSERT(!committedInstanceIds_.empty());
    InstanceId lowestCommittedInstanceId = committedInstanceIds_.top();
    committedInstanceIds_.pop();
    MORDOR_LOG_TRACE(g_log) << this << " evicting committed instance iid=" <<
                               lowestCommittedInstanceId;
    auto instanceIter = instances_.find(lowestCommittedInstanceId);
    MORDOR_ASSERT(instanceIter != instances_.end());
    instances_.erase(instanceIter);
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
    FiberMutex::ScopedLock lk(mutex_);
    instances_.clear();
    committedInstanceIds_ = InstanceIdHeap();
    pendingInstanceCount_ = 0;
    notCommittedInstanceIds_.clear();
    afterLastCommittedInstanceId_ = 0;
}

}  // namespace lightning
