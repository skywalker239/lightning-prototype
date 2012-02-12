#include "acceptor_state.h"
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
        addCommittedInstanceId(instanceId);
    }
    return boolToStatus(result);
}

AcceptorState::Status AcceptorState::value(InstanceId instanceId, Value* value) const {
    FiberMutex::ScopedLock lk(mutex_);

    auto instanceIter = committedInstances_.find(instanceId);
    if(instanceIter == committedInstances_.end()) {
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
    auto committedIter = committedInstances_.find(instanceId);
    if(committedIter != committedInstances_.end()) {
        return &committedIter->second;
    }
    auto pendingIter = pendingInstances_.find(instanceId);
    if(pendingIter != pendingInstances_.end()) {
        return &pendingIter->second;
    } else {
        if(pendingInstances_.size() < pendingInstancesLimit_) {
            pendingIter = pendingInstances_.insert(make_pair(instanceId,
                                                   AcceptorInstance())).first;
            return &pendingIter->second;
        } else {
            return NULL;
        }
    }
}

void AcceptorState::addCommittedInstanceId(InstanceId instanceId) {
    if(instanceId >= afterLastCommittedInstanceId_) {
        for(InstanceId iid = afterLastCommittedInstanceId_;
            iid < instanceId;
            ++iid)
        {
            notCommittedInstanceIds_.insert(iid);
        }
        afterLastCommittedInstanceId_ = instanceId + 1;
    } else {
        notCommittedInstanceIds_.erase(instanceId);
    }
}

AcceptorState::Status AcceptorState::boolToStatus(const bool boolean) const
{
    return boolean ? OK : NACKED;
}

void AcceptorState::reset() {
    FiberMutex::ScopedLock lk(mutex_);
    pendingInstances_.clear();
    committedInstances_.clear();
    notCommittedInstanceIds_.clear();
    afterLastCommittedInstanceId_ = 0;
}

}  // namespace lightning
