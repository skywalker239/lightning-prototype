#include "commit_tracker.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using std::make_pair;
using std::map;

static Logger::ptr g_log = Log::lookup("lightning:commit_tracker");

CommitTracker::CommitTracker(const uint64_t recoveryGracePeriodUs,
                             InstanceSink::ptr sink,
                             RecoveryManager::ptr recoveryManager,
                             IOManager* ioManager)
    : recoveryGracePeriodUs_(recoveryGracePeriodUs),
      sink_(sink),
      recoveryManager_(recoveryManager),
      ioManager_(ioManager),
      afterLastCommittedInstanceId_(0)
{}

void CommitTracker::updateEpoch(const Guid& newEpoch) {
    if(newEpoch != epoch_) {
        MORDOR_LOG_DEBUG(g_log) << this << " epoch change " << epoch_ <<
            " -> " << newEpoch;
        sink_->updateEpoch(newEpoch);
        FiberMutex::ScopedLock lk(mutex_);
        epoch_ = newEpoch;
        notCommittedRecoveryTimers_.clear();
        afterLastCommittedInstanceId_ = 0;
    }
}

void CommitTracker::push(const Guid& epoch,
                         InstanceId instanceId,
                         BallotId   ballotId,
                         Value      value)
{
    MORDOR_LOG_TRACE(g_log) << this << " push(" << epoch << ", " <<
        instanceId << ", " <<
        ballotId << ", " << value << ")";
    updateEpoch(epoch);
    sink_->push(instanceId, ballotId, value);
    FiberMutex::ScopedLock lk(mutex_);

    if(instanceId >= afterLastCommittedInstanceId_) {
        for(InstanceId iid = afterLastCommittedInstanceId_;
            iid < instanceId;
            ++iid)
        {
            auto timer = ioManager_->registerTimer(
                             recoveryGracePeriodUs_,
                             boost::bind(
                                &CommitTracker::startRecovery,
                                this,
                                epoch_,
                                iid));
            MORDOR_LOG_TRACE(g_log) << this << " scheduling recovery of (" <<
                epoch_ << ", " << iid << ") in " << recoveryGracePeriodUs_;
            auto iter = notCommittedRecoveryTimers_.insert(
                            make_pair(iid, timer));
            MORDOR_ASSERT(iter.second);
        }
        afterLastCommittedInstanceId_ = instanceId + 1;
    } else {
        auto iter = notCommittedRecoveryTimers_.find(instanceId);
        MORDOR_ASSERT(iter != notCommittedRecoveryTimers_.end());
        if(iter->second) {
            MORDOR_LOG_TRACE(g_log) << this << " canceling recovery of (" <<
                epoch << ", " << instanceId << ")";
            iter->second->cancel();
        }
        notCommittedRecoveryTimers_.erase(iter);
    }
}

bool CommitTracker::needsRecovery(const Guid& epoch,
                                  paxos::InstanceId instance) const
{
    FiberMutex::ScopedLock lk(mutex_);
    if(epoch != epoch_) {
        return false;
    }

    return (instance >= afterLastCommittedInstanceId_) ||
           (notCommittedRecoveryTimers_.find(instance) !=
                notCommittedRecoveryTimers_.end());
}

void CommitTracker::startRecovery(const Guid epoch,
                                  InstanceId instanceId)
{
    FiberMutex::ScopedLock lk(mutex_);
    if(epoch != epoch_) {
        MORDOR_LOG_TRACE(g_log) << this << " stale recovery (" <<
            epoch << ", " << instanceId << "): current epoch is " <<
            epoch_;
        return;
    } else {
        auto iter = notCommittedRecoveryTimers_.find(instanceId);
        MORDOR_ASSERT(iter != notCommittedRecoveryTimers_.end());
        MORDOR_LOG_TRACE(g_log) << this << " submitting (" <<
            epoch << ", " << instanceId << ") to recovery";
        iter->second.reset();
        lk.unlock();
        recoveryManager_->addInstance(epoch, instanceId);
    }
}

InstanceId CommitTracker::firstNotCommittedInstanceId() const {
    FiberMutex::ScopedLock lk(mutex_);
    return (notCommittedRecoveryTimers_.empty()) ?
               afterLastCommittedInstanceId_ :
               notCommittedRecoveryTimers_.begin()->first;
}

}  // namespace lightning
