#include "commit_tracker.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::CountStatistic;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using Mordor::MaxStatistic;
using Mordor::Statistics;
using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using std::make_pair;
using std::map;

static Logger::ptr g_log = Log::lookup("lightning:commit_tracker");

CountStatistic<uint64_t>& g_instancesScheduledForRecovery =
    Statistics::registerStatistic("commit_tracker.instances_scheduled_for_recovery",
                                  CountStatistic<uint64_t>());
MaxStatistic<uint64_t>& g_minPendingInstance =
    Statistics::registerStatistic("commit_tracker.min_pending_instance",
                                  MaxStatistic<uint64_t>());
MaxStatistic<uint64_t>& g_maxCommittedInstance =
    Statistics::registerStatistic("commit_tracker.max_committed_instance",
                                  MaxStatistic<uint64_t>());
CountStatistic<uint64_t>& g_committedBytes =
    Statistics::registerStatistic("commit_tracker.committed_bytes",
                                  CountStatistic<uint64_t>());

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

void CommitTracker::updateEpoch(const Guid& epoch) {
    FiberMutex::ScopedLock lk(mutex_);
    updateEpochInternal(epoch);
}

void CommitTracker::updateEpochInternal(const Guid& newEpoch) {
    if(newEpoch != epoch_) {
        MORDOR_LOG_INFO(g_log) << this << " epoch change " << epoch_ <<
            " -> " << newEpoch;
        sink_->updateEpoch(newEpoch);
        epoch_ = newEpoch;
        notCommittedRecoveryTimers_.clear();
        afterLastCommittedInstanceId_ = 0;
        g_instancesScheduledForRecovery.reset();
        g_minPendingInstance.reset();
        g_maxCommittedInstance.reset();
        g_committedBytes.reset();
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
    FiberMutex::ScopedLock lk(mutex_);
    updateEpochInternal(epoch);
    if(!needsRecoveryInternal(epoch, instanceId)) {
        MORDOR_LOG_DEBUG(g_log) << this << " spurious recommit(" <<
            epoch << ", " << instanceId << ")";
        return;
    }

    g_maxCommittedInstance.update(instanceId);
    g_committedBytes.add(value.size());
    g_minPendingInstance.update(firstNotCommittedInstanceIdInternal());


    sink_->push(instanceId, ballotId, value);

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
            g_instancesScheduledForRecovery.increment();
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
        g_instancesScheduledForRecovery.decrement();
    }
}

bool CommitTracker::needsRecovery(
    const Guid& epoch,
    paxos::InstanceId instance) const
{
    FiberMutex::ScopedLock lk(mutex_);
    return needsRecoveryInternal(epoch, instance);
}

bool CommitTracker::needsRecoveryInternal(
    const Guid& epoch,
    paxos::InstanceId instance) const
{
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
    return firstNotCommittedInstanceIdInternal();
}

InstanceId CommitTracker::firstNotCommittedInstanceIdInternal() const {
    return (notCommittedRecoveryTimers_.empty()) ?
               afterLastCommittedInstanceId_ :
               notCommittedRecoveryTimers_.begin()->first;
}

}  // namespace lightning
