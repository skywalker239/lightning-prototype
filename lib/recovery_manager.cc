#include "recovery_manager.h"
#include "recovery_connection.h"
#include "commit_tracker.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/sleep.h>
#include <algorithm>
#include <string>
#include <stdlib.h>

namespace lightning {

using paxos::kInvalidBallotId;
using paxos::InstanceId;
using paxos::Value;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using std::find;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:recovery_manager");

RecoveryManager::RecoveryManager()
    : randSeed_(239),
      hasActiveConnection_(false),
      recoveryQueue_("recovery_manager_queue"),
      randomDestinationQueue_("recovery_manager_random_dst_queue")
{}

void RecoveryManager::enableConnection(RecoveryConnection::ptr connection) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_DEBUG(g_log) << this << " enabling connection " <<
        connection->name();
    connections_.insert(connection);
    connectionVector_.push_back(connection);
    hasActiveConnection_.set();
    MORDOR_LOG_DEBUG(g_log) << this << " set " << connections_.size() << ", " << " vector " << connectionVector_.size();
    MORDOR_ASSERT(connections_.size() == connectionVector_.size());
}

void RecoveryManager::disableConnection(RecoveryConnection::ptr connection) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_DEBUG(g_log) << this << " disabling connection " <<
        connection->name();
    if(connections_.erase(connection) != 1) {
        MORDOR_LOG_DEBUG(g_log) << this << " connection " <<
            connection->name() << " not found";
    }

    auto iter = find(connectionVector_.begin(),
                     connectionVector_.end(),
                     connection);
    if(iter != connectionVector_.end()) {
        connectionVector_.erase(iter);
    }

    if(connections_.empty()) {
        hasActiveConnection_.reset();
    }
    MORDOR_ASSERT(connections_.size() == connectionVector_.size());
}

RecoveryConnection::ptr RecoveryManager::getBestConnection() {
    while(true) {
        hasActiveConnection_.wait();
        FiberMutex::ScopedLock lk(mutex_);
        if(connections_.empty()) {
            continue;
        } else {
            return *connections_.begin();
        }
    }
}

RecoveryConnection::ptr RecoveryManager::getRandomConnection() {
    while(true) {
        hasActiveConnection_.wait();
        FiberMutex::ScopedLock lk(mutex_);
        if(connectionVector_.empty()) {
            continue;
        } else {
            auto index = rand_r(&randSeed_) % connectionVector_.size();
            return connectionVector_[index];
        }
    }
}

void RecoveryManager::processMainQueue() {
    while(true) {
        RecoveryRecord::ptr recoveryRecord = recoveryQueue_.pop();
        bool submitted = false;
        while(!submitted) {
            RecoveryConnection::ptr connection = getBestConnection();
            MORDOR_LOG_TRACE(g_log) << this << " active connection is " <<
                connection->name() << " with metric " << connection->metric();
            submitted = connection->addInstance(recoveryRecord);
            MORDOR_LOG_TRACE(g_log) << this << " submit(" << *recoveryRecord <<
                ") to " << connection->name() << " = " << submitted;
            if(!submitted) {
                disableConnection(connection);
            } else {
                MORDOR_LOG_TRACE(g_log) << this << " submitted " <<
                    *recoveryRecord << " to " << connection->name();
            }
        }
    }
}

void RecoveryManager::processRandomDestinationQueue() {
    while(true) {
        RecoveryRecord::ptr recoveryRecord = randomDestinationQueue_.pop();
        bool submitted = false;
        while(!submitted) {
            RecoveryConnection::ptr connection = getRandomConnection();
            MORDOR_LOG_TRACE(g_log) << this << " got random connection " <<
                connection->name() << " with metric " << connection->metric();
            submitted = connection->addInstance(recoveryRecord);
            MORDOR_LOG_TRACE(g_log) << this << " submit(" << *recoveryRecord <<
                ") to " << connection->name() << " = " << submitted;
            if(!submitted) {
                disableConnection(connection);
            } else {
                MORDOR_LOG_TRACE(g_log) << this << " submitted " <<
                    *recoveryRecord << " to " << connection->name();
            }
        }
    }
}

void RecoveryManager::addInstance(const Guid& epoch,
                                  InstanceId instanceId,
                                  bool tryBestConnection)
{
    MORDOR_ASSERT(commitTracker_);
    if(!commitTracker_->needsRecovery(epoch, instanceId)) {
        MORDOR_LOG_TRACE(g_log) << this <<
            " addInstance: tracker not interested in (" << epoch << ", " <<
            instanceId << ")";
        return;
    }

    if(tryBestConnection) {
        recoveryQueue_.push(
            RecoveryRecord::ptr(
                new RecoveryRecord(epoch, instanceId)));
    } else {
        randomDestinationQueue_.push(
            RecoveryRecord::ptr(
                new RecoveryRecord(epoch, instanceId)));
    }
    MORDOR_LOG_TRACE(g_log) << this << " enqueued (" << epoch << ", " <<
        instanceId << ")";
}

void RecoveryManager::addRecoveredValue(const Guid& epoch,
                                        InstanceId instanceId,
                                        const Value& value)
{
    MORDOR_ASSERT(commitTracker_);
    MORDOR_LOG_TRACE(g_log) << this << " addRecoveredValue(" <<
        epoch << ", " << instanceId << ", " << value << ")";
    commitTracker_->push(epoch, instanceId, kInvalidBallotId, value);
}

void RecoveryManager::setCommitTracker(CommitTracker::ptr commitTracker) {
    FiberMutex::ScopedLock lk(mutex_);
    commitTracker_ = commitTracker;
}

void RecoveryManager::setupConnections(
    GroupConfiguration::ptr groupConfiguration,
    IOManager* ioManager,
    uint32_t localMetric,
    uint32_t remoteMetric,
    uint64_t connectionPollIntervalUs,
    uint64_t reconnectDelayUs,
    uint64_t socketTimeoutUs,
    uint64_t instanceRetryIntervalUs)
{
    const uint32_t thisHostId = groupConfiguration->thisHostId();
    const string& datacenter = groupConfiguration->datacenter();
    for(size_t i = 0; i < groupConfiguration->size(); ++i) {
        if(i != thisHostId &&
           i != groupConfiguration->masterId())
        {
            bool isLocal =
                groupConfiguration->host(i).datacenter == datacenter;
            uint32_t metric = isLocal ? localMetric : remoteMetric;
            if(isLocal || !groupConfiguration->isLearner()) {
                RecoveryConnection::ptr connection(
                    new RecoveryConnection(
                        groupConfiguration->host(i).name,
                        metric + i, // TODO(skywalker): be more clever
                        connectionPollIntervalUs,
                        reconnectDelayUs,
                        socketTimeoutUs,
                        instanceRetryIntervalUs,
                        groupConfiguration->host(i).unicastAddress,
                        shared_from_this(),
                        ioManager));
                ioManager->schedule(
                    boost::bind(&RecoveryConnection::run,
                                connection));
            }
        }
    }
}

}  // namespace lightning
