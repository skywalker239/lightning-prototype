#include "recovery_manager.h"
#include "recovery_connection.h"
#include "acceptor_state.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/sleep.h>
#include <algorithm>
#include <string>

namespace lightning {

using paxos::BallotId;
using paxos::InstanceId;
using paxos::Value;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:recovery_manager");

RecoveryManager::RecoveryManager()
    : hasActiveConnection_(false),
      recoveryQueue_("recovery_manager_queue")
{}

void RecoveryManager::enableConnection(RecoveryConnection::ptr connection) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << this << " enabling connection " <<
        connection->name();
    connections_.insert(connection);
    hasActiveConnection_.set();
}

void RecoveryManager::disableConnection(RecoveryConnection::ptr connection) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << this << " disabling connection " <<
        connection->name();
    if(connections_.erase(connection) != 1) {
        MORDOR_LOG_DEBUG(g_log) << this << " connection " <<
            connection->name() << " not found";
    }
    if(connections_.empty()) {
        hasActiveConnection_.reset();
    }
}

RecoveryConnection::ptr RecoveryManager::getActiveConnection() {
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

void RecoveryManager::run() {
    while(true) {
        RecoveryRecord::ptr recoveryRecord = recoveryQueue_.pop();
        bool submitted = false;
        while(!submitted) {
            RecoveryConnection::ptr connection = getActiveConnection();
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

void RecoveryManager::addInstance(const Guid& epoch, InstanceId instanceId) {
    MORDOR_ASSERT(!!acceptor_);
    if(!acceptor_->needsRecovery(epoch, instanceId)) {
        MORDOR_LOG_TRACE(g_log) << this <<
            " addInstance: acceptor not interested in (" << epoch << ", " <<
            instanceId << ")";
        return;
    }

    recoveryQueue_.push(
        RecoveryRecord::ptr(
            new RecoveryRecord(epoch, instanceId)));
    MORDOR_LOG_TRACE(g_log) << this << " enqueued (" << epoch << ", " <<
        instanceId << ")";
}

void RecoveryManager::addRecoveredValue(const Guid& epoch,
                                        InstanceId instanceId,
                                        const Value& value,
                                        BallotId ballot)
{
    MORDOR_ASSERT(!!acceptor_);
    MORDOR_LOG_TRACE(g_log) << this << " addRecoveredValue(" <<
        epoch << ", " << instanceId << ", " << value << ", " << ballot << ")";
    acceptor_->setInstance(epoch, instanceId, value, ballot);
}

void RecoveryManager::setAcceptor(AcceptorState::ptr acceptor) {
    FiberMutex::ScopedLock lk(mutex_);
    acceptor_ = acceptor;
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
                        metric,
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
