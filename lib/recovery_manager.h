#pragma once

#include "blocking_queue.h"
#include "guid.h"
#include "host_configuration.h"
#include "paxos_defs.h"
#include "recovery_connection.h"
#include "recovery_record.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <boost/enable_shared_from_this.hpp>
#include <set>
#include <vector>

namespace lightning {

class CommitTracker;

class RecoveryManager : public boost::enable_shared_from_this<RecoveryManager>
{
public:
    typedef boost::shared_ptr<RecoveryManager> ptr;

    RecoveryManager();

    void enableConnection(RecoveryConnection::ptr connection);

    void disableConnection(RecoveryConnection::ptr connection);

    //! Main task, processes the main queue.
    void processMainQueue();
    //! Processes the random destination retry queue.
    void processRandomDestinationQueue();

    //! Adds an instance to the recovery queue.
    void addInstance(const Guid& epoch,
                     paxos::InstanceId instanceId,
                     bool tryBestConnection = true);

    //! Creates the connections to other acceptors with suitable metrics.
    void setupConnections(GroupConfiguration::ptr groupConfiguration,
                          Mordor::IOManager* ioManager,
                          uint32_t localMetric,
                          uint32_t remoteMetric,
                          uint64_t connectionPollIntervalUs,
                          uint64_t reconnectDelayUs,
                          uint64_t socketTimeoutUs,
                          uint64_t instanceRetryDelayUs);

    //! Sets the recovery destination
    void setCommitTracker(boost::shared_ptr<CommitTracker> commitTracker);

    //! Stores the recovered value in the acceptor state.
    void addRecoveredValue(const Guid& epoch,
                           paxos::InstanceId instanceId,
                           const paxos::Value& value);
private:
    RecoveryConnection::ptr getBestConnection();
    RecoveryConnection::ptr getRandomConnection();

    //! Compares connections by their metric.
    struct ConnectionCompare {
        bool operator()(const RecoveryConnection::ptr& a,
                        const RecoveryConnection::ptr& b)
        {
            return a->metric() < b->metric();
        }
    };

    std::set<RecoveryConnection::ptr,
             ConnectionCompare> connections_;
    std::vector<RecoveryConnection::ptr>
        connectionVector_;
    unsigned int randSeed_;

    Mordor::FiberEvent hasActiveConnection_;

    boost::shared_ptr<CommitTracker> commitTracker_;

    BlockingQueue<RecoveryRecord::ptr> recoveryQueue_;
    BlockingQueue<RecoveryRecord::ptr> randomDestinationQueue_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
