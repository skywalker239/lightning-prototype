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

class AcceptorState;

class RecoveryManager : public boost::enable_shared_from_this<RecoveryManager>
{
public:
    typedef boost::shared_ptr<RecoveryManager> ptr;

    RecoveryManager();

    void enableConnection(RecoveryConnection::ptr connection);

    void disableConnection(RecoveryConnection::ptr connection);

    //! Main task, processes the recovery queue.
    void run();

    //! Adds an instance to the recovery queue.
    void addInstance(const Guid& epoch,
                     paxos::InstanceId instanceId);

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
    void setAcceptor(boost::shared_ptr<AcceptorState> acceptor);

    //! Stores the recovered value in the acceptor state.
    void addRecoveredValue(const Guid& epoch,
                           paxos::InstanceId instanceId,
                           const paxos::Value& value,
                           paxos::BallotId ballot);
private:
    RecoveryConnection::ptr getActiveConnection();

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
    Mordor::FiberEvent hasActiveConnection_;

    boost::shared_ptr<AcceptorState> acceptor_;

    BlockingQueue<RecoveryRecord::ptr> recoveryQueue_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
