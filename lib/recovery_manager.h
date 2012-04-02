#pragma once

#include "blocking_queue.h"
#include "guid.h"
#include "host_configuration.h"
#include "paxos_defs.h"
#include "rpc_requester.h"
#include <mordor/iomanager.h>
#include <boost/enable_shared_from_this.hpp>
#include <vector>

namespace lightning {

class AcceptorState;

class RecoveryManager : public boost::enable_shared_from_this<RecoveryManager>
{
public:
    typedef boost::shared_ptr<RecoveryManager> ptr;

    RecoveryManager(GroupConfiguration::ptr groupConfiguration,
                    RpcRequester::ptr requester,
                    Mordor::IOManager* ioManager,
                    uint64_t recoveryIntervalUs,
                    uint64_t recoveryTimeoutUs);

    //! Main task, processes the recovery queue.
    void recoverInstances();

    //! Adds an instance to the recovery queue.
    void addInstance(const Guid& epoch,
                     paxos::InstanceId instanceId,
                     boost::shared_ptr<AcceptorState> acceptor);
private:
    struct RecoveryRecord {
        const Guid epoch;
        const paxos::InstanceId instanceId;
        const boost::shared_ptr<AcceptorState> acceptor;
        const uint32_t recoveryHostIndex;
        RecoveryRecord(const Guid& _epoch,
                       paxos::InstanceId _instanceId,
                       boost::shared_ptr<AcceptorState> _acceptor,
                       uint32_t _recoveryHostIndex)
            : epoch(_epoch),
              instanceId(_instanceId),
              acceptor(_acceptor),
              recoveryHostIndex(_recoveryHostIndex)
        {}
    };

    //! Does the actual work, scheduled in its own fiber.
    void doRecovery(const RecoveryRecord recoveryRecord);

    GroupConfiguration::ptr groupConfiguration_;
    RpcRequester::ptr requester_;
    Mordor::IOManager* ioManager_;
    const uint64_t recoveryIntervalUs_;
    const uint64_t recoveryTimeoutUs_;

    //! Recovery is attempted in a round-robin fashion starting with
    //  acceptors in our datacenter (if possible).
    std::vector<uint32_t> recoveryHostIds_;

    BlockingQueue<RecoveryRecord> recoveryQueue_;
};

}  // namespace lightning
