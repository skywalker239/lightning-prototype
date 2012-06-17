#pragma once

#include "instance_sink.h"
#include "recovery_manager.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>
#include <set>

namespace lightning {

class CommitTracker {
public:
    typedef boost::shared_ptr<CommitTracker> ptr;

    CommitTracker(const uint64_t recoveryGracePeriodUs,
                  InstanceSink::ptr sink,
                  RecoveryManager::ptr recoveryManager,
                  Mordor::IOManager* ioManager);

    void push(const Guid& epoch,
              paxos::InstanceId instanceId,
              paxos::BallotId   ballotId,
              paxos::Value      value);

    bool needsRecovery(const Guid& epoch,
                       paxos::InstanceId instance) const;

    paxos::InstanceId firstNotCommittedInstanceId() const;

    void updateEpoch(const Guid& epoch);

private:
    paxos::InstanceId firstNotCommittedInstanceIdInternal() const;

    bool needsRecoveryInternal(const Guid& epoch,
                               paxos::InstanceId instance) const;

    void updateEpochInternal(const Guid& epoch);

    const uint64_t recoveryGracePeriodUs_;
    InstanceSink::ptr sink_;
    RecoveryManager::ptr recoveryManager_;
    Mordor::IOManager* ioManager_;

    Guid epoch_;
    std::map<paxos::InstanceId, Mordor::Timer::ptr>
        notCommittedRecoveryTimers_;
    paxos::InstanceId afterLastCommittedInstanceId_;

    void startRecovery(const Guid epoch,
                       paxos::InstanceId instanceId);

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
