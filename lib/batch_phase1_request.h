#pragma once

#include "paxos_defs.h"
#include "ring_configuration.h"
#include "sync_group_request.h"
#include <mordor/fibersynchronization.h>
#include <map>
#include <set>

namespace lightning {

class BatchPhase1Request : public SyncGroupRequest {
public:
    typedef boost::shared_ptr<BatchPhase1Request> ptr;

    BatchPhase1Request(RingConfiguration::const_ptr ringConfiguration,
                       paxos::BallotId ballotId,
                       paxos::InstanceId instanceRangeBegin,
                       paxos::InstanceId instanceRangeEnd);

    enum Result {
        PENDING,
        ALL_OPEN,
        START_IID_TOO_LOW,
        NOT_ALL_OPEN
    };

    Result result() const;

    Status status() const;

    const std::map<paxos::InstanceId, paxos::BallotId>&
        reservedInstances() const;

    paxos::InstanceId retryStartInstanceId() const;
private:
    const std::string requestString() const;

    void onReply(Mordor::Address::ptr source,
                 const std::string& reply);

    void onTimeout();

    void wait();

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                        const Mordor::Address::ptr& rhs) const
        {
            return *lhs < *rhs;
        }
    }; 

    RingConfiguration::const_ptr ringConfiguration_;
    const paxos::BallotId ballotId_;
    const paxos::InstanceId instanceRangeBegin_;
    const paxos::InstanceId instanceRangeEnd_;
    std::set<Mordor::Address::ptr, AddressCompare> notAcked_;
    Status status_;
    Result result_;

    std::map<paxos::InstanceId, paxos::BallotId> reservedInstances_;
    paxos::InstanceId retryStartInstanceId_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

} // namespace lightning
