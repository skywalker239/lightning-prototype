#pragma once

#include "guid.h"
#include "host_configuration.h"
#include "instance_pool.h"
#include "paxos_defs.h"
#include "ring_holder.h"
#include "multicast_rpc_requester.h"
#include <mordor/fibersynchronization.h>

namespace lightning {

//! Executes Phase 1 of Paxos in batches of given size
//  and pushes new protocol instances into an InstancePool.
class Phase1Batcher : public RingHolder {
public:
    //! Since all Paxos instances are logically distinct
    //  we only need one initial ballot id with which to
    //  attempt to reserve them. If we fail to reserve
    //  some instance in batch phase 1, it will go
    //  through a complete phase 1 elsewhere.
    Phase1Batcher(const GroupConfiguration& groupConfiguration,
                  const Guid& epoch,
                  uint64_t timeoutUs,
                  uint32_t batchSize,
                  paxos::BallotId initialBallot,
                  paxos::InstancePool::ptr instancePool,
                  MulticastRpcRequester::ptr requester,
                  boost::shared_ptr<Mordor::FiberEvent>
                      pushMoreOpenInstancesEvent);

    void run();

private:
    void openInstances(paxos::InstanceId startInstance,
                       paxos::InstanceId endInstance,
                       const std::set<paxos::InstanceId>& reservedInstances);
    //! Invoked when our batch start was too low and needs to
    //  be fast-forwarded to the least oldest remembered instance
    //  across the acceptors.
    void resetNextInstanceId(paxos::InstanceId newStartId);

    void generateRingAddresses(std::vector<Mordor::Address::ptr>* hosts,
                               uint32_t* ringId) const;

    const GroupConfiguration groupConfiguration_;
    const Guid epoch_;
    const uint64_t timeoutUs_;
    const uint32_t batchSize_;
    const paxos::BallotId initialBallot_;

    paxos::InstancePool::ptr instancePool_;
    MulticastRpcRequester::ptr requester_;
    boost::shared_ptr<Mordor::FiberEvent>
        pushMoreOpenInstancesEvent_;
    
    paxos::InstanceId nextStartInstanceId_;
};

}  // namespace lightning
