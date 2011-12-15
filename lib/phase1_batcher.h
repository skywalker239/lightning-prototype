#pragma once

#include "instance_pool.h"
#include "paxos_defs.h"
#include "ring_holder.h"
#include "sync_group_requester.h"
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
    Phase1Batcher(uint32_t batchSize,
                  paxos::BallotId initialBallot,
                  paxos::InstancePool::ptr instancePool,
                  SyncGroupRequester::ptr requester,
                  boost::shared_ptr<Mordor::FiberEvent>
                      pushMoreOpenInstancesEvent);

    void run();

private:
    //! Best case: push the entire [startId, endId) range to
    //  open instances.
    void openInstanceRange(paxos::InstanceId startId,
                           paxos::InstanceId endId);

    //! Process the case when [startId, endId) contains some
    //  reserved instances.
    void processMixedResult(paxos::InstanceId startId,
                            paxos::InstanceId endId,
                            const std::map<paxos::InstanceId,
                                           paxos::BallotId>&
                                  reservedInstances);
    //! Invoked when our batch start was too low and needs to
    //  be fast-forwarded to the least oldest remembered instance
    //  across the acceptors.
    void resetNextInstanceId(paxos::InstanceId newStartId);

    const uint32_t batchSize_;
    const paxos::BallotId initialBallot_;

    paxos::InstancePool::ptr instancePool_;
    SyncGroupRequester::ptr requester_;
    boost::shared_ptr<Mordor::FiberEvent>
        pushMoreOpenInstancesEvent_;
    
    paxos::InstanceId nextStartInstanceId_;
};

}  // namespace lightning
