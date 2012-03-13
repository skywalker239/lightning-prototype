#pragma once

#include "ballot_generator.h"
#include "batch_phase1_request.h"
#include "guid.h"
#include "host_configuration.h"
#include "instance_pool.h"
#include "paxos_defs.h"
#include "ring_holder.h"
#include "rpc_requester.h"
#include <mordor/fibersynchronization.h>

namespace lightning {

//! Executes Phase 1 of Paxos in batches of given size
//  and pushes new protocol instances into an InstancePool.
class Phase1Batcher : public RingHolder {
public:
    typedef boost::shared_ptr<Phase1Batcher> ptr;

    Phase1Batcher(const Guid& epoch,
                  uint64_t timeoutUs,
                  uint32_t batchSize,
                  const paxos::BallotGenerator& ballotGenerator,
                  paxos::InstancePool::ptr instancePool,
                  RpcRequester::ptr requester,
                  boost::shared_ptr<Mordor::FiberEvent>
                      pushMoreOpenInstancesEvent);

    void run();

private:
    //! This requests to perform batch phase 1 on [startInstance, endInstance)
    //  and loops while the request times out, boosting the ballot id
    //  on every iteration.
    //  On success, request is reset to the completed request instance and
    //  successfulBallot is set to the ballot id with which we succeeded.
    //  If the request result is IID_TOO_LOW, successfulBallot is set to
    //  kInvalidBallotId.
    void requestInstanceRange(paxos::InstanceId startInstance,
                              paxos::InstanceId endInstance,
                              RingConfiguration::const_ptr ring,
                              BatchPhase1Request::ptr* request,
                              paxos::BallotId *successfulBallot);


    void openInstances(paxos::InstanceId startInstance,
                       paxos::InstanceId endInstance,
                       paxos::BallotId   ballotId,
                       const std::set<paxos::InstanceId>& reservedInstances);
    //! Invoked when our batch start was too low and needs to
    //  be fast-forwarded to the least oldest remembered instance
    //  across the acceptors.
    void resetNextInstanceId(paxos::InstanceId newStartId);


    const Guid epoch_;
    const uint64_t timeoutUs_;
    const uint32_t batchSize_;
    const paxos::BallotGenerator ballotGenerator_;

    paxos::InstancePool::ptr instancePool_;
    RpcRequester::ptr requester_;
    boost::shared_ptr<Mordor::FiberEvent>
        pushMoreOpenInstancesEvent_;
    
    paxos::InstanceId nextStartInstanceId_;
};

}  // namespace lightning
