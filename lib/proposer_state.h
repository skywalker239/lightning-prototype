#pragma once

#include "ballot_generator.h"
#include "guid.h"
#include "host_configuration.h"
#include "instance_pool.h"
#include "rpc_requester.h"
#include "client_value_queue.h"
#include "proposer_instance.h"
#include "ring_holder.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <boost/enable_shared_from_this.hpp>
#include <deque>

namespace lightning {

class ProposerState : public RingHolder,
                      public boost::enable_shared_from_this<ProposerState>
{
public:
    typedef paxos::BallotGenerator BallotGenerator;
    typedef paxos::InstancePool InstancePool;
    typedef paxos::ProposerInstance ProposerInstance;

    typedef boost::shared_ptr<ProposerState> ptr;

    ProposerState(GroupConfiguration::ptr group,
                  const Guid& epoch,
                  InstancePool::ptr instancePool,
                  RpcRequester::ptr requester,
                  ClientValueQueue::ptr clientValueQueue,
                  Mordor::IOManager* ioManager,
                  uint64_t phase1TimeoutUs,
                  uint64_t phase1IntervalUs,
                  uint64_t phase2TimeoutUs,
                  uint64_t phase2IntervalUs,
                  uint64_t commitFlushIntervalUs);
    
    void processReservedInstances();

    void processClientValues();

    void flushCommits();

    //! Perform complete Paxos phase 1. On success it is scheduled
    //  for phase 2, on failure it is returned to the instance pool.
    void doPhase1(ProposerInstance::ptr instance);

    //! Perform Paxos phase 2.
    void doPhase2(ProposerInstance::ptr instance);
private:
    // XXX stub
    void onCommit(ProposerInstance::ptr instance);

    GroupConfiguration::ptr group_;
    const Guid epoch_;
    InstancePool::ptr instancePool_;
    RpcRequester::ptr requester_;
    ClientValueQueue::ptr clientValueQueue_;
    Mordor::IOManager* ioManager_;
    const uint64_t phase1TimeoutUs_;
    const uint64_t phase1IntervalUs_;
    const uint64_t phase2TimeoutUs_;
    const uint64_t phase2IntervalUs_;
    const uint64_t commitFlushIntervalUs_;

    BallotGenerator ballotGenerator_;

    typedef std::pair<paxos::InstanceId, Guid> Commit;
    std::deque<Commit> commitQueue_;
    static const size_t kCommitBatchLimit = 10;

    //! Dummy ring id for the phase 2 one-host 'ring'.
    static const size_t kPhase2RingId = 239239;

    Mordor::FiberMutex mutex_;
};


}  // namespace lightning
