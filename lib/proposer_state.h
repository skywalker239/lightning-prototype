#pragma once

#include "ballot_generator.h"
#include "guid.h"
#include "host_configuration.h"
#include "instance_pool.h"
#include "multicast_rpc_requester.h"
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

    ProposerState(const GroupConfiguration& groupConfiguration,
                  const Guid& epoch,
                  InstancePool::ptr instancePool,
                  MulticastRpcRequester::ptr requester,
                  ClientValueQueue::ptr clientValueQueue,
                  Mordor::IOManager* ioManager,
                  uint64_t phase1TimeoutUs,
                  uint64_t phase2TimeoutUs);
    
    void processReservedInstances();

    void processClientValues();

    //! Perform complete Paxos phase 1. On success it is scheduled
    //  for phase 2, on failure it is returned to the instance pool.
    void doPhase1(ProposerInstance::ptr instance);

    //! Perform Paxos phase 2.
    void doPhase2(ProposerInstance::ptr instance);
private:
    // XXX stub
    void onCommit(ProposerInstance::ptr instance);

    //! Blocks until a valid ring configuration is available.
    void generateRingAddresses(std::vector<Mordor::Address::ptr>* hosts,
                               uint32_t* ringId) const;
    //! Ditto
    void getLastRingHost(Mordor::Address::ptr* lastRingHost,
                         uint32_t* ringId) const;

    const GroupConfiguration groupConfiguration_;
    const Guid epoch_;
    InstancePool::ptr instancePool_;
    MulticastRpcRequester::ptr requester_;
    ClientValueQueue::ptr clientValueQueue_;
    Mordor::IOManager* ioManager_;
    const uint64_t phase1TimeoutUs_;
    const uint64_t phase2TimeoutUs_;

    BallotGenerator ballotGenerator_;

    typedef std::pair<paxos::InstanceId, Guid> Commit;
    std::deque<Commit> commitQueue_;
    static const size_t kCommitBatchLimit = 10;

    Mordor::FiberMutex mutex_;
};


}  // namespace lightning