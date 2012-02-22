#include "proposer_state.h"
#include "phase1_request.h"
#include "phase2_request.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Statistics;
using Mordor::CountStatistic;
using paxos::BallotId;
using paxos::kInvalidBallotId;
using paxos::InstanceId;
using paxos::Value;
using std::make_pair;
using std::min;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:proposer_state");

static CountStatistic<uint64_t>& g_pendingPhase1 =
    Statistics::registerStatistic("proposer.pending_phase1",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_pendingPhase2 =
    Statistics::registerStatistic("proposer.pending_phase2",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_phase1Timeouts =
    Statistics::registerStatistic("proposer.phase1_timeouts",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_phase2Timeouts =
    Statistics::registerStatistic("proposer.phase2_timeouts",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_committedValues =
    Statistics::registerStatistic("proposer.committed_values",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_committedBytes =
    Statistics::registerStatistic("proposer.committed_bytes",
                                  CountStatistic<uint64_t>());

ProposerState::ProposerState(GroupConfiguration::ptr group,
                             const Guid& epoch,
                             InstancePool::ptr instancePool,
                             MulticastRpcRequester::ptr requester,
                             ClientValueQueue::ptr clientValueQueue,
                             IOManager* ioManager,
                             uint64_t phase1TimeoutUs,
                             uint64_t phase2TimeoutUs)
    : group_(group),
      epoch_(epoch),
      instancePool_(instancePool),
      requester_(requester),
      clientValueQueue_(clientValueQueue),
      ioManager_(ioManager),
      phase1TimeoutUs_(phase1TimeoutUs),
      phase2TimeoutUs_(phase2TimeoutUs),
      ballotGenerator_(group_)
{
    MORDOR_ASSERT(group_->thisHostId() == 0);
}

void ProposerState::processReservedInstances() {
    while(true) {
        ProposerInstance::ptr instance = instancePool_->popReservedInstance();
        ioManager_->schedule(boost::bind(&ProposerState::doPhase1,
                                         shared_from_this(),
                                         instance));
        g_pendingPhase1.increment();
        MORDOR_LOG_TRACE(g_log) << this << " scheduled phase 1 for iid=" <<
                                   instance->instanceId();
    }
}

void ProposerState::processClientValues() {
    while(true) {
        ProposerInstance::ptr instance = instancePool_->popOpenInstance();
        Value::ptr currentValue = clientValueQueue_->pop();
        MORDOR_LOG_TRACE(g_log) << this << " submitting value " <<
                                   currentValue->valueId << " to instance " <<
                                   instance->instanceId();
        instance->phase2PendingWithClientValue(currentValue);
        ioManager_->schedule(boost::bind(&ProposerState::doPhase2,
                                         shared_from_this(),
                                         instance));
        g_pendingPhase2.increment();
    }
}

void ProposerState::doPhase1(ProposerInstance::ptr instance) {
    //! Full phase 1 presumes that we have already set some ballot id.
    MORDOR_ASSERT(instance->ballotId() != kInvalidBallotId);

    RingConfiguration::const_ptr ring = acquireRingConfiguration();
    Phase1Request::ptr request(new Phase1Request(epoch_,
                                                 instance->ballotId(),
                                                 instance->instanceId(),
                                                 ring,
                                                 phase1TimeoutUs_));
    if(requester_->request(request) ==
           MulticastRpcRequest::COMPLETED)
    {
        switch(request->result()) {
            case Phase1Request::FORGOTTEN:
                {
                    MORDOR_LOG_TRACE(g_log) << this <<
                                               "iid=" <<
                                               instance->instanceId() <<
                                               " was forgotten";
                    break;
                }
            case Phase1Request::BALLOT_TOO_LOW:
                {
                    MORDOR_LOG_TRACE(g_log) << this <<
                                               " ballot too low for iid=" <<
                                               instance->instanceId() <<
                                               " last promised=" <<
                                               request->lastPromisedBallot();
                    BallotId newBallot =
                        ballotGenerator_.boostBallotId(
                            request->lastPromisedBallot());
                    MORDOR_LOG_TRACE(g_log) << this << " iid=" <<
                                               instance->instanceId() <<
                                               " retry with ballot " << newBallot;
                    instance->phase1Retry(newBallot);
                    instancePool_->pushReservedInstance(instance);
                }
                break;
            case Phase1Request::SUCCESS:
                if(request->lastVotedBallot() != kInvalidBallotId) {
                    MORDOR_LOG_TRACE(g_log) << this << " phase1 found " <<
                                               "a value for iid=" <<
                                               instance->instanceId() <<
                                               ", value_id=" <<
                                               request->lastVotedValue()->valueId;
                    instance->phase2Pending(request->lastVotedValue());
                    ioManager_->schedule(boost::bind(&ProposerState::doPhase2,
                                                     shared_from_this(),
                                                     instance));
                    g_pendingPhase2.increment();
                } else {
                    MORDOR_LOG_TRACE(g_log) << this << " phase1 for iid=" <<
                                               instance->instanceId() <<
                                               " is open at ballot=" <<
                                               instance->ballotId();
                    instance->phase1Open(instance->ballotId());
                    instancePool_->pushOpenInstance(instance);
                }
                break;
            default:
                MORDOR_ASSERT(1==0);
        }
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " phase1 timeout for iid=" <<
                                   instance->instanceId();
        BallotId newBallot =
            ballotGenerator_.boostBallotId(instance->ballotId());
        instance->phase1Retry(newBallot);
        instancePool_->pushReservedInstance(instance);
        g_phase1Timeouts.increment();
    }
    g_pendingPhase1.decrement();
}

void ProposerState::doPhase2(ProposerInstance::ptr instance) {
    vector<Commit> commits;
    {
        FiberMutex::ScopedLock lk(mutex_);
        for(size_t i = 0;
            i < min(kCommitBatchLimit, commitQueue_.size());
            ++i)
        {
            commits.push_back(commitQueue_.front());
            commitQueue_.pop_front();
        }
    }

    RingConfiguration::const_ptr ring = acquireRingConfiguration();
    // XXX extra allocation
    RingConfiguration::ptr phase2Ring(
        new RingConfiguration(group_,
                              vector<uint32_t>(1, ring->ringHostIds().back()),
                              kPhase2RingId));
    Phase2Request::ptr request(new Phase2Request(epoch_,
                                                 ring->ringId(),
                                                 instance->instanceId(),
                                                 instance->ballotId(),
                                                 instance->value(),
                                                 commits,
                                                 phase2Ring,
                                                 phase2TimeoutUs_));
    if(requester_->request(request) ==
        MulticastRpcRequest::COMPLETED)
    {
        MORDOR_LOG_TRACE(g_log) << this << " phase2 for iid=" <<
                                   instance->instanceId() << " successful";
        instance->close();
        commitQueue_.push_back(make_pair(instance->instanceId(),
                                         instance->value()->valueId));
        onCommit(instance);
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " phase2 for iid=" <<
                                   instance->instanceId() << " timed out";
        if(instance->state() == ProposerInstance::P2_PENDING_CLIENT_VALUE) {
            clientValueQueue_->push_front(instance->value());
        }
        instance->phase1Pending(
            ballotGenerator_.boostBallotId(instance->ballotId()));
        instancePool_->pushReservedInstance(instance);
        {
            FiberMutex::ScopedLock lk(mutex_);
            for(size_t i = 0; i < commits.size(); ++i) {
                commitQueue_.push_front(commits[i]);
            }
        }
        g_phase2Timeouts.increment();
    }
    g_pendingPhase2.decrement();
}

void ProposerState::onCommit(ProposerInstance::ptr instance) {
    MORDOR_LOG_INFO(g_log) << this << " COMMIT iid=" <<
                              instance->instanceId() <<
                              " value id=" << instance->value()->valueId;
    g_committedValues.increment();
    g_committedBytes.add(instance->value()->size);
}

}  // namespace lightning
