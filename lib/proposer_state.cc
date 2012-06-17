#include "proposer_state.h"
#include "guid.h"
#include "phase1_request.h"
#include "phase2_request.h"
#include "sleep_helper.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <algorithm>

namespace lightning {

const size_t ProposerState::kCommitBatchLimit;
const size_t ProposerState::kPhase2RingId;

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
using boost::shared_ptr;
using std::find;
using std::list;
using std::make_pair;
using std::min;
using std::string;
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
static CountStatistic<uint64_t>& g_commitQueueSize =
    Statistics::registerStatistic("proposer.commmit_queue_size",
                                  CountStatistic<uint64_t>());

ProposerState::ProposerState(GroupConfiguration::ptr group,
                             const Guid& epoch,
                             InstancePool::ptr instancePool,
                             RpcRequester::ptr requester,
                             BlockingQueue<Value>::ptr clientValueQueue,
                             ValueCache::ptr valueCache,
                             IOManager* ioManager,
                             uint64_t phase1TimeoutUs,
                             uint64_t phase1IntervalUs,
                             uint64_t phase2TimeoutUs,
                             uint64_t phase2IntervalUs,
                             uint64_t commitFlushIntervalUs)
    : group_(group),
      epoch_(epoch),
      instancePool_(instancePool),
      requester_(requester),
      clientValueQueue_(clientValueQueue),
      valueCache_(valueCache),
      ioManager_(ioManager),
      phase1TimeoutUs_(phase1TimeoutUs),
      phase1IntervalUs_(phase1IntervalUs),
      phase2TimeoutUs_(phase2TimeoutUs),
      phase2IntervalUs_(phase2IntervalUs),
      commitFlushIntervalUs_(commitFlushIntervalUs),
      ballotGenerator_(group_)
{
    valueCache_->updateEpoch(epoch);
    MORDOR_ASSERT(group_->thisHostId() == group_->masterId());
}

void ProposerState::processReservedInstances() {
    SleepHelper sleeper(ioManager_,
                        phase1IntervalUs_,
                        SleepHelper::kEpollSleepPrecision);
    while(true) {
        sleeper.startWaiting();
        ProposerInstance::ptr instance = instancePool_->popReservedInstance();
        sleeper.stopWaiting();
        sleeper.wait();
        ioManager_->schedule(boost::bind(&ProposerState::doPhase1,
                                         shared_from_this(),
                                         instance));
        g_pendingPhase1.increment();
        MORDOR_LOG_TRACE(g_log) << this << " scheduled phase 1 for iid=" <<
                                   instance->instanceId();
    }
}

void ProposerState::processClientValues() {
    SleepHelper sleeper(ioManager_,
                        phase2IntervalUs_,
                        SleepHelper::kEpollSleepPrecision);
    while(true) {
        sleeper.startWaiting();
        ProposerInstance::ptr instance = instancePool_->popOpenInstance();
        auto currentValue = clientValueQueue_->pop();
        sleeper.stopWaiting();
        sleeper.wait();
        MORDOR_LOG_TRACE(g_log) << this << " submitting value " <<
                                   currentValue << " to instance " <<
                                   instance->instanceId();
        instance->setValue(currentValue, true);
        ioManager_->schedule(boost::bind(&ProposerState::doPhase2,
                                         shared_from_this(),
                                         instance));
        g_pendingPhase2.increment();
    }
}

// XXX HACK, push a dummy value for now
void ProposerState::flushCommits() {
    SleepHelper sleeper(ioManager_,
                        commitFlushIntervalUs_,
                        SleepHelper::kEpollSleepPrecision);
    GuidGenerator g;
    while(true) {
        sleeper.wait();
        Value value(g.generate(), shared_ptr<string>(new string));
        clientValueQueue_->push(value);
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
                    instance->setBallotId(newBallot);
                    instancePool_->pushReservedInstance(instance);
                }
                break;
            case Phase1Request::SUCCESS:
                if(request->lastVotedBallot() != kInvalidBallotId) {
                    Value foundValue = request->lastVotedValue();
                    MORDOR_LOG_TRACE(g_log) << this << " phase1 found " <<
                                               "a value for iid=" <<
                                               instance->instanceId() <<
                                               ", value_id=" <<
                                               foundValue;
                    bool foundClientValue = true;
                    if(instance->hasClientValue()) {
                        if(instance->value().valueId() !=
                           foundValue.valueId())
                        {
                            MORDOR_LOG_TRACE(g_log) << this <<
                                                       " returning client " <<
                                                       instance->value() <<
                                                       " to the queue";
                            clientValueQueue_->push_front(
                                instance->releaseValue());
                            foundClientValue = false;
                        }
                    }

                    instance->setValue(foundValue, foundClientValue);
                    ioManager_->schedule(boost::bind(&ProposerState::doPhase2,
                                                     shared_from_this(),
                                                     instance));
                    g_pendingPhase2.increment();
                } else {
                    MORDOR_LOG_TRACE(g_log) << this << " phase1 for iid=" <<
                                               instance->instanceId() <<
                                               " is open at ballot=" <<
                                               instance->ballotId();
                    if(instance->hasClientValue()) {
                        clientValueQueue_->push_front(
                            instance->releaseValue());
                    }
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
        instance->setBallotId(newBallot);
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
            i < min(ProposerState::kCommitBatchLimit, commitQueue_.size());
            ++i)
        {
            commits.push_back(commitQueue_.front());
            commitQueue_.pop_front();
        }
        g_commitQueueSize.reset();
        g_commitQueueSize.add(commitQueue_.size());
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
        {
            FiberMutex::ScopedLock lk(mutex_);
            commitQueue_.push_back(make_pair(instance->instanceId(),
                                             instance->value().valueId()));
            g_commitQueueSize.reset();
            g_commitQueueSize.add(commitQueue_.size());
        }
        onCommit(instance);
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " phase2 for iid=" <<
                                   instance->instanceId() << " timed out";
        instance->setBallotId(
            ballotGenerator_.boostBallotId(instance->ballotId()));
        instancePool_->pushReservedInstance(instance);
        {
            FiberMutex::ScopedLock lk(mutex_);
            for(size_t i = 0; i < commits.size(); ++i) {
                commitQueue_.push_front(commits[i]);
            }
            g_commitQueueSize.reset();
            g_commitQueueSize.add(commitQueue_.size());
        }
        g_phase2Timeouts.increment();
    }
    g_pendingPhase2.decrement();
}

void ProposerState::addNotifier(
    Notifier<ProposerInstance::ptr>* notifier)
{
    FiberMutex::ScopedLock lk(mutex_);

    notifiers_.push_back(notifier);
}

void ProposerState::removeNotifier(
    Notifier<ProposerInstance::ptr>* notifier)
{
    FiberMutex::ScopedLock lk(mutex_);

    auto iter = find(notifiers_.begin(), notifiers_.end(), notifier);
    if(iter != notifiers_.end()) {
        notifiers_.erase(iter);
    }
}

void ProposerState::onCommit(ProposerInstance::ptr instance) {
    MORDOR_LOG_DEBUG(g_log) << this << " COMMIT iid=" <<
                               instance->instanceId() <<
                               " value=" << instance->value();
    g_committedValues.increment();
    g_committedBytes.add(instance->value().size());
    for(auto i = notifiers_.begin(); i != notifiers_.end(); ++i) {
        (*i)->notify(instance);
    }
    valueCache_->push(instance->instanceId(),
                      instance->ballotId(),
                      instance->value());

}

}  // namespace lightning
