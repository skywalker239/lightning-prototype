#include "proposer_state.h"
#include "phase1_request.h"
#include "phase2_request.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Logger;
using Mordor::Log;
using paxos::BallotId;
using paxos::kInvalidBallotId;
using paxos::InstanceId;
using paxos::Value;
using std::make_pair;
using std::min;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:proposer_state");

ProposerState::ProposerState(const GroupConfiguration& groupConfiguration,
                             const Guid& epoch,
                             InstancePool::ptr instancePool,
                             MulticastRpcRequester::ptr requester,
                             ClientValueQueue::ptr clientValueQueue,
                             IOManager* ioManager,
                             uint64_t phase1TimeoutUs,
                             uint64_t phase2TimeoutUs)
    : groupConfiguration_(groupConfiguration),
      epoch_(epoch),
      instancePool_(instancePool),
      requester_(requester),
      clientValueQueue_(clientValueQueue),
      ioManager_(ioManager),
      phase1TimeoutUs_(phase1TimeoutUs),
      phase2TimeoutUs_(phase2TimeoutUs),
      ballotGenerator_(groupConfiguration_)
{
    MORDOR_ASSERT(groupConfiguration_.thisHostId() == 0);
}

void ProposerState::processReservedInstances() {
    while(true) {
        ProposerInstance::ptr instance = instancePool_->popReservedInstance();
        ioManager_->schedule(boost::bind(&ProposerState::doPhase1,
                                         shared_from_this(),
                                         instance));
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
    }
}

void ProposerState::doPhase1(ProposerInstance::ptr instance) {
    //! Full phase 1 presumes that we have already set some ballot id.
    MORDOR_ASSERT(instance->ballotId() != kInvalidBallotId);
    vector<Address::ptr> replyAddresses;
    uint32_t ringId;
    generateRingAddresses(&replyAddresses, &ringId);
    Phase1Request::ptr request(new Phase1Request(epoch_,
                                                 ringId,
                                                 instance->ballotId(),
                                                 instance->instanceId(),
                                                 replyAddresses));
    if(requester_->request(request, phase1TimeoutUs_) ==
           MulticastRpcRequest::COMPLETED)
    {
        switch(request->result()) {
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
        instancePool_->pushReservedInstance(instance);
    }
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
    Address::ptr lastRingHost;
    uint32_t ringId;
    getLastRingHost(&lastRingHost, &ringId);
    Phase2Request::ptr request(new Phase2Request(epoch_,
                                                 ringId,
                                                 instance->instanceId(),
                                                 instance->ballotId(),
                                                 instance->value(),
                                                 commits,
                                                 lastRingHost));
    if(requester_->request(request, phase2TimeoutUs_) ==
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
        instance->phase1Pending(instance->ballotId());
        instancePool_->pushReservedInstance(instance);
        {
            FiberMutex::ScopedLock lk(mutex_);
            for(size_t i = 0; i < commits.size(); ++i) {
                commitQueue_.push_front(commits[i]);
            }
        }
    }
}

void ProposerState::onCommit(ProposerInstance::ptr instance) {
    MORDOR_LOG_INFO(g_log) << this << " COMMIT iid=" <<
                              instance->instanceId() <<
                              " value id=" << instance->value()->valueId;
}

void ProposerState::generateRingAddresses(vector<Address::ptr>* hosts,
                                          uint32_t* ringId) const
{
    RingConfiguration::const_ptr ringConfiguration =
        acquireRingConfiguration();
    // exclude us
    for(size_t i = 1; i < ringConfiguration->ringHostIds().size(); ++i) {
        const uint32_t hostId = ringConfiguration->ringHostIds()[i];
        hosts->push_back(groupConfiguration_.hosts()[hostId].multicastReplyAddress);
    }
    *ringId = ringConfiguration->ringId();
}

void ProposerState::getLastRingHost(Address::ptr* lastRingHost,
                                    uint32_t* ringId) const
{
    RingConfiguration::const_ptr ringConfiguration =
        acquireRingConfiguration();
    const uint32_t lastHostId = ringConfiguration->ringHostIds().back();
    *lastRingHost = groupConfiguration_.hosts()[lastHostId].multicastReplyAddress;
    *ringId = ringConfiguration->ringId();
}

}  // namespace lightning
