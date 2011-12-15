#include "phase1_batcher.h"
#include "batch_phase1_request.h"
#include "proposer_instance.h"
#include <mordor/log.h>

namespace lightning {

using boost::shared_ptr;
using Mordor::FiberEvent;
using Mordor::Log;
using Mordor::Logger;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::InstancePool;
using paxos::ProposerInstance;
using std::map;

static Logger::ptr g_log = Log::lookup("lightning:phase1_batcher");

Phase1Batcher::Phase1Batcher(uint32_t batchSize,
                             BallotId initialBallot,
                             InstancePool::ptr instancePool,
                             SyncGroupRequester::ptr requester,
                             shared_ptr<FiberEvent>
                                pushMoreOpenInstancesEvent)
    : RingHolder(),
      batchSize_(batchSize),
      initialBallot_(initialBallot),
      instancePool_(instancePool),
      requester_(requester),
      pushMoreOpenInstancesEvent_(pushMoreOpenInstancesEvent),
      nextStartInstanceId_(0)
{}

void Phase1Batcher::run() {
    while(true) {
        MORDOR_LOG_TRACE(g_log) << this << " waiting until more open " <<
                                   "instances are needed";
        pushMoreOpenInstancesEvent_->wait();

        RingConfiguration::const_ptr
            ringConfiguration(acquireRingConfiguration());
        const InstanceId batchStartId = nextStartInstanceId_;
        const InstanceId batchEndId   = nextStartInstanceId_ + batchSize_;

        MORDOR_LOG_TRACE(g_log) << this << " batch phase 1 ring_id=" <<
                                   ringConfiguration->ringId() << ", " <<
                                   "ballot=" << initialBallot_ << ", " <<
                                   "instances=[" << batchStartId << ", " <<
                                   batchEndId << ")";
        BatchPhase1Request::ptr request(
            new BatchPhase1Request(
                ringConfiguration,
                initialBallot_,
                batchStartId,
                batchEndId));

        SyncGroupRequest::Status status = requester_->request(request);
        MORDOR_LOG_TRACE(g_log) << this << " instances [" << batchStartId <<
                                   ", " << batchEndId << "): (" <<
                                   status << ", " << request->result();
        switch(request->result()) {
            case BatchPhase1Request::ALL_OPEN:
                openInstanceRange(batchStartId, batchEndId);
                break;
            case BatchPhase1Request::NOT_ALL_OPEN:
                processMixedResult(batchStartId,
                                   batchEndId,
                                   request->reservedInstances());
                break;
            case BatchPhase1Request::START_IID_TOO_LOW:
               resetNextInstanceId(request->retryStartInstanceId());
               break;
            default:
                MORDOR_ASSERT(1==0);
                break;
        }

        nextStartInstanceId_ += batchSize_;
    }
}

void Phase1Batcher::openInstanceRange(InstanceId startId,
                                      InstanceId endId)
{
    MORDOR_LOG_TRACE(g_log) << this << " opening all instances in range [" <<
                               startId << ", " << endId << ")";
    for(InstanceId iid = startId; iid < endId; ++iid) {
        ProposerInstance::ptr instance(new ProposerInstance(iid));
        instance->phase1Open(initialBallot_);
        instancePool_->pushOpenInstance(instance);
    }
}

void Phase1Batcher::processMixedResult(InstanceId startId,
                                       InstanceId endId,
                                       const map<InstanceId, BallotId>&
                                           reservedInstances)
{
    MORDOR_LOG_TRACE(g_log) << this << " processing mixed range [" <<
                               startId << ", " << endId;
    for(InstanceId iid = startId; iid < endId; ++iid) {
        auto reservedIter = reservedInstances.find(iid);
        ProposerInstance::ptr instance(new ProposerInstance(iid));
        if(reservedIter == reservedInstances.end()) {
            instance->phase1Open(initialBallot_);
            instancePool_->pushOpenInstance(instance);
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " instance " << iid <<
                                       " reserved with ballot " <<
                                       reservedIter->second;
            instance->phase1Pending(reservedIter->second);
            instancePool_->pushReservedInstance(instance);
        }
    }
}

void Phase1Batcher::resetNextInstanceId(InstanceId newStartId) {
    MORDOR_LOG_TRACE(g_log) << this << " reset next iid to " << newStartId;
    nextStartInstanceId_ = newStartId;
}

}  // namespace lightning
