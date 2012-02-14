#include "phase1_batcher.h"
#include "batch_phase1_request.h"
#include "proposer_instance.h"
#include <mordor/log.h>

namespace lightning {

using boost::shared_ptr;
using Mordor::Address;
using Mordor::FiberEvent;
using Mordor::Log;
using Mordor::Logger;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::InstancePool;
using paxos::ProposerInstance;
using std::set;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:phase1_batcher");

Phase1Batcher::Phase1Batcher(const Guid& epoch,
                             uint64_t timeoutUs,
                             uint32_t batchSize,
                             BallotId initialBallot,
                             InstancePool::ptr instancePool,
                             MulticastRpcRequester::ptr requester,
                             shared_ptr<FiberEvent>
                                pushMoreOpenInstancesEvent)
    : epoch_(epoch),
      timeoutUs_(timeoutUs),
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

        MORDOR_LOG_TRACE(g_log) << this << " requesting ring configuration";
        RingConfiguration::const_ptr ring = acquireRingConfiguration();

        const InstanceId batchStartId = nextStartInstanceId_;
        const InstanceId batchEndId   = nextStartInstanceId_ + batchSize_;

        MORDOR_LOG_TRACE(g_log) << this << " batch phase 1 ring=" << *ring <<
                                   ", " <<
                                   "ballot=" << initialBallot_ << ", " <<
                                   "instances=[" << batchStartId << ", " <<
                                   batchEndId << ")";
        BatchPhase1Request::ptr request(
            new BatchPhase1Request(
                epoch_,
                initialBallot_,
                batchStartId,
                batchEndId,
                ring,
                timeoutUs_));

        MulticastRpcRequest::Status status = requester_->request(request);
        MORDOR_LOG_TRACE(g_log) << this << " instances [" << batchStartId <<
                                   ", " << batchEndId << "): (" <<
                                   uint32_t(status) << ", " <<
                                   uint32_t(request->result()) << ")";
        if(status == MulticastRpcRequest::TIMED_OUT) {
            MORDOR_LOG_TRACE(g_log) << this << " timed out";
            continue;
        }


        switch(request->result()) {
            case BatchPhase1Request::IID_TOO_LOW:
                resetNextInstanceId(request->retryStartInstanceId());
                break;
            case BatchPhase1Request::SUCCESS:
                openInstances(batchStartId,
                              batchEndId,
                              request->reservedInstances());
                break;
            default:
                MORDOR_ASSERT(1==0);
                break;
        }

        nextStartInstanceId_ += batchSize_;
    }
}

void Phase1Batcher::openInstances(InstanceId startId,
                                  InstanceId endId,
                                  const set<InstanceId>& reservedInstances)
{
    MORDOR_LOG_TRACE(g_log) << this << " opening range [" << startId <<
                               ", " << endId << ")";
    auto reservedEnd = reservedInstances.end();
    for(InstanceId iid = startId; iid < endId; ++iid) {
        ProposerInstance::ptr instance(new ProposerInstance(iid));
        if(reservedInstances.find(iid) == reservedEnd) {
            instance->phase1Open(initialBallot_);
            instancePool_->pushOpenInstance(instance);
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " iid=" << iid <<
                                       " is reserved";
            instance->phase1Pending(initialBallot_);
            instancePool_->pushReservedInstance(instance);
        }
    }
}

void Phase1Batcher::resetNextInstanceId(InstanceId newStartId) {
    MORDOR_LOG_TRACE(g_log) << this << " reset next iid to " << newStartId;
    nextStartInstanceId_ = newStartId;
}

}  // namespace lightning
