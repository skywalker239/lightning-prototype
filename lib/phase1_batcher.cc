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
using paxos::BallotGenerator;
using paxos::kInvalidBallotId;
using paxos::InstanceId;
using paxos::InstancePool;
using paxos::ProposerInstance;
using std::set;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:phase1_batcher");

Phase1Batcher::Phase1Batcher(const Guid& epoch,
                             uint64_t timeoutUs,
                             uint32_t batchSize,
                             const BallotGenerator& ballotGenerator,
                             InstancePool::ptr instancePool,
                             MulticastRpcRequester::ptr requester,
                             shared_ptr<FiberEvent>
                                pushMoreOpenInstancesEvent)
    : epoch_(epoch),
      timeoutUs_(timeoutUs),
      batchSize_(batchSize),
      ballotGenerator_(ballotGenerator),
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

        const InstanceId startInstance = nextStartInstanceId_;
        const InstanceId endInstance   = nextStartInstanceId_ + batchSize_;

        BatchPhase1Request::ptr request;
        BallotId successfulBallot = kInvalidBallotId;

        requestInstanceRange(startInstance,
                             endInstance,
                             ring,
                             &request,
                             &successfulBallot);
        MORDOR_ASSERT(request.get());
        MORDOR_ASSERT(request->status() == MulticastRpcRequest::COMPLETED);

        MORDOR_LOG_TRACE(g_log) << this << " instances [" << startInstance <<
                                   ", " << endInstance << "): (" <<
                                   uint32_t(request->status()) << ", " <<
                                   uint32_t(request->result()) << ")";

        switch(request->result()) {
            case BatchPhase1Request::IID_TOO_LOW:
                resetNextInstanceId(request->retryStartInstanceId());
                break;
            case BatchPhase1Request::SUCCESS:
                MORDOR_ASSERT(successfulBallot != kInvalidBallotId);
                openInstances(startInstance,
                              endInstance,
                              successfulBallot,
                              request->reservedInstances());
                nextStartInstanceId_ += batchSize_;
                break;
            default:
                MORDOR_LOG_ERROR(g_log) << this << " unknown result " <<
                                           uint32_t(request->result());
                MORDOR_ASSERT(1==0);
                break;
        }
    }
}

void Phase1Batcher::requestInstanceRange(InstanceId startInstance,
                                         InstanceId endInstance,
                                         RingConfiguration::const_ptr ring,
                                         BatchPhase1Request::ptr* request,
                                         BallotId* successfulBallot)
{
    BallotId currentBallot = ballotGenerator_.boostBallotId(kInvalidBallotId);
    while(true) {
        MORDOR_LOG_TRACE(g_log) << this << " batch phase 1 ring=" << *ring <<
                                   ", " <<
                                   "ballot=" << currentBallot << ", " <<
                                   "instances=[" << startInstance << ", " <<
                                   endInstance << ")";
        request->reset(
            new BatchPhase1Request(
                epoch_,
                currentBallot,
                startInstance,
                endInstance,
                ring,
                timeoutUs_));

        MulticastRpcRequest::Status status = requester_->request(*request);
        if(status == MulticastRpcRequest::TIMED_OUT) {
            MORDOR_LOG_TRACE(g_log) << this << " [[" << startInstance <<
                                       ", " << endInstance << "), " <<
                                       currentBallot << "] timed out";
            currentBallot = ballotGenerator_.boostBallotId(kInvalidBallotId);
            continue;
        } else {
            *successfulBallot = currentBallot;
            return;
        }
    }
}

void Phase1Batcher::openInstances(InstanceId startId,
                                  InstanceId endId,
                                  BallotId ballotId,
                                  const set<InstanceId>& reservedInstances)
{
    MORDOR_LOG_TRACE(g_log) << this << " opening range [" << startId <<
                               ", " << endId << ") with ballot " << ballotId;
    auto reservedEnd = reservedInstances.end();
    for(InstanceId iid = startId; iid < endId; ++iid) {
        ProposerInstance::ptr instance(new ProposerInstance(iid));
        if(reservedInstances.find(iid) == reservedEnd) {
            instance->phase1Open(ballotId);
            instancePool_->pushOpenInstance(instance);
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " iid=" << iid <<
                                       " is reserved";
            instance->phase1Pending(ballotId);
            instancePool_->pushReservedInstance(instance);
        }
    }
}

void Phase1Batcher::resetNextInstanceId(InstanceId newStartId) {
    MORDOR_LOG_TRACE(g_log) << this << " reset next iid to " << newStartId;
    nextStartInstanceId_ = newStartId;
}

}  // namespace lightning
