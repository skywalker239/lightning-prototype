#include "recovery_manager.h"
#include "acceptor_state.h"
#include "recovery_request.h"
#include "sleep_helper.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <algorithm>
#include <string>

namespace lightning {

using paxos::InstanceId;
using Mordor::CountStatistic;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Statistics;
using std::back_inserter;
using std::copy;
using std::string;
using std::vector;

static CountStatistic<uint64_t>& g_recoveryAttempts =
    Statistics::registerStatistic("recovery.attempts",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_recoveredInstances =
    Statistics::registerStatistic("recovery.recovered_instances",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_recoveryTimeouts =
    Statistics::registerStatistic("recovery.timeouts",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_recoveryFailures =
    Statistics::registerStatistic("recovery.failures",
                                  CountStatistic<uint64_t>());

static Logger::ptr g_log = Log::lookup("lightning:recovery_manager");

RecoveryManager::RecoveryManager(GroupConfiguration::ptr groupConfiguration,
                                 RpcRequester::ptr requester,
                                 IOManager* ioManager,
                                 uint64_t recoveryIntervalUs,
                                 uint64_t recoveryTimeoutUs)
    : groupConfiguration_(groupConfiguration),
      requester_(requester),
      ioManager_(ioManager),
      recoveryIntervalUs_(recoveryIntervalUs),
      recoveryTimeoutUs_(recoveryTimeoutUs)
{
    vector<uint32_t> ourDcAcceptors;
    vector<uint32_t> otherAcceptors;
    const uint32_t thisHostId = groupConfiguration_->thisHostId();
    const string& thisHostDc =
        groupConfiguration_->host(thisHostId).datacenter;
    for(size_t i = 0; i < groupConfiguration_->size(); ++i) {
        if(groupConfiguration_->host(i).datacenter == thisHostDc &&
           i != thisHostId)
        {
            ourDcAcceptors.push_back(i);
        } else {
            otherAcceptors.push_back(i);
        }
    }

    copy(ourDcAcceptors.begin(),
         ourDcAcceptors.end(),
         back_inserter(recoveryHostIds_));
    copy(otherAcceptors.begin(),
         otherAcceptors.end(),
         back_inserter(recoveryHostIds_));
    MORDOR_ASSERT(recoveryHostIds_.size() > 0);
}

void RecoveryManager::addInstance(const Guid& epoch,
                                  InstanceId instanceId,
                                  AcceptorState::ptr acceptor)
{
    MORDOR_LOG_TRACE(g_log) << this << " addInstance(" << epoch << ", " <<
                               instanceId << ")";
    recoveryQueue_.push(RecoveryRecord(epoch, instanceId, acceptor, 0));
    g_recoveryAttempts.increment();
}

void RecoveryManager::recoverInstances() {
    SleepHelper sleeper(ioManager_,
                        recoveryIntervalUs_,
                        SleepHelper::kEpollSleepPrecision);
    while(true) {
        sleeper.startWaiting();
        RecoveryRecord recoveryRecord(recoveryQueue_.pop());
        sleeper.stopWaiting();
        sleeper.wait();
        MORDOR_LOG_TRACE(g_log) << this << " scheduling recovery of (" <<
                                   recoveryRecord.epoch << ", " <<
                                   recoveryRecord.instanceId << ") from " <<
                                   groupConfiguration_->host(
                                       recoveryHostIds_[
                                           recoveryRecord.recoveryHostIndex]);
        ioManager_->schedule(boost::bind(&RecoveryManager::doRecovery,
                                         shared_from_this(),
                                         recoveryRecord));
    }
}

void RecoveryManager::doRecovery(const RecoveryRecord recoveryRecord) {
    RecoveryRequest::ptr request(
        new RecoveryRequest(
            groupConfiguration_,
            recoveryHostIds_[recoveryRecord.recoveryHostIndex],
            recoveryTimeoutUs_,
            recoveryRecord.epoch,
            recoveryRecord.instanceId));
    auto status = requester_->request(request);
    if(status == RpcRequest::COMPLETED) {
        switch(request->result()) {
            case RecoveryRequest::NOT_COMMITTED:
                MORDOR_LOG_TRACE(g_log) << this <<  " (" <<
                    recoveryRecord.epoch << ", " <<
                    recoveryRecord.instanceId << ") not committed on " <<
                    groupConfiguration_->host(recoveryHostIds_[recoveryRecord.recoveryHostIndex]);
                if(recoveryRecord.acceptor->needsRecovery(
                   recoveryRecord.instanceId))
                {
                    MORDOR_LOG_TRACE(g_log) << this << " pushing " <<
                        recoveryRecord.epoch << ", " <<
                        recoveryRecord.instanceId << ") back to queue";
                    recoveryQueue_.push(recoveryRecord);
                    g_recoveryAttempts.increment();
                } else {
                    MORDOR_LOG_TRACE(g_log) << this << " (" <<
                        recoveryRecord.epoch << ", " <<
                        recoveryRecord.instanceId << ") already recovered";
                }
                break;
            case RecoveryRequest::FORGOTTEN:
                // XXX what to do here?
                MORDOR_LOG_ERROR(g_log) << this << " (" <<
                    recoveryRecord.epoch << ", " <<
                    recoveryRecord.instanceId << ") forgotten on" <<
                    groupConfiguration_->host(recoveryHostIds_[recoveryRecord.recoveryHostIndex]);
                g_recoveryFailures.increment();
                break;
            case RecoveryRequest::OK:
                MORDOR_LOG_TRACE(g_log) << this << " recovered (" <<
                    recoveryRecord.epoch << ", " <<
                    recoveryRecord.instanceId << ") = (" <<
                    request->value()->valueId << ", " << request->ballot() <<
                    ")";
                recoveryRecord.acceptor->setInstance(
                    recoveryRecord.instanceId,
                    *request->value(),
                    request->ballot());
                g_recoveredInstances.increment();
                break;
        }
    } else {
        const uint32_t newIndex =
            (recoveryRecord.recoveryHostIndex + 1) % recoveryHostIds_.size();
        MORDOR_LOG_TRACE(g_log) << this << " recovery of (" <<
            recoveryRecord.epoch << ", " << recoveryRecord.instanceId <<
            ") from " <<
            groupConfiguration_->host(recoveryHostIds_[recoveryRecord.recoveryHostIndex]) << " timed out, retrying with host " <<
            groupConfiguration_->host(recoveryHostIds_[newIndex]);
        recoveryQueue_.push(
            RecoveryRecord(
                recoveryRecord.epoch,
                recoveryRecord.instanceId,
                recoveryRecord.acceptor,
                newIndex));
        g_recoveryTimeouts.increment();
    }
}

}  // namespace lightning
