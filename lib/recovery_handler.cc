#include "recovery_handler.h"
#include "paxos_defs.h"
#include "value.h"
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::Address;
using Mordor::CountStatistic;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Statistics;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::kInvalidBallotId;
using paxos::Value;

static Logger::ptr g_log = Log::lookup("lightning:recovery_handler");

RecoveryHandler::RecoveryHandler(AcceptorState::ptr acceptorState)
    : acceptorState_(acceptorState)
{}

bool RecoveryHandler::handleRequest(Address::ptr sourceAddress,
                                    const RpcMessageData& request,
                                    RpcMessageData* reply)
{
    const RecoveryRequestData& recoveryRequest = request.recovery_request();
    Guid requestEpoch = Guid::parse(recoveryRequest.epoch());
    InstanceId instanceId = recoveryRequest.instance();

    MORDOR_LOG_TRACE(g_log) << this << " recover(" << requestEpoch << ", " <<
                               instanceId << ") from " << *sourceAddress;

    reply->set_type(RpcMessageData::RECOVERY);
    RecoveryReplyData* recoveryReply = reply->mutable_recovery_reply();

    Value value;
    BallotId ballotId = kInvalidBallotId;
    // XXX EPOCH, also race condition on recovery status
    if(!acceptorState_->getCommittedInstance(instanceId, &value, &ballotId)) {
        if(instanceId < acceptorState_->firstNotForgottenInstance()) {
            MORDOR_LOG_TRACE(g_log) << this << " recover: iid " <<
                                       instanceId << " forgotten";
            recoveryReply->set_type(RecoveryReplyData::FORGOTTEN);
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " recover: iid " <<
                                       instanceId << " not committed";
            recoveryReply->set_type(RecoveryReplyData::NOT_COMMITTED);
        }
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " recover : iid " <<
                                   instanceId << " -> (" << value <<
                                   ", " << ballotId << ")";
        recoveryReply->set_type(RecoveryReplyData::OK);
        value.serialize(recoveryReply->mutable_value());
    }
    return true;
}

}  // namespace lightning
