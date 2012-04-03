#include "recovery_request.h"
#include <mordor/assert.h>

namespace lightning {

using paxos::BallotId;
using paxos::kInvalidBallotId;
using paxos::Value;

RecoveryRequest::RecoveryRequest(GroupConfiguration::ptr groupConfiguration,
                                 uint32_t destinationHostId,
                                 uint64_t timeoutUs,
                                 const Guid& epoch,
                                 paxos::InstanceId instanceId)
    : UnicastRpcRequest(groupConfiguration, destinationHostId, timeoutUs),
      ballotId_(kInvalidBallotId),
      result_(OK)
{
    requestData_.set_type(RpcMessageData::RECOVERY);
    RecoveryRequestData* request = requestData_.mutable_recovery_request();
    epoch.serialize(request->mutable_epoch());
    request->set_instance(instanceId);
}

RecoveryRequest::Result RecoveryRequest::result() const {
    return result_;
}

Value::ptr RecoveryRequest::value() const {
    return value_;
}

BallotId RecoveryRequest::ballot() const {
    return ballotId_;
}

void RecoveryRequest::applyReply(const RpcMessageData& reply) {
    MORDOR_ASSERT(reply.type() == RpcMessageData::RECOVERY);
    MORDOR_ASSERT(reply.has_recovery_reply());
    const RecoveryReplyData& recoveryReply = reply.recovery_reply();

    switch(recoveryReply.type()) {
        case RecoveryReplyData::NOT_COMMITTED:
            result_ = NOT_COMMITTED;
            break;
        case RecoveryReplyData::FORGOTTEN:
            result_ = FORGOTTEN;
            break;
        case RecoveryReplyData::OK:
            result_ = OK;
            value_ = Value::parse(recoveryReply.value());
            break;
        default:
            MORDOR_ASSERT(1==0);
            break;
    }
}

std::ostream& RecoveryRequest::output(std::ostream& os) const {
    const Guid epoch = Guid::parse(requestData_.recovery_request().epoch());
    os << "Recovery(" << epoch << ", " <<
          requestData_.recovery_request().instance() << ")";
    return os;
}

}  // namespace lightning
