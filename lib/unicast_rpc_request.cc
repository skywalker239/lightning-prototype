#include "unicast_rpc_request.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;

static Logger::ptr g_log = Log::lookup("lightning:unicast_rpc_request");

UnicastRpcRequest::UnicastRpcRequest(GroupConfiguration::ptr groupConfiguration,
                                     uint32_t destinationHostId,
                                     uint64_t timeoutUs)
    : RpcRequest(groupConfiguration->host(destinationHostId).unicastAddress,
                 timeoutUs),
      groupConfiguration_(groupConfiguration),
      destinationHostId_(destinationHostId),
      acked_(false)
{}

void UnicastRpcRequest::onReply(const Address::ptr& sourceAddress,
                                const RpcMessageData& reply)
{
    FiberMutex::ScopedLock lk(mutex_);

    if(acked_) {
        MORDOR_LOG_WARNING(g_log) << *this << "@" << this <<
                                     " already acked, got reply from" <<
                                     groupConfiguration_->host(
                                        destinationHostId_);
        return;
    }

    uint32_t sourceHostId =
        groupConfiguration_->replyAddressToId(sourceAddress);
    if(sourceHostId != destinationHostId_) {
        MORDOR_LOG_WARNING(g_log) << *this << "@" << this <<
                                     " got reply from " <<
                                     groupConfiguration_->host(sourceHostId) <<
                                     ", expected from " <<
                                     groupConfiguration_->host(
                                        destinationHostId_);
        return;
    }
    
    applyReply(reply);

    acked_ = true;
    status_ = COMPLETED;
    event_.set();
}

void UnicastRpcRequest::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << *this << "@" << this <<  " timed out, acked=" <<
                                acked_;
    status_ = (status_ == IN_PROGRESS) ? TIMED_OUT : status_;
    event_.set();
}

}  // namespace lightning
