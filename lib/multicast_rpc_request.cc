#include "multicast_rpc_request.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Timer;

static Logger::ptr g_log = Log::lookup("lightning:multicast_rpc_request");

MulticastRpcRequest::MulticastRpcRequest(
    RingConfiguration::const_ptr ring,
    uint64_t timeoutUs)
    : ring_(ring),
      timeoutUs_(timeoutUs),
      notAckedMask_(ring_->ringMask()),
      status_(IN_PROGRESS),
      event_(true)
{}

MulticastRpcRequest::~MulticastRpcRequest()
{}

void MulticastRpcRequest::onReply(Address::ptr sourceAddress,
                                  const RpcMessageData& reply)
{
    FiberMutex::ScopedLock lk(mutex_);
    //MORDOR_ASSERT(status_ == IN_PROGRESS);

    const uint32_t hostId = ring_->replyAddressToId(sourceAddress);
    if(hostId == GroupConfiguration::kInvalidHostId) {
        MORDOR_LOG_WARNING(g_log) << this << " reply from unknown address " <<
                                     *sourceAddress << " to " << *this;
        return;
    }
    if((notAckedMask_ & (1 << hostId)) == 0) {
        MORDOR_LOG_WARNING(g_log) << this << " unexpected reply from " <<
                                     ring_->group()->host(hostId) <<
                                     " to " << *this;
        return;
    }

    MORDOR_LOG_TRACE(g_log) << this << " reply from " <<
                               ring_->group()->host(hostId) << " to " <<
                               *this;
    applyReply(hostId, reply);

    notAckedMask_ &= ~(1 << hostId);
    if(notAckedMask_ == 0) {
        MORDOR_LOG_TRACE(g_log) << this << " got all replies for " << *this;
        status_ = COMPLETED;
        event_.set();
    }
}

void MulticastRpcRequest::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    if(notAckedMask_ == 0) {
        MORDOR_LOG_TRACE(g_log) << this << " onTimeout() with no pending acks";
        status_ = COMPLETED;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " timed out, notAckedMask=" <<
                                   notAckedMask_;
        status_ = TIMED_OUT;
    }
    event_.set();
}

void MulticastRpcRequest::wait() {
    //! XXX Probably should lock here. Then probably not.
    event_.wait();
}

uint64_t MulticastRpcRequest::timeoutUs() const {
    return timeoutUs_;
}

void MulticastRpcRequest::setTimeoutTimer(Timer::ptr timer) {
    FiberMutex::ScopedLock lk(mutex_);
    timeoutTimer_ = timer;
}

void MulticastRpcRequest::cancelTimeoutTimer() {
    FiberMutex::ScopedLock lk(mutex_);

    if(timeoutTimer_.get()) {
        timeoutTimer_->cancel();
        timeoutTimer_.reset();
    } else {
        MORDOR_LOG_WARNING(g_log) << this << " no timeout timer to reset";
    }
}

void MulticastRpcRequest::setRpcGuid(const Guid& guid) {
    FiberMutex::ScopedLock lk(mutex_);
    rpcGuid_ = guid;
    rpcGuid_.serialize(requestData_.mutable_uuid());
}

const Guid& MulticastRpcRequest::rpcGuid() const {
    FiberMutex::ScopedLock lk(mutex_);
    return rpcGuid_;
}

MulticastRpcRequest::Status MulticastRpcRequest::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
