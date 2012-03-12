#include "rpc_request.h"

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::Timer;

RpcRequest::RpcRequest(Address::ptr destination,
                       uint64_t timeoutUs)
    : status_(IN_PROGRESS),
      event_(true),
      destination_(destination),
      timeoutUs_(timeoutUs)
{}

void RpcRequest::wait() {
    event_.wait();
    //! XXX lock here?
}

void RpcRequest::setTimeoutTimer(const Timer::ptr& timer) {
    FiberMutex::ScopedLock lk(mutex_);
    timeoutTimer_ = timer;
}

void RpcRequest::cancelTimeoutTimer() 
{
    FiberMutex::ScopedLock lk(mutex_);

    if(timeoutTimer_.get()) {
        timeoutTimer_->cancel();
        timeoutTimer_.reset();
    }
}

void RpcRequest::setRpcGuid(const Guid& guid) {
    FiberMutex::ScopedLock lk(mutex_);
    rpcGuid_ = guid;
    rpcGuid_.serialize(requestData_.mutable_uuid());
}

const Guid& RpcRequest::rpcGuid() const {
    FiberMutex::ScopedLock lk(mutex_);
    return rpcGuid_;
}

RpcRequest::Status RpcRequest::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
