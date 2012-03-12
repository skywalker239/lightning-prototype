#pragma once

#include "guid.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <mordor/timer.h>
#include <boost/shared_ptr.hpp>
#include <iostream>

namespace lightning {

//! An abstract RPC request, contains common code for both the multicast
//  and the unicast cases.
class RpcRequest {
public:
    typedef boost::shared_ptr<RpcRequest> ptr;

    enum Status {
        IN_PROGRESS,
        COMPLETED,
        TIMED_OUT
    };

    RpcRequest(Mordor::Address::ptr destination,
               uint64_t timeoutUs);

    virtual ~RpcRequest();

    //! Process a reply from some address.
    virtual void onReply(const Mordor::Address::ptr& sourceAddress,
                         const RpcMessageData& reply) = 0;

    //! The request protocol buffer, ready for serialization.
    const RpcMessageData* requestData() const { return &requestData_; }

    //! The request destination.
    const Mordor::Address::ptr& destination() const { return destination_; }

    //! For debugging.
    virtual std::ostream& output(std::ostream& os) const = 0;

    //! Must set status to TIMED_OUT and release the waiter.
    virtual void onTimeout() = 0;

    //! Blocks the caller until all the necessary replies are collected or
    //  the request times out.
    void wait();

    //! The timeout for this request in microseconds.
    uint64_t timeoutUs() const { return timeoutUs_; }

    //! Set the timer that tracks the timeout expiration for this request
    //  so it can be canceled later.
    void setTimeoutTimer(const Mordor::Timer::ptr& timer);

    //! Cancel the pending timeout timer (if any).
    void cancelTimeoutTimer();

    //! Set the RPC GUID for this request.
    void setRpcGuid(const Guid& guid);

    const Guid& rpcGuid() const;

    //! Current status.
    Status status() const;

protected:
    //! An implementation must fill all needed fields in its constructor.
    RpcMessageData requestData_;
    Status status_;

    //! An implementation sets event_ whenever it is required to release
    //  the waiter (if any), i.e. when all needed replies are collected or
    //  upon timeout.
    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;

private:
    const Mordor::Address::ptr destination_;
    const uint64_t timeoutUs_;
    Mordor::Timer::ptr timeoutTimer_;
    Guid rpcGuid_;
};

inline
std::ostream& operator<<(std::ostream& os, const RpcRequest& request) {
    return request.output(os);
}

}  // namespace lightning
