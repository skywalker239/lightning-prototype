#pragma once

#include "proto/rpc_messages.pb.h"
#include <mordor/socket.h>
#include <boost/shared_ptr.hpp>
#include <string>

namespace lightning {

//! An abstract command to be executed synchronously on several hosts.
//  See MulticastRpcRequester for the description of its semantics.
//  An implementation must be fiber-safe since onReply and
//  onTimeout may in principle be called concurrently (and will be
//  certainly called concurrently with wait()).
class MulticastRpcRequest {
public:
    typedef boost::shared_ptr<MulticastRpcRequest> ptr;

    enum Status {
        IN_PROGRESS,
        COMPLETED,
        TIMED_OUT
    };

    virtual ~MulticastRpcRequest() {}

    //! The serialized request to transmit over the network.
    virtual const RpcMessageData& request() const = 0;

    //! Registers a reply from a certain address.
    //  Releases the wait()'er if this reply is enough for the
    //  request to be considered completed.
    virtual void onReply(Mordor::Address::ptr sourceAddress,
                         const RpcMessageData& reply) = 0;

    //! Registers that a timeout occurred.
    //  The implementation should allow this to be called
    //  even after all the necessary acks have been received.
    virtual void onTimeout() = 0;

    //! Should block the calling fiber until all necessary
    //  acks have been collected or a nack/timeout happened.
    virtual void wait() = 0;

    //! The timeout for this request, in microseconds.
    virtual uint64_t timeoutUs() const = 0;

    //! Current status. 
    virtual Status status() const = 0;
};

}  // namespace lightning
