#pragma once

#include "proto/rpc_messages.pb.h"
#include "ring_configuration.h"
#include <mordor/fibersynchronization.h>
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

    //! This request will await responses from all acceptors
    //  of ring (except itself) and have a timeout of timeoutUs.
    MulticastRpcRequest(RingConfiguration::const_ptr ring,
                        uint64_t timeoutUs);

    virtual ~MulticastRpcRequest();

    //! If this address belongs to the request ring,
    //  passes the reply to applyReply(), then marks
    //  that sourceAddress has responded. If it
    //  was the last needed ack, releases the wait()'er.
    void onReply(Mordor::Address::ptr sourceAddress,
                 const RpcMessageData& reply);

    //! Request-specific logic goes here.
    virtual void applyReply(uint32_t hostId,
                            const RpcMessageData& reply) = 0;

    //! The serialized request to transmit over the network.
    virtual const RpcMessageData& request() const = 0;

    //! Sets the status to TIMED_OUT and releases the waiter.
    void onTimeout();

    //! Blocks the caller until all necessary replies are collected
    //  or a timeout happens.
    void wait();

    //! The timeout for this request, in microseconds.
    uint64_t timeoutUs() const;

    //! Current status. 
    Status status() const;
private:
    RingConfiguration::const_ptr ring_;
    const uint64_t timeoutUs_;
    uint64_t notAckedMask_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
