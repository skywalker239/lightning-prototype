#pragma once

#include "rpc_request.h"
#include "ring_configuration.h"
#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <mordor/timer.h>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <string>

namespace lightning {

//! An abstract command to be executed synchronously on several hosts.
//  See MulticastRpcRequester for the description of its semantics.
//  An implementation must be fiber-safe since onReply and
//  onTimeout may in principle be called concurrently (and will be
//  certainly called concurrently with wait()).
class MulticastRpcRequest : public RpcRequest {
public:
    typedef boost::shared_ptr<MulticastRpcRequest> ptr;

    //! This request will await responses from all acceptors
    //  of ring (except itself) and have a timeout of timeoutUs.
    MulticastRpcRequest(RingConfiguration::const_ptr ring,
                        uint64_t timeoutUs);

private:
    //! If this address belongs to the request ring,
    //  passes the reply to applyReply(), then marks
    //  that sourceAddress has responded. If it
    //  was the last needed ack, releases the wait()'er.
    void onReply(const Mordor::Address::ptr& sourceAddress,
                 const RpcMessageData& reply);

    //! Request-specific logic goes here.
    virtual void applyReply(uint32_t hostId,
                            const RpcMessageData& reply) = 0;

    //! For debug logging.
    virtual std::ostream& output(std::ostream& os) const = 0;

    //! Sets the status to TIMED_OUT and releases the waiter.
    virtual void onTimeout();

private:
    RingConfiguration::const_ptr ring_;
    uint64_t notAckedMask_;
};

}  // namespace lightning
