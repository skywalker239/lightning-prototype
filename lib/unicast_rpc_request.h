#pragma once

#include "host_configuration.h"
#include "rpc_request.h"
#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <mordor/timer.h>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <string>

namespace lightning {

//! A unicast RPC request.
class UnicastRpcRequest : public RpcRequest {
public:
    typedef boost::shared_ptr<UnicastRpcRequest> ptr;

    UnicastRpcRequest(GroupConfiguration::ptr groupConfiguration,
                      uint32_t destinationHostId,
                      uint64_t timeoutUs);

private:
    void onReply(const Mordor::Address::ptr& sourceAddress,
                 const RpcMessageData& reply);

    //! Request-specific logic goes here.
    virtual void applyReply(const RpcMessageData& reply) = 0;

    //! For debug logging.
    virtual std::ostream& output(std::ostream& os) const = 0;

    //! Sets the status to TIMED_OUT and releases the waiter.
    virtual void onTimeout();

private:
    GroupConfiguration::ptr groupConfiguration_;
    const uint32_t destinationHostId_;
    bool acked_;
};

}  // namespace lightning
