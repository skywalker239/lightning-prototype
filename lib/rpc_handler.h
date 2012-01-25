#pragma once

#include "proto/rpc_messages.pb.h"
#include <mordor/socket.h>

namespace lightning {

class RpcHandler {
public:
    typedef boost::shared_ptr<RpcHandler> ptr;

    virtual ~RpcHandler() {}

    //! Return false if no reply should be sent.
    virtual bool handleRequest(Mordor::Address::ptr sourceAddress,
                               const RpcMessageData& request,
                               RpcMessageData* reply) = 0;
};

}  // namespace lightning
