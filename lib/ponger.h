#pragma once

#include "rpc_handler.h"
#include <mordor/socket.h>

namespace lightning {

class Ponger : public RpcHandler {
private:
    virtual bool handleRequest(Mordor::Address::ptr sourceAddress,
                           const RpcMessageData& request,
                           RpcMessageData* reply);
};

}  // namespace lightning
