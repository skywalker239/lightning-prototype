#pragma once

#include "rpc_handler.h"
#include "acceptor_state.h"

namespace lightning {

class RecoveryHandler : public RpcHandler {
public:
    RecoveryHandler(AcceptorState::ptr acceptorState);
private:
    bool handleRequest(Mordor::Address::ptr sourceAddress,
                       const RpcMessageData& request,
                       RpcMessageData* reply);

    AcceptorState::ptr acceptorState_;
};

}  // namespace lightning
