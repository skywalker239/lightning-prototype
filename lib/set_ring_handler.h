#pragma once

#include "host_configuration.h"
#include "guid.h"
#include "ring_change_notifier.h"
#include "rpc_handler.h"

namespace lightning {

class SetRingHandler : public RpcHandler {
public:
    //! GroupConfiguration is needed to extract the master address.
    SetRingHandler(const Guid& hostGroupGuid,
                   RingChangeNotifier::ptr ringChangeNotifier,
                   const GroupConfiguration& groupConfiguration);
private:
    bool handleRequest(Mordor::Address::ptr sourceAddress,
                       const RpcMessageData& request,
                       RpcMessageData* reply);

    const Guid hostGroupGuid_;
    const RingChangeNotifier::ptr ringChangeNotifier_;
    Mordor::Address::ptr masterAddress_;
};

}  // namespace lightning
