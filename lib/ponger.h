#pragma once

#include "sync_group_responder.h"
#include <mordor/socket.h>

namespace lightning {

class Ponger : public SyncGroupResponder {
public:
    Ponger(Mordor::Socket::ptr listenSocket,
           Mordor::Address::ptr multicastGroup,
           Mordor::Socket::ptr replySocket);

private:
    virtual bool onRequest(Mordor::Address::ptr sourceAddress,
                           const std::string& request,
                           std::string* reply);
};

}  // namespace lightning
