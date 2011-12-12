#pragma once

#include "sync_group_requester.h"
#include <mordor/socket.h>
#include <vector>

namespace lightning {

//! Attempts to establish a new ring.
class RingSetter : public SyncGroupRequester {
public:
    RingSetter(Mordor::Socket::ptr socket,
               Mordor::Address::ptr ringMulticastAddress,
               const std::vector<Mordor::Address::ptr>& ringHosts,
               uint64_t ringId,
               uint64_t timeoutUs);

    bool setRing();
private:
    virtual bool onReply(Mordor::Address::ptr source,
                         const std::string& reply);

    virtual void onTimeout();

    const std::vector<Mordor::Address::ptr> ringHosts_;
    const uint64_t ringId_;
};

}  // namespace lightning
