#pragma once

#include "sync_group_requester.h"
#include "ping_tracker.h"
#include <map>

namespace lightning {

//! Multicast pinger.
class PingRequester : public SyncGroupRequester {
public:
    PingRequester(Mordor::Socket::ptr socket,
                  Mordor::Address::ptr pingAddress,
                  const std::vector<Mordor::Address::ptr>& hosts,
                  uint64_t timeoutUs,
                  PingTracker::ptr pingTracker);

    bool ping(uint64_t id);
private:
    virtual bool onReply(Mordor::Address::ptr source,
                         const std::string& reply);

    virtual void onTimeout();

    PingTracker::ptr pingTracker_;
};

}  // namespace lightning
