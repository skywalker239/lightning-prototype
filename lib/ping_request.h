#pragma once

#include "sync_group_request.h"
#include "ping_tracker.h"
#include <mordor/fibersynchronization.h>
#include <set>
#include <vector>

namespace lightning {

//! A single multicast ping.
class PingRequest : public SyncGroupRequest {
public:
    PingRequest(const std::vector<Mordor::Address::ptr>& hosts,
                uint64_t pingId,
                PingTracker::ptr pingTracker);

private:
    const std::string requestString() const;

    void onReply(Mordor::Address::ptr sourceAddress,
                 const std::string& reply);

    void onTimeout();

    void wait();

    Status status() const;

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                       const Mordor::Address::ptr& rhs) const
        {
            return *lhs < *rhs;
        }
    };

    const uint64_t pingId_;
    PingTracker::ptr pingTracker_;
    std::set<Mordor::Address::ptr, AddressCompare> notAcked_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
