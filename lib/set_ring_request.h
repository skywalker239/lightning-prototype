#pragma once

#include "sync_group_request.h"
#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <set>
#include <vector>

namespace lightning {

//! Attempts to establish a new ring.
class SetRingRequest : public SyncGroupRequest {
public:
    SetRingRequest(const std::vector<Mordor::Address::ptr>& ringHosts,
                   uint64_t ringId);

private:
    const std::string requestString() const;

    void onReply(Mordor::Address::ptr source,
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

    const std::vector<Mordor::Address::ptr> ringHosts_;
    const uint64_t ringId_;
    std::set<Mordor::Address::ptr, AddressCompare> notAcked_;
    Status status_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
