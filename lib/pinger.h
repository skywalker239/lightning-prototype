#pragma once

#include "ping_tracker.h"
#include "sync_group_requester.h"
#include <mordor/socket.h>
#include <mordor/scheduler.h>
#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>
#include <vector>

namespace lightning {

class Pinger : boost::noncopyable,
               public boost::enable_shared_from_this<Pinger>
{
public:
    typedef boost::shared_ptr<Pinger> ptr;

    //! Sends multicast pings to pingAddress once in pingIntervalUs us,
    //  collects replies from the given hosts using the supplied PingTracker.
    Pinger(Mordor::IOManager* ioManager,
           SyncGroupRequester::ptr requester,
           const std::vector<Mordor::Address::ptr>& hosts,
           uint64_t pingIntervalUs,
           PingTracker::ptr pingTracker);
    
    void run();

private:
    void doSinglePing(uint64_t id);

    Mordor::IOManager* ioManager_;
    SyncGroupRequester::ptr requester_;
    const std::vector<Mordor::Address::ptr> hosts_;
    const uint64_t pingIntervalUs_;
    PingTracker::ptr pingTracker_;
    uint64_t currentId_;
};

}  // namespace lightning
