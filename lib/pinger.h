#pragma once

#include "ping_tracker.h"
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
           Mordor::Socket::ptr socket,
           Mordor::Address::ptr pingAddress,
           const std::vector<Mordor::Address::ptr>& hosts,
           uint64_t pingIntervalUs,
           uint64_t pingTimeoutUs,
           PingTracker::ptr pingTracker);
    
    void run();

private:
    static void doSinglePing(Pinger::ptr pinger, uint64_t id);

    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr socket_;
    Mordor::Address::ptr pingAddress_;
    const std::vector<Mordor::Address::ptr> hosts_;
    const uint64_t pingIntervalUs_;
    const uint64_t pingTimeoutUs_;
    PingTracker::ptr pingTracker_;
    uint64_t currentId_;
};

}  // namespace lightning
