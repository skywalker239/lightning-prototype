#pragma once

#include "group_configuration.h"
#include "ping_tracker.h"
#include "ring_configuration.h"
#include "rpc_requester.h"
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
           RpcRequester::ptr requester,
           GroupConfiguration::ptr groupConfiguration,
           uint64_t pingIntervalUs,
           uint64_t pingTimeoutUs,
           PingTracker::ptr pingTracker);
    
    void run();

private:
    void doSinglePing(uint64_t id);

    Mordor::IOManager* ioManager_;
    RpcRequester::ptr requester_;
    RingConfiguration::const_ptr pingRing_;
    const uint64_t pingIntervalUs_;
    const uint64_t pingTimeoutUs_;
    PingTracker::ptr pingTracker_;
    uint64_t currentId_;
};

}  // namespace lightning
