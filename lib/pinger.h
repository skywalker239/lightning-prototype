#pragma once

#include "ping_stats.h"
#include <mordor/socket.h>
#include <mordor/scheduler.h>
#include <boost/bind.hpp>
#include <map>

namespace lightning {

class Pinger {
public:
    Pinger(Mordor::IOManager* ioManager,
           Mordor::Socket::ptr socket,
           Mordor::Address::ptr destinationAddress,
           uint64_t pingIntervalUs,
           uint64_t pingTimeoutUs,
           boost::function<void (uint64_t, uint64_t)> pingRegisterCallback,
           boost::function<void (uint64_t)> pingTimeoutCallback);
    
    void run();

private:
    void setupSocket();

    void waitAndTimeoutPing(uint64_t id);

    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr socket_;
    Mordor::Address::ptr destinationAddress_;
    const uint64_t pingIntervalUs_;
    const uint64_t pingTimeoutUs_;
    boost::function<void (uint64_t, uint64_t)> pingRegisterCallback_;
    boost::function<void (uint64_t)> pingTimeoutCallback_;
    uint64_t currentId_;
};

}  // namespace lightning
