#pragma once

#include "ping_tracker.h"
#include <mordor/socket.h>
#include <vector>

namespace lightning {

//! An interface to choose the best ring using ping statistics.
class RingOracle {
public:
    //! False if no suitable ring can be established.
    virtual bool chooseRing(const PingTracker::PingStatsMap& pingStatsMap,
                            std::vector<Mordor::Address::ptr>* ring) const = 0;
};
                            
}  // namespace lightning
