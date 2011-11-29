#pragma once

#include "ping_tracker.h"
#include <mordor/socket.h>
#include <boost/noncopyable.hpp>
#include <vector>

namespace lightning {

//! An interface to choose the best ring using ping statistics.
class RingOracle : boost::noncopyable {
public:
    //! False if no suitable ring can be established.
    virtual bool chooseRing(const PingTracker::PingStatsMap& pingStatsMap,
                            std::vector<Mordor::Address::ptr>* ring) const = 0;

    typedef boost::shared_ptr<RingOracle> ptr;
};
                            
}  // namespace lightning
