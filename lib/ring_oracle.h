#pragma once

#include "ping_tracker.h"
#include <boost/noncopyable.hpp>
#include <string>
#include <vector>

namespace lightning {

//! An interface to choose the best ring using ping statistics.
//  A ring is represented by an ordered list of host ids.
class RingOracle : boost::noncopyable {
public:
    //! False if no suitable ring can be established.
    virtual bool chooseRing(const PingTracker::PingStatsMap& pingStatsMap,
                            std::vector<uint32_t>* ring) const = 0;

    typedef boost::shared_ptr<RingOracle> ptr;
};
                            
}  // namespace lightning
