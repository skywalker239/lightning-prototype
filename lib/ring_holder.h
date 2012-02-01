#pragma once

#include "ring_configuration.h"
#include <boost/shared_ptr.hpp>
#include <mordor/fibersynchronization.h>
#include <string>

namespace lightning {

//! A base class for all things that rely on a valid
//  ring configuration to work.
//  This class is fiber-safe.
class RingHolder {
public:
    typedef boost::shared_ptr<RingHolder> ptr;

    RingHolder();

    virtual ~RingHolder()
    {}

    void resetRingConfiguration(RingConfiguration::const_ptr ringConfiguration);
protected:
    //! Blocks until a valid ring configuration is available.
    RingConfiguration::const_ptr acquireRingConfiguration() const;

    //! Returns immediately with the current ring configuration,
    //  which may be null.
    RingConfiguration::const_ptr tryAcquireRingConfiguration() const;

private:
    //! For debug logging, called under lock.
    std::string configurationToString() const;

    mutable Mordor::FiberMutex mutex_;
    mutable Mordor::FiberEvent ringEvent_;
    RingConfiguration::const_ptr ringConfiguration_;
};

}  // namespace lightning
