#include "ring_holder.h"
#include <mordor/log.h>
#include <sstream>

namespace lightning {

using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using std::string;
using std::ostringstream;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ring_holder");

RingHolder::RingHolder()
    : ringEvent_(false)
{}

void RingHolder::resetRingConfiguration(
    RingConfiguration::const_ptr ringConfiguration)
{
    FiberMutex::ScopedLock lk(mutex_);
    ringConfiguration_ = ringConfiguration;
    MORDOR_LOG_TRACE(g_log) << this << " reset ring configuration to " <<
                               configurationToString();
    if(ringConfiguration_) {
        ringEvent_.set();
    } else {
        ringEvent_.reset();
    }
}

RingConfiguration::const_ptr RingHolder::acquireRingConfiguration() const {
    while(true) {
        ringEvent_.wait();
        FiberMutex::ScopedLock lk(mutex_);
        if(!ringConfiguration_) {
            continue;
        }
        MORDOR_LOG_TRACE(g_log) << this << " acquired ring configuration " <<
                                   configurationToString();
        return ringConfiguration_;
    }
}

RingConfiguration::const_ptr RingHolder::tryAcquireRingConfiguration() const {
    FiberMutex::ScopedLock lk(mutex_);
    MORDOR_LOG_TRACE(g_log) << this << " tryAcquireRingConfiguration = " <<
                               configurationToString();
    return ringConfiguration_;
}

string RingHolder::configurationToString() const {
    ostringstream ss;
    if(!ringConfiguration_) {
        ss << "(null)";
    } else {
        ss << *ringConfiguration_;
    }
    return ss.str();
}

}  // namespace lightning
