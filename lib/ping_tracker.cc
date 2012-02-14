#include "ping_tracker.h"
#include <mordor/log.h>
#include <mordor/timer.h>
#include <algorithm>

namespace lightning {

using Mordor::FiberMutex;
using Mordor::FiberEvent;
using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using Mordor::TimerManager;
using std::make_pair;
using std::map;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ping_tracker");

PingTracker::PingTracker(GroupConfiguration::ptr groupConfiguration,
                         uint64_t pingWindowSize,
                         uint64_t singlePingTimeoutUs,
                         uint64_t noHeartbeatTimeoutUs,
                         boost::shared_ptr<FiberEvent> hostDownEvent)
    : noHeartbeatTimeoutUs_(noHeartbeatTimeoutUs),
      hostDownEvent_(hostDownEvent)
{
    MORDOR_LOG_TRACE(g_log) << this << " creating tracker for " <<
                               groupConfiguration->size() - 1<<
                               " hosts, ping window=" <<
                               pingWindowSize << ", single ping timeout=" <<
                               singlePingTimeoutUs << ", heartbeat timeout=" <<
                               noHeartbeatTimeoutUs;
    for(size_t i = 0; i < groupConfiguration->size(); ++i) {
        if(i != groupConfiguration->thisHostId()) {
            perHostPingStats_.insert(make_pair(i,
                                               PingStats(pingWindowSize,
                                                         singlePingTimeoutUs)));
        }
    }
}

void PingTracker::registerPing(uint64_t id, uint64_t sendTime) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << this << " new ping id=" << id <<
                               " sendTime=" << sendTime;
    for(auto i = perHostPingStats_.begin();
        i != perHostPingStats_.end();
        ++i)
   {
       i->second.addPing(id, sendTime);
   }
}

void PingTracker::registerPong(uint32_t hostId,
                               uint64_t id,
                               uint64_t recvTime)
{
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << this << " got pong from " <<
                               hostId << " (" << id <<
                               ", " << recvTime << ")";
    auto i = perHostPingStats_.find(hostId);
    if(i == perHostPingStats_.end()) {
        MORDOR_LOG_WARNING(g_log) << this << " pong from unknown host " <<
                                     hostId;
        return;
    }
    i->second.closePing(id, recvTime);
}

void PingTracker::timeoutPing(uint64_t id) {
    FiberMutex::ScopedLock lk(mutex_);

    const uint64_t now = TimerManager::now();
    MORDOR_LOG_TRACE(g_log) << this << " timing out ping id=" << id <<
                               ", now=" << now;

    for(auto i = perHostPingStats_.begin();
        i != perHostPingStats_.end();
        ++i)
    {
        i->second.timeoutPing(id);
        if(i->second.maxReceivedPongSendTime() + noHeartbeatTimeoutUs_ < now)
        {
            MORDOR_LOG_WARNING(g_log) << this << " " << i->first <<
                                         " is down";
            hostDownEvent_->set();
        }
    }
}

void PingTracker::snapshot(PingStatsMap* pingStatsMap) const {
    FiberMutex::ScopedLock lk(mutex_);
    *pingStatsMap = perHostPingStats_;
}

uint64_t PingTracker::noHeartbeatTimeoutUs() const {
    return noHeartbeatTimeoutUs_;
}

}  // namespace lightning
