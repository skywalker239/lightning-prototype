#pragma once

#include "host_configuration.h"
#include "ping_stats.h"
#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <stdint.h>
#include <map>
#include <string>
#include <vector>

namespace lightning {

/** Tracks multicast ping statistics for a static group of hosts.
  * This class is fiber-safe, but it achieves this using a single
  * global lock.
  */
class PingTracker : boost::noncopyable {
public:
    typedef boost::shared_ptr<PingTracker> ptr;
    //! Latency statistics are tracked over a sliding window of given size.
    //  
    //  A single ping is considered lost if it has not returned within
    //  singlePingTimeoutUs microseconds.
    //
    //  If the maximal send time of all received pongs is
    //  older than noHeartbeatTimeoutUs microseconds ago, then
    //  the host is considered to be down.
    //
    //  noHeartbeatTimeoutUs MUST be greater than singlePingTimeoutUs.
    //
    //  hostDownEvent is signaled in timeoutPing if one or more hosts
    //  go down. PingTracker does not assume ownership over it.
    PingTracker(GroupConfiguration::ptr groupConfiguration,
                uint64_t pingWindowSize,
                uint64_t singlePingTimeoutUs,
                uint64_t noHeartbeatTimeoutUs,
                boost::shared_ptr<Mordor::FiberEvent> hostDownEvent);

    //! Register an outgoing multicast ping.
    void registerPing(uint64_t id, uint64_t sendTime);

    //! Try to timeout the given ping.
    //  The single ping timeout must be less than noHeartbeatTimeout for
    //  the failure detection to work meaningfully.
    //  There is no event loop in PingTracker, so all timeout expirations
    //  are detected by the user periodically invoking timeoutPing().
    void timeoutPing(uint64_t id);

    //! Register a received unicast pong.
    void registerPong(uint32_t hostId,
                      uint64_t id,
                      uint64_t recvTime);
    
    typedef std::map<uint32_t, PingStats> PingStatsMap;

    //! Returns a snapshot of current ping stats.
    void snapshot(PingStatsMap* pingStatsMap) const;

    //! 'host down' timeout.
    uint64_t noHeartbeatTimeoutUs() const;
private:
    std::map<uint32_t, PingStats> perHostPingStats_;

    const uint64_t noHeartbeatTimeoutUs_;

    boost::shared_ptr<Mordor::FiberEvent> hostDownEvent_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
