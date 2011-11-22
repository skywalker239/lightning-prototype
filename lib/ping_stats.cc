#include "ping_stats.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <cmath>

using Mordor::FiberMutex;
using Mordor::Logger;
using Mordor::Log;

namespace lightning {

static Logger::ptr g_log = Log::lookup("lightning:ping_stats");

PingStats::PingStats(int windowSize, uint64_t pingTimeoutUs)
    : pingWindow_(windowSize),
      pingTimeoutUs_(pingTimeoutUs),
      nextPingId_(0),
      lowestMaybeOpenId_(0),
      firstTrailingLostId_(0)
{}

void PingStats::addPing(uint64_t id, uint64_t sendTime) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_ASSERT(id == nextPingId_);
    MORDOR_LOG_TRACE(g_log) << this << " addPing id=" << id << " sendTime=" <<
                               sendTime;

    Ping& ping = pingWindow_[id % pingWindow_.size()];
    if(ping.id != Ping::kInvalidPingId) {
        if(ping.recvTime == Ping::kPendingPacketRecvTime) {
            MORDOR_LOG_WARNING(g_log) << this << " ping id=" << id <<
                                         " overruns pending id=" << ping.id;
            pendingPingsInWindow_.decrement();
            if(ping.id == lowestMaybeOpenId_) {
                ++lowestMaybeOpenId_;
            }
        } else if(ping.recvTime == Ping::kLostPacketRecvTime) {
            lostPingsInWindow_.decrement();
        } else {
            const int64_t latency = ping.recvTime - ping.sendTime;
            sumLatenciesInWindow_.add(- latency);
            sumSquaredLatenciesInWindow_.add(- latency * latency);
            receivedPingsInWindow_.decrement();
        }
    }
    ping.id = id;
    ping.sendTime = sendTime;
    ping.recvTime = Ping::kPendingPacketRecvTime;
    pendingPingsInWindow_.increment();

    ++nextPingId_;
}

void PingStats::closePing(uint64_t id, uint64_t recvTime) {
    FiberMutex::ScopedLock lk(mutex_);

    Ping& ping = pingWindow_[id % pingWindow_.size()];
    if(ping.id != id) {
        MORDOR_LOG_WARNING(g_log) << this << " closePing id=" << id <<
                                     " overwritten by id=" << ping.id;
        return;
    }

    pendingPingsInWindow_.decrement();
    const int64_t latency = recvTime - ping.sendTime;
    MORDOR_ASSERT(latency >= 0);
    if(uint64_t(latency) <= pingTimeoutUs_) {
        MORDOR_LOG_TRACE(g_log) << this << " closePing id=" << id <<
                                   " latency=" << latency;
        ping.recvTime = recvTime;
        receivedPingsInWindow_.increment();
        sumLatenciesInWindow_.add(latency);
        sumSquaredLatenciesInWindow_.add(latency * latency);
        if(id >= firstTrailingLostId_) {
            firstTrailingLostId_ = id + 1;
        }
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " closePing id=" << id <<
                                   " lost, latency=" << latency;
        ping.recvTime = Ping::kLostPacketRecvTime;
        lostPingsInWindow_.increment();
    }

    if(id == lowestMaybeOpenId_) {
        ++lowestMaybeOpenId_;
    }
}

void PingStats::markLosses(uint64_t sendTimeThreshold) {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_ASSERT(lowestMaybeOpenId_ <= nextPingId_);

    for(uint64_t id = lowestMaybeOpenId_; id < nextPingId_; ++id) {
        Ping& ping = pingWindow_[id % pingWindow_.size()];
        if(ping.id != id) {
            MORDOR_LOG_WARNING(g_log) << this << " markLosses trying id=" <<
                                         id << " got id=" << ping.id;
            continue;
        }
        if(ping.recvTime == Ping::kPendingPacketRecvTime &&
           ping.sendTime < sendTimeThreshold)
        {
            MORDOR_LOG_TRACE(g_log) << this << " markLosses threshold=" <<
                                       sendTimeThreshold <<
                                       " marking ping id=" << id << " lost";
            ping.recvTime = Ping::kLostPacketRecvTime;
            lostPingsInWindow_.increment();
            pendingPingsInWindow_.decrement();
            if(id == lowestMaybeOpenId_) {
                ++lowestMaybeOpenId_;
            }
        } else if(ping.recvTime != Ping::kPendingPacketRecvTime) {
            if(id == lowestMaybeOpenId_) {
                ++lowestMaybeOpenId_;
            }
        }
    }
}

uint64_t PingStats::nextPingId() const {
    FiberMutex::ScopedLock lk(mutex_);
    return nextPingId_;
}

double PingStats::packetLoss() const {
    FiberMutex::ScopedLock lk(mutex_);

    int64_t lostPingsNumber = lostPingsInWindow_.count;
    int64_t closedPingsNumber = receivedPingsInWindow_.count + lostPingsInWindow_.count;
    return (closedPingsNumber == 0) ? 0.0 : double(lostPingsNumber) / closedPingsNumber;
}

double PingStats::meanLatency() const {
    FiberMutex::ScopedLock lk(mutex_);

    int64_t receivedPingsNumber = receivedPingsInWindow_.count;
    return (receivedPingsNumber == 0) ? 0.0 : double(sumLatenciesInWindow_.sum) /
                                              receivedPingsNumber;
}

double PingStats::latencyStd() const {
    FiberMutex::ScopedLock lk(mutex_);

    int64_t receivedPingsNumber = receivedPingsInWindow_.count;
    if(receivedPingsNumber == 0) {
        return 0.0;
    } else {
        double averageLatency = double(sumLatenciesInWindow_.sum) / receivedPingsNumber;
        double averageSquaredLatency = double(sumSquaredLatenciesInWindow_.sum) /
                                       receivedPingsNumber;
        return averageSquaredLatency - averageLatency * averageLatency;
    }
}

uint64_t PingStats::trailingLostPingsNumber() const {
    FiberMutex::ScopedLock lk(mutex_);

    return nextPingId_ - firstTrailingLostId_;
}   

}  // namespace lightning
