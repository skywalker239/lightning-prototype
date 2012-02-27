#include "ping_stats.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <algorithm>
#include <cmath>

using Mordor::FiberMutex;
using Mordor::Logger;
using Mordor::Log;
using std::max;

namespace lightning {

const uint64_t PingStats::kNever;
const uint64_t PingStats::Ping::kLostPacketRecvTime;
const uint64_t PingStats::Ping::kPendingPacketRecvTime;
const uint64_t PingStats::Ping::kInvalidPingId;

static Logger::ptr g_log = Log::lookup("lightning:ping_stats");

PingStats::PingStats(int windowSize, uint64_t pingTimeoutUs)
    : pingWindow_(windowSize),
      pingTimeoutUs_(pingTimeoutUs),
      nextPingId_(0),
      maxReceivedPongSendTime_(kNever)
{}

void PingStats::addPing(uint64_t id, uint64_t sendTime) {
    MORDOR_ASSERT(id == nextPingId_);
    MORDOR_LOG_TRACE(g_log) << this << " addPing id=" << id << " sendTime=" <<
                               sendTime;

    Ping& ping = pingWindow_[id % pingWindow_.size()];
    if(ping.id != Ping::kInvalidPingId) {
        if(ping.recvTime == Ping::kPendingPacketRecvTime) {
            MORDOR_LOG_WARNING(g_log) << this << " ping id=" << id <<
                                         " overruns pending id=" << ping.id;
            pendingPingsInWindow_.decrement();
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
    Ping& ping = pingWindow_[id % pingWindow_.size()];
    if(ping.id != id) {
        MORDOR_LOG_WARNING(g_log) << this << " closePing id=" << id <<
                                     " overwritten by id=" << ping.id;
        return;
    }
    if(ping.recvTime != Ping::kPendingPacketRecvTime) {
        MORDOR_LOG_TRACE(g_log) << this << " closePing id=" << id <<
                                     " is not pending";
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
        maxReceivedPongSendTime_ = max(maxReceivedPongSendTime_,
                                       ping.sendTime);
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " closePing id=" << id <<
                                   " lost, latency=" << latency;
        ping.recvTime = Ping::kLostPacketRecvTime;
        lostPingsInWindow_.increment();
    }
}

void PingStats::timeoutPing(uint64_t id) {
    Ping& ping = pingWindow_[id % pingWindow_.size()];
    if(ping.id != id) {
        MORDOR_LOG_WARNING(g_log) << this << " timeoutPing id=" << id <<
                                     " overwritten by id=" << ping.id;
        return;
    }
    if(ping.recvTime != Ping::kPendingPacketRecvTime) {
        MORDOR_LOG_TRACE(g_log) << this << " timeoutPing id=" << id <<
                                     " is not pending";
        return;                      
    }

    ping.recvTime = Ping::kLostPacketRecvTime;
    lostPingsInWindow_.increment();
    pendingPingsInWindow_.decrement();
}

double PingStats::packetLoss() const {
    int64_t lostPingsNumber = lostPingsInWindow_.count;
    int64_t closedPingsNumber = receivedPingsInWindow_.count + lostPingsInWindow_.count;
    return (closedPingsNumber == 0) ? 0.0 : double(lostPingsNumber) / closedPingsNumber;
}

double PingStats::meanLatency() const {
    int64_t receivedPingsNumber = receivedPingsInWindow_.count;
    return (receivedPingsNumber == 0) ? 0.0 : double(sumLatenciesInWindow_.sum) /
                                              receivedPingsNumber;
}

double PingStats::latencyStd() const {
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

uint64_t PingStats::maxReceivedPongSendTime() const {
    return maxReceivedPongSendTime_;
}

}  // namespace lightning
