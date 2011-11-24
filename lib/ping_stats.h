#pragma once

#include <mordor/fibersynchronization.h>
#include <mordor/statistics.h>
#include <vector>

namespace lightning {

//! Ping statistics for one host.
//  This class is not (fiber|thread)-safe.
class PingStats {
public:
    PingStats(int windowSize, uint64_t pingTimeoutUs);

    void addPing(uint64_t id, uint64_t sendTime);

    void closePing(uint64_t id, uint64_t recvTime);

    void timeoutPing(uint64_t id);

    double packetLoss() const;

    double meanLatency() const;

    double latencyStd() const;

    uint64_t maxReceivedPongSendTime() const;

    static const uint64_t kNever = 0;

private:
    struct Ping {
        uint64_t id;
        uint64_t sendTime;
        uint64_t recvTime;

        static const uint64_t kLostPacketRecvTime = 0xFFFFFFFFFFFFFFFF;
        static const uint64_t kPendingPacketRecvTime = 0;
        static const uint64_t kInvalidPingId = 0xFFFFFFFFFFFFFFFF;

        Ping()
            : id(kInvalidPingId),
              sendTime(0),
              recvTime(0)
        {}

        Ping(uint64_t _id, uint64_t _sendTime, uint64_t _recvTime)
            : id(_id),
              sendTime(_sendTime),
              recvTime(_recvTime)
        {}
    } __attribute__((packed));

    std::vector<Ping> pingWindow_;
    const uint64_t pingTimeoutUs_;

    //! All pings so far were for the id range [0, nextPingId_).
    uint64_t nextPingId_;
    //! Maximal send time of all received pongs.
    uint64_t maxReceivedPongSendTime_;

    Mordor::SumStatistic<int64_t> sumLatenciesInWindow_;
    Mordor::SumStatistic<int64_t> sumSquaredLatenciesInWindow_;
    Mordor::CountStatistic<int64_t> lostPingsInWindow_;
    Mordor::CountStatistic<int64_t> receivedPingsInWindow_;
    Mordor::CountStatistic<int64_t> pendingPingsInWindow_;
};

}  // namespace lightning
