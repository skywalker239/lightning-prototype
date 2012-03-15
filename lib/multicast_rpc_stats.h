#pragma once

#include <mordor/fibersynchronization.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <list>
#include <stdint.h>

namespace lightning {

class MulticastRpcStats : boost::noncopyable
{
public:
    typedef boost::shared_ptr<MulticastRpcStats> ptr;

    MulticastRpcStats(int sendWindowUs, int recvWindowUs);

    virtual ~MulticastRpcStats()
    {}

    //! Called after a packet has been sent.
    void sentPacket(size_t bytes);

    //! Called after a packet has been received.
    void receivedPacket(size_t bytes);

private:
    struct Packet {
        uint64_t sendTime;
        uint64_t recvTime;
        size_t bytes;
    };

    struct PacketStat {
        uint64_t sumLatency;
        uint64_t packetCount;
        uint64_t lastTime;
        size_t byteCount;
	int windowUs;
        std::list<Packet> window;
    };

    //! Reset all stats to zero.
    void clearStats(PacketStat &stat, int windowUs);

    //! Called when we are adding a new packet to the window.
    void updateStats(PacketStat &stat, size_t bytes);

    PacketStat sent_;
    PacketStat received_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
