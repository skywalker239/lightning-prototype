#pragma once

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
    void sentPacket(uint64_t sendTime, uint64_t recvTime, size_t bytes);

    //! Called after a packet has been received.
    void receivedPacket(uint64_t recvTime, size_t bytes);

private:
    struct Packet {
        uint64_t sendTime;
        uint64_t recvTime;
	size_t bytes;
    };

    struct PacketStat {
	uint64_t sum_latency;
        uint64_t packet_count;
        size_t byte_count;
        std::list<Packet> window;
    };

    //! Called when we are adding a new packet to the window.
    void updateStats(PacketStat &stat, Packet &packet, int windowUs);

    PacketStat sent_;
    PacketStat received_;

    int sendWindowUs_;
    int recvWindowUs_;
    uint64_t lastRecvTime_;
};

}  // namespace lightning
