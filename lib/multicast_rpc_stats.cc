#include "multicast_rpc_stats.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Logger;
using Mordor::Log;

static Logger::ptr g_log = Log::lookup("lightning:multicast_rpc_stats");

MulticastRpcStats::MulticastRpcStats(
    int sendWindowUs,
    int recvWindowUs)
    : sendWindowUs_(sendWindowUs),
      recvWindowUs_(recvWindowUs)
{
	lastRecvTime_ = 0;
}

void MulticastRpcStats::sentPacket(uint64_t sendTime, uint64_t recvTime, size_t bytes)
{
    if (sent_.window.size() > 0 && sendTime > sent_.window.back().recvTime) {
        MORDOR_LOG_WARNING(g_log) << this << " received packets are interferred: " <<
                                     sendTime << "/" << recvTime << " vs. " <<
				     sent_.window.back().recvTime;
        return;
    }

    if (recvTime < sendTime) {
        MORDOR_LOG_WARNING(g_log) << this << " bad packet times: " <<
                                     sendTime << "/" << recvTime;
        return;
    }

    // For outgoing packets we know the exact send/recv times, excluding flow control delays.
    Packet packet;
    packet.sendTime = sendTime;
    packet.recvTime = recvTime;
    packet.bytes = bytes;

    updateStats(sent_, packet, sendWindowUs_);
}

void MulticastRpcStats::receivedPacket(uint64_t recvTime, size_t bytes)
{
    if (recvTime > lastRecvTime_) {
        MORDOR_LOG_WARNING(g_log) << this << " bad packet time: " << recvTime;
        return;
    }

    // For incoming packets we can only count raw stats over some period, with send times unknown.
    Packet packet;
    packet.sendTime = (lastRecvTime_ > 0 ? lastRecvTime_ : recvTime);
    packet.recvTime = recvTime;
    packet.bytes = bytes;

    lastRecvTime_ = recvTime;
    updateStats(received_, packet, recvWindowUs_);
}

void MulticastRpcStats::updateStats(PacketStat &stat, Packet &packet, int windowUs)
{
    // We need to keep the window non-empty, so we'll only clean it up when we got a non-instant packet.
    if (packet.sendTime > packet.recvTime)
        while (stat.window.size() > 0 && stat.window.front().sendTime + windowUs < packet.sendTime) {
            Packet &pop = stat.window.front();
	    stat.sum_latency -= (pop.recvTime - pop.sendTime);
	    stat.packet_count--;
	    stat.byte_count -= pop.bytes;
            stat.window.pop_front();
	}

    stat.sum_latency += (packet.recvTime - packet.sendTime);
    stat.packet_count++;
    stat.byte_count += packet.bytes;
    stat.window.push_back(packet);
}

}  // namespace lightning
