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
	memset(&sent_, 0, sizeof(sent_));
	memset(&received_, 0, sizeof(received_));
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
    stat.sum_latency += (packet.recvTime - packet.sendTime);
    stat.packet_count++;
    stat.byte_count += packet.bytes;
    stat.window.push_back(packet);

    // We'll keep our window size not smaller than windowUs, but with minimal packets count.
    while (stat.window.size() > 0) {
        Packet &pop = stat.window.front();
	uint64_t latency = (pop.recvTime - pop.sendTime);
        if (stat.sum_latency < windowUs + latency) break;
        stat.sum_latency -= latency;
        stat.packet_count--;
        stat.byte_count -= pop.bytes;
        stat.window.pop_front();
    }
}

}  // namespace lightning
