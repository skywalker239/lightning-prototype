#include "multicast_rpc_stats.h"
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <mordor/timer.h>
#include <iostream>

namespace lightning {

using Mordor::FiberMutex;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Statistics;
using Mordor::CountStatistic;
using Mordor::TimerManager;

static Logger::ptr g_log = Log::lookup("lightning:multicast_rpc_stats");

static CountStatistic<uint64_t>& g_inWindow =
    Statistics::registerStatistic("multicast_rpc_stats.in_window",
                                  CountStatistic<uint64_t>("us"));
static CountStatistic<uint64_t>& g_outWindow =
    Statistics::registerStatistic("multicast_rpc_stats.out_window",
                                  CountStatistic<uint64_t>("us"));
static CountStatistic<uint64_t>& g_inPackets =
    Statistics::registerStatistic("multicast_rpc_stats.in_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_outPackets =
    Statistics::registerStatistic("multicast_rpc_stats.out_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_inBytes =
    Statistics::registerStatistic("multicast_rpc_stats.in_bytes",
                                  CountStatistic<uint64_t>("bytes"));
static CountStatistic<uint64_t>& g_outBytes =
    Statistics::registerStatistic("multicast_rpc_stats.out_bytes",
                                  CountStatistic<uint64_t>("bytes"));

MulticastRpcStats::MulticastRpcStats(
    int sendWindowUs,
    int recvWindowUs)
{
    clearStats(sent_, sendWindowUs);
    clearStats(received_, recvWindowUs);
}

void MulticastRpcStats::sentPacket(size_t bytes)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateStats(sent_, bytes);

    g_outWindow.reset();
    g_outWindow.add(sent_.sumLatency);
    g_outPackets.reset();
    g_outPackets.add(sent_.packetCount);
    g_outBytes.reset();
    g_outBytes.add(sent_.byteCount);
}

void MulticastRpcStats::receivedPacket(size_t bytes)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateStats(received_, bytes);

    g_inWindow.reset();
    g_inWindow.add(received_.sumLatency);
    g_inPackets.reset();
    g_inPackets.add(received_.packetCount);
    g_inBytes.reset();
    g_inBytes.add(received_.byteCount);
}

void MulticastRpcStats::clearStats(PacketStat &stat, int windowUs)
{
    stat.sumLatency = 0;
    stat.packetCount = 0;
    stat.byteCount = 0;
    stat.lastTime = 0;
    stat.windowUs = windowUs;
}

void MulticastRpcStats::updateStats(PacketStat &stat, size_t bytes)
{
    Packet packet;
    packet.recvTime = TimerManager::now();
    packet.sendTime = (stat.lastTime > 0 ? stat.lastTime : packet.recvTime);
    packet.bytes = bytes;

    if (packet.recvTime < packet.sendTime) {
        MORDOR_LOG_WARNING(g_log) << this << " bad packet times: " <<
                                     packet.recvTime << " < " << packet.sendTime;
        return;
    }

    stat.lastTime = packet.recvTime;
    stat.sumLatency += (packet.recvTime - packet.sendTime);
    stat.packetCount++;
    stat.byteCount += packet.bytes;
    stat.window.push_back(packet);

    // We'll keep our window size not smaller than windowUs, but with minimal packets count.
    while (stat.packetCount > 0) {
        Packet &pop = stat.window.front();
        uint64_t latency = (pop.recvTime - pop.sendTime);
        if (stat.sumLatency < stat.windowUs + latency) break;
        stat.sumLatency -= latency;
        stat.packetCount--;
        stat.byteCount -= pop.bytes;
        stat.window.pop_front();
    }
}

}  // namespace lightning
