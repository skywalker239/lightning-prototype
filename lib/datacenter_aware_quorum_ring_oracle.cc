#include "datacenter_aware_quorum_ring_oracle.h"
#include <mordor/log.h>
#include <mordor/timer.h>
#include <algorithm>
#include <set>

namespace lightning {

using Mordor::Logger;
using Mordor::Log;
using Mordor::TimerManager;
using std::string;
using std::vector;
using std::map;
using std::pair;
using std::make_pair;
using std::sort;
using std::set;

static Logger::ptr g_log = Log::lookup("lightning:ring_oracle");

DatacenterAwareQuorumRingOracle::DatacenterAwareQuorumRingOracle(
    const GroupConfiguration& groupConfiguration,
    bool okToMissDatacenter,
    uint64_t noHeartbeatTimeoutUs)
    : okToMissDatacenter_(okToMissDatacenter),
      noHeartbeatTimeoutUs_(noHeartbeatTimeoutUs),
      nextDatacenterId_(0)
{
    // XXX For now only allow creating this on master.
    MORDOR_ASSERT(groupConfiguration.thisHostId() ==
                  groupConfiguration.masterId());
    MORDOR_LOG_TRACE(g_log) << this << " okToMiss=" << okToMissDatacenter_ <<
                               " timeout=" << noHeartbeatTimeoutUs_;
    const vector<HostConfiguration>& hosts = groupConfiguration.hosts();
    for(size_t i = 0; i < hosts.size(); ++i) {
        const uint32_t dcId = datacenterId(hosts[i].datacenter);
        acceptorToDatacenterId_[i] = dcId;
        MORDOR_LOG_TRACE(g_log) << this << " host " << i << " is in dc " << dcId;
    }
    thisHostDatacenterId_ =
        datacenterId(groupConfiguration.thisHostConfiguration().datacenter);
}

uint32_t DatacenterAwareQuorumRingOracle::datacenterId(
    const std::string& name)
{
    auto dcIdIter = datacenterNameToId_.find(name);
    if(dcIdIter != datacenterNameToId_.end()) {
        return dcIdIter->second;
    } else {
        uint32_t newDcId = nextDatacenterId_++;
        MORDOR_LOG_TRACE(g_log) << this << " ad dc " << name << " id=" << newDcId;
        datacenterNameToId_[name] = newDcId;
        return newDcId;
    }
}

bool DatacenterAwareQuorumRingOracle::chooseRing(
    const PingTracker::PingStatsMap& pingStatsMap,
    vector<uint32_t>* ring) const
{
    // XXX probably we should also inject 'now'.
    const uint64_t now = TimerManager::now();

    vector<TaggedHostId> liveHosts;
    gatherLiveHosts(pingStatsMap, now, &liveHosts);
    sort(liveHosts.begin(), liveHosts.end());

    vector<TaggedHostId> newRing;
    vector<TaggedHostId> stash;
    if(!tryToCoverDatacenters(liveHosts, &newRing, &stash) &&
       !okToMissDatacenter_)
    {
        MORDOR_LOG_WARNING(g_log) << this << " could not reach all dc's";
        return false;
    }

    if(!fillToQuorum(stash, &newRing)) {
        MORDOR_LOG_WARNING(g_log) << this <<
                                     " ring selection failed, selected " <<
                                     newRing.size() << ", quorum=" << quorumSize();
        return false;
    } else {
        sort(newRing.begin(), newRing.end());
        ring->clear();
        // XXX force add master for now
        ring->push_back(0);

        for(size_t i = 0; i < newRing.size(); ++i) {
            const uint32_t hostId = newRing[i].second;
            MORDOR_LOG_TRACE(g_log) << this << " new ring (" << i + 1 <<
                                       ", " << hostId << ")";
            ring->push_back(hostId);
        }
        return true;
    }
}

void DatacenterAwareQuorumRingOracle::gatherLiveHosts(
    const PingTracker::PingStatsMap& pingStatsMap,
    uint64_t now,
    vector<TaggedHostId>* liveHosts) const
{
    for(auto i = pingStatsMap.begin(); i != pingStatsMap.end(); ++i) {
        auto dcIter = acceptorToDatacenterId_.find(i->first);
        if(dcIter == acceptorToDatacenterId_.end()) {
            MORDOR_LOG_WARNING(g_log) << this << " acceptor " << i->first <<
                                         " not found";
            continue;
        }
        const uint32_t hostId = i->first;
        const PingStats& pingStats = i->second;
        bool isLive = (now - pingStats.maxReceivedPongSendTime() <
                          noHeartbeatTimeoutUs_);
        if(isLive) {
            double latency = pingStats.meanLatency();
            double loss    = pingStats.packetLoss();
            MORDOR_LOG_TRACE(g_log) << this << " adding live " << hostId <<
                                       " latency=" << latency <<
                                       " loss=" << loss;
            liveHosts->push_back(make_pair(
                                 make_pair(loss, latency), hostId));
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " host " << hostId <<
                                       " not alive: now=" << now <<
                                       ", maxSendTime=" <<
                                       pingStats.maxReceivedPongSendTime() <<
                                       ", delta=" <<
                                       now-pingStats.maxReceivedPongSendTime();
        }
    }
}

bool DatacenterAwareQuorumRingOracle::tryToCoverDatacenters(
    const vector<TaggedHostId>& sortedLiveHosts,
    vector<TaggedHostId>* newRing,
    vector<TaggedHostId>* stash) const
{
    const uint32_t datacenterNumber = nextDatacenterId_;
    set<uint32_t> reachedDatacenters;

    for(size_t i = 0; i < sortedLiveHosts.size(); ++i) {
        const uint32_t hostId = sortedLiveHosts[i].second;
        auto dcIdIter = acceptorToDatacenterId_.find(hostId);
        MORDOR_ASSERT(dcIdIter != acceptorToDatacenterId_.end());
        const uint32_t datacenterId = dcIdIter->second;
        if(reachedDatacenters.find(datacenterId) == reachedDatacenters.end()) {
            MORDOR_LOG_TRACE(g_log) << this << " adding " << hostId <<
                                       " in new dc " << datacenterId;
            newRing->push_back(sortedLiveHosts[i]);
            reachedDatacenters.insert(datacenterId);
        } else {
            stash->push_back(sortedLiveHosts[i]);
        }
    }

    reachedDatacenters.insert(thisHostDatacenterId_);

    MORDOR_LOG_TRACE(g_log) << this << " reached " <<
                               reachedDatacenters.size() << " dcs";

    return reachedDatacenters.size() >= datacenterNumber;
}

bool DatacenterAwareQuorumRingOracle::fillToQuorum(
    const vector<TaggedHostId>& stash,
    vector<TaggedHostId>* newRing) const
{
    const uint32_t quorum = quorumSize();
    for(size_t i = 0; i < stash.size(); ++i) {
        if(newRing->size() >= quorum) {
            return true;
        }
        newRing->push_back(stash[i]);
    }
    
    return newRing->size() >= quorum;
}

uint32_t DatacenterAwareQuorumRingOracle::quorumSize() const {
    return (acceptorToDatacenterId_.size() / 2);
}

}  // namespace lightning
