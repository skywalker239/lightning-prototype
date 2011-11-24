#include "datacenter_aware_quorum_ring_oracle.h"
#include <mordor/log.h>
#include <mordor/timer.h>
#include <algorithm>
#include <set>

namespace lightning {

using Mordor::Address;
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
    bool okToMissDatacenter,
    uint64_t noHeartbeatTimeoutUs)
    : nextDatacenterId_(0),
      okToMissDatacenter_(okToMissDatacenter),
      noHeartbeatTimeoutUs_(noHeartbeatTimeoutUs)
{
    MORDOR_LOG_TRACE(g_log) << this << " okToMiss=" << okToMissDatacenter_ <<
                               " timeout=" << noHeartbeatTimeoutUs_;
}

void DatacenterAwareQuorumRingOracle::addDatacenter(const string& name) {
    if(datacenterNameToId_.find(name) != datacenterNameToId_.end()) {
        MORDOR_LOG_WARNING(g_log) << this << " dc " << name << " is duplicate";
        return;
    }
    MORDOR_LOG_TRACE(g_log) << this << " add dc " << name << ", id=" <<
                               nextDatacenterId_;
    datacenterNameToId_[name] = nextDatacenterId_++;
}

bool DatacenterAwareQuorumRingOracle::addAcceptor(Address::ptr address,
                                                  const string& datacenter)
{
    auto dcIter = datacenterNameToId_.find(datacenter);
    if(dcIter == datacenterNameToId_.end()) {
        MORDOR_LOG_WARNING(g_log) << this << " add " << *address <<
                                     ": dc " << datacenter << " not found";
        return false;
    }
    const uint32_t datacenterId = dcIter->second;
    MORDOR_LOG_TRACE(g_log) << this << " add " << *address <<
                               " dc " << datacenterId << " (" <<
                               datacenter << ")";
    acceptorToDatacenterId_[address] = datacenterId;
    return true;
}

bool DatacenterAwareQuorumRingOracle::chooseRing(
    const PingTracker::PingStatsMap& pingStatsMap,
    vector<Address::ptr>* ring) const
{
    // XXX probably we should also inject 'now'.
    const uint64_t now = TimerManager::now();
    typedef pair<pair<double, double>, Address::ptr> TaggedAddress;

    vector<TaggedAddress> liveAddresses;
    gatherLiveAddresses(pingStatsMap, now, &liveAddresses);
    sort(liveAddresses.begin(), liveAddresses.end());

    vector<TaggedAddress> newRing;
    vector<TaggedAddress> stash;
    if(!tryToCoverDatacenters(liveAddresses, &newRing, &stash) &&
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
        for(size_t i = 0; i < newRing.size(); ++i) {
            const Address::ptr& address = newRing[i].second;
            MORDOR_LOG_TRACE(g_log) << this << " new ring (" << i <<
                                       ", " << *address << ")";
            ring->push_back(address);
        }
        return true;
    }
}

void DatacenterAwareQuorumRingOracle::gatherLiveAddresses(
    const PingTracker::PingStatsMap& pingStatsMap,
    uint64_t now,
    vector<TaggedAddress>* liveAddresses) const
{
    for(auto i = pingStatsMap.begin(); i != pingStatsMap.end(); ++i) {
        auto dcIter = acceptorToDatacenterId_.find(i->first);
        if(dcIter == acceptorToDatacenterId_.end()) {
            MORDOR_LOG_WARNING(g_log) << this << " acceptor " << *i->first <<
                                         " not found";
            continue;
        }
        Address::ptr address = i->first;
        const PingStats& pingStats = i->second;
        bool isLive = (now - pingStats.maxReceivedPongSendTime() <
                          noHeartbeatTimeoutUs_);
        if(isLive) {
            double latency = pingStats.meanLatency();
            double loss    = pingStats.packetLoss();
            MORDOR_LOG_TRACE(g_log) << this << " adding live " << *address <<
                                       " latency=" << latency <<
                                       " loss=" << loss;
            liveAddresses->push_back(make_pair(
                                     make_pair(loss, latency), address));
        }
    }
}

bool DatacenterAwareQuorumRingOracle::tryToCoverDatacenters(
    const vector<TaggedAddress>& sortedLiveAddresses,
    vector<TaggedAddress>* newRing,
    vector<TaggedAddress>* stash) const
{
    const uint32_t datacenterNumber = nextDatacenterId_;
    set<uint32_t> reachedDatacenters;

    for(size_t i = 0; i < sortedLiveAddresses.size(); ++i) {
        const Address::ptr& address = sortedLiveAddresses[i].second;
        auto dcIdIter = acceptorToDatacenterId_.find(address);
        MORDOR_ASSERT(dcIdIter != acceptorToDatacenterId_.end());
        const uint32_t datacenterId = dcIdIter->second;
        if(reachedDatacenters.find(datacenterId) == reachedDatacenters.end()) {
            MORDOR_LOG_TRACE(g_log) << this << " adding " << *address <<
                                       " in new dc " << datacenterId;
            newRing->push_back(sortedLiveAddresses[i]);
        } else {
            stash->push_back(sortedLiveAddresses[i]);
        }
    }

    return reachedDatacenters.size() >= datacenterNumber;
}

bool DatacenterAwareQuorumRingOracle::fillToQuorum(
    const vector<TaggedAddress>& stash,
    vector<TaggedAddress>* newRing) const
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
    return (acceptorToDatacenterId_.size() + 1) / 2;
}

}  // namespace lightning
