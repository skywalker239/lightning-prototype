#pragma once

#include "group_configuration.h"
#include "ring_oracle.h"
#include <mordor/socket.h>
#include <string>
#include <vector>

namespace lightning {

//! This oracle knows about multiple datacenters and acceptor->datacenter
//  mapping.
//  It builds the ring such that it is oriented in order of increasing latency
//  and goes through every datacenter (and is a majority quorum).
//
//  This class is not (thread|fiber)-safe.
class DatacenterAwareQuorumRingOracle : public RingOracle {
public:
    //! If okToMissDatacenter is false then it will fail to build a ring
    //  in case of any datacenters going down.
    //  In any case, if it succeeds, then the ring is an m-quorum.
    //
    //  noHeartbeatTimeout is used to detect dead hosts' ping stats.
    DatacenterAwareQuorumRingOracle(GroupConfiguration::ptr group,
                                    bool okToMissDatacenter,
                                    uint64_t noHeartbeatTimeoutUs);

    //! XXX For now the first element of the ring is 0, as host 0 is the
    //  designated master.
    virtual bool chooseRing(const PingTracker::PingStatsMap& pingStatsMap,
                            std::vector<uint32_t>* ring) const;

    typedef boost::shared_ptr<DatacenterAwareQuorumRingOracle> ptr;
private:
    //! XXX this returns floor(n/2) instead of floor(n/2) + 1
    //  because the ring is completed by the master itself.
    uint32_t quorumSize() const;

    //! ((loss, latency), address). for sorting.
    typedef std::pair<std::pair<double, double>, uint32_t>
        TaggedHostId;

    void gatherLiveHosts(const PingTracker::PingStatsMap& pingStatsMap,
                         uint64_t now,
                         std::vector<TaggedHostId>* destination) const;
    bool tryToCoverDatacenters(const std::vector<TaggedHostId>& sortedLiveHosts,
                               std::vector<TaggedHostId>* newRing,
                               std::vector<TaggedHostId>* stash) const;
    bool fillToQuorum(const std::vector<TaggedHostId>& stash,
                      std::vector<TaggedHostId>* newRing) const;
                      

    uint32_t datacenterId(const std::string& name);

    const bool okToMissDatacenter_;
    const uint64_t noHeartbeatTimeoutUs_;

    std::map<std::string, uint32_t> datacenterNameToId_;
    uint32_t nextDatacenterId_;
    std::map<uint32_t, uint32_t> acceptorToDatacenterId_;
    uint32_t thisHostDatacenterId_;
};
                            
}  // namespace lightning
