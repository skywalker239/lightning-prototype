#pragma once

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
class DatacenterAwareQuorumRingOracle {
public:
    //! If okToMissDatacenter is false then it will fail to build a ring
    //  in case of any datacenters going down.
    //  In any case, if it succeeds, then the ring is an m-quorum.
    //
    //  noHeartbeatTimeout is used to detect dead hosts' ping stats.
    DatacenterAwareQuorumRingOracle(bool okToMissDatacenter,
                                    uint64_t noHeartbeatTimeoutUs);

    void addDatacenter(const std::string& name);
    bool addAcceptor(Mordor::Address::ptr address,
                     const std::string& datacenter);

    virtual bool chooseRing(const PingTracker::PingStatsMap& pingStatsMap,
                            std::vector<Mordor::Address::ptr>* ring) const;
private:
    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                        const Mordor::Address::ptr& rhs)
        {
            return *lhs < *rhs;
        }
    };

    uint32_t quorumSize() const;

    //! ((loss, latency), address). for sorting.
    typedef std::pair<std::pair<double, double>, Mordor::Address::ptr>
        TaggedAddress;

    void gatherLiveAddresses(const PingTracker::PingStatsMap& pingStatsMap,
                             uint64_t now,
                             std::vector<TaggedAddress>* destination) const;
    bool tryToCoverDatacenters(const std::vector<TaggedAddress>& sortedAddresses,
                               std::vector<TaggedAddress>* newRing,
                               std::vector<TaggedAddress>* stash) const;
    bool fillToQuorum(const std::vector<TaggedAddress>& stash,
                      std::vector<TaggedAddress>* newRing) const;
                      

    std::map<std::string, uint32_t> datacenterNameToId_;
    uint32_t nextDatacenterId_;
    const bool okToMissDatacenter_;
    const uint64_t noHeartbeatTimeoutUs_;

    std::map<Mordor::Address::ptr, uint32_t> acceptorToDatacenterId_;
};
                            
}  // namespace lightning
