#pragma once

#include "host_configuration.h"
#include <mordor/socket.h>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <vector>

namespace lightning {

//! Represents an established ring configuration.
//  The hosts are represented by their ids wrt a common global
//  configuration across all hosts.
class RingConfiguration {
public:
    typedef boost::shared_ptr<RingConfiguration> ptr;
    typedef boost::shared_ptr<const RingConfiguration> const_ptr;

    static const uint32_t kInvalidRingIndex =
        GroupConfiguration::kMaxGroupSize;

    RingConfiguration(GroupConfiguration::ptr groupConfiguration,
                      const std::vector<uint32_t>& ringHostIds,
                      uint32_t ringId);

    const std::vector<uint32_t> ringHostIds() const { return ringHostIds_; }

    //! The unique ring id.
    uint32_t ringId() const { return ringId_; }

    //! The bitmask of ring acceptor ids EXCEPT THIS ONE.
    uint64_t ringMask() const { return ringMask_; }

    //! True if this acceptor is part of the ring.
    bool isInRing() const { return ringIndex_ != kInvalidRingIndex; }

    //! The ring index of this acceptor.
    //  kInvalidRingIndex if not in the ring.
    uint32_t ringIndex() const { return ringIndex_; }

    //! Multicast address to reach the whole ring.
    Mordor::Address::ptr ringMulticastAddress() const {
        return groupConfiguration_->groupMulticastAddress();
    }

    //! The address that this host should forward vote data to.
    //  NULL if this acceptor is not in the ring.
    Mordor::Address::ptr nextRingAddress() const { return nextRingAddress_; }

    //! The multicast reply address of the last host in the ring.
    //  NULL if this acceptor is not in the ring.
    Mordor::Address::ptr lastRingAddress() const { return lastRingAddress_; }

    //! Returns kInvalidHostId for unknown addresses.
    uint32_t replyAddressToId(const Mordor::Address::ptr& address) const {
        return groupConfiguration_->replyAddressToId(address);
    }

    const GroupConfiguration::ptr& group() const {
        return groupConfiguration_;
    }

private:
    GroupConfiguration::ptr groupConfiguration_;
    const std::vector<uint32_t> ringHostIds_;
    const uint32_t ringId_;
    uint64_t ringMask_;
    size_t ringIndex_;
    Mordor::Address::ptr nextRingAddress_;
    Mordor::Address::ptr lastRingAddress_;

    friend std::ostream& operator<<(std::ostream& os,
                                    const RingConfiguration&
                                        ringConfiguration);
};

std::ostream& operator<<(std::ostream& os,
                         const RingConfiguration& ringConfiguration);

}  // namespace lightning
