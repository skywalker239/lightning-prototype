#pragma once

#include <mordor/socket.h>
#include <boost/shared_ptr.hpp>
#include <vector>

namespace lightning {

//! Represents an established ring configuration.
//  The hosts are represented by their ids wrt a common global
//  configuration across all hosts.
class RingConfiguration {
public:
    typedef boost::shared_ptr<RingConfiguration> ptr;
    typedef boost::shared_ptr<const RingConfiguration> const_ptr;

    RingConfiguration(const std::vector<uint32_t>& ringHostIds,
                      uint32_t ringId)
        : ringHostIds_(ringHostIds),
          ringId_(ringId)
    {}

    const std::vector<uint32_t>& ringHostIds() const { return ringHostIds_; }

    uint32_t ringId() const { return ringId_; }
private:
    const std::vector<uint32_t> ringHostIds_;
    const uint32_t ringId_;
};

}  // namespace lightning
