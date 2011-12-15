#pragma once

#include <mordor/socket.h>
#include <boost/shared_ptr.hpp>
#include <vector>

namespace lightning {

//! Represents an established ring configuration.
class RingConfiguration {
public:
    typedef boost::shared_ptr<RingConfiguration> ptr;
    typedef boost::shared_ptr<const RingConfiguration> const_ptr;

    //! The addresses in ringAddresses must be complete with both the host and
    //  port parts. These are the addresses that the corresponding
    //  SyncGroupRequester  will await replies from.
    //  As we have many requesters
    //  (ping, SetRing, Phase1, Phase1Batch, Phase2 etc.)
    //  ring configurations for each of them will have the same addresses, but
    //  different ports.
    //  TODO(skywalker): multiplex all network I/O into one requester?
    RingConfiguration(const std::vector<Mordor::Address::ptr>& ringAddresses,
                      const uint64_t ringId)
        : ringAddresses_(ringAddresses),
          ringId_(ringId)
    {}

    const std::vector<Mordor::Address::ptr>& ringAddresses() const {
        return ringAddresses_;
    }

    uint64_t ringId() const { return ringId_; }
private:
    std::vector<Mordor::Address::ptr> ringAddresses_;
    uint64_t ringId_;
};

}  // namespace lightning
