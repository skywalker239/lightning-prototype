#pragma once

#include <mordor/json.h>
#include <mordor/socket.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace lightning {

struct HostConfiguration {
    std::string name;
    std::string datacenter;
    Mordor::Address::ptr multicastListenAddress;
    Mordor::Address::ptr multicastReplyAddress;
    Mordor::Address::ptr multicastSourceAddress;
    Mordor::Address::ptr ringAddress;
    Mordor::Address::ptr unicastAddress;

    HostConfiguration(const std::string& _name,
                      const std::string& _datacenter,
                      Mordor::Address::ptr _multicastListenAddress,
                      Mordor::Address::ptr _multicastReplyAddress,
                      Mordor::Address::ptr _multicastSourceAddress,
                      Mordor::Address::ptr _ringAddress,
                      Mordor::Address::ptr _unicastAddress)
        : name(_name),
          datacenter(_datacenter),
          multicastListenAddress(_multicastListenAddress),
          multicastReplyAddress(_multicastReplyAddress),
          multicastSourceAddress(_multicastSourceAddress),
          ringAddress(_ringAddress),
          unicastAddress(_unicastAddress)
    {}
};

std::ostream& operator<<(std::ostream& os,
                         const HostConfiguration& hostConfiguration);

//! For the design with a fixed master, host 0 is always the master for
//  simplicity.
class GroupConfiguration {
public:
    typedef boost::shared_ptr<GroupConfiguration> ptr;
    //! So that a quorum can be represented by a 64-bit bitmask.
    //  I think that 64 acceptors ought to be enough for anybody(TM).
    static const size_t kMaxGroupSize = 64;

    static const uint32_t kInvalidHostId = ~0;
    static const uint32_t kLearnerHostId = kInvalidHostId - 1;

    //! Constructs a configuration on an acceptor.
    GroupConfiguration(const Mordor::Address::ptr groupMulticastAddress,
                       const std::vector<HostConfiguration>& hosts,
                       uint32_t thisHostId);

    //! Constructs a configuration on a learner.
    GroupConfiguration(const Mordor::Address::ptr groupMulticastAddress,
                       const std::vector<HostConfiguration>& hosts,
                       const std::string& datacenter);

    //! Number of hosts in the group
    size_t size() const;

    const Mordor::Address::ptr groupMulticastAddress() const {
            return groupMulticastAddress_;
    }

    //! Asserts on invalid indices.
    const HostConfiguration& host(size_t index) const;

    //! Returns kInvalidHostId for unknown addresses.
    uint32_t replyAddressToId(const Mordor::Address::ptr& address) const;

    //! For debug logging.
    std::string addressToServiceName(const Mordor::Address::ptr& address) const;

    uint32_t masterId() const { return kMasterId; }

    uint32_t thisHostId() const { return thisHostId_; }

    bool isLearner() const { return thisHostId_ == kLearnerHostId; }

    const std::string& datacenter() const { return datacenter_; }
private:
    friend std::ostream& operator<<(std::ostream&,
                                    const GroupConfiguration&);

    void populateAddressMaps();

    const Mordor::Address::ptr groupMulticastAddress_;
    const std::vector<HostConfiguration> hosts_;
    const uint32_t thisHostId_;
    std::string datacenter_;

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& a,
                        const Mordor::Address::ptr& b) const
        {
            return *a < *b;
        }
    };

    std::map<Mordor::Address::ptr, uint32_t, AddressCompare>
        replyAddressToHostId_;

    std::map<Mordor::Address::ptr, std::string, AddressCompare>
        addressToServiceName_;

    //! XXX fixed master for now
    static const uint32_t kMasterId = 0;
};

GroupConfiguration::ptr parseGroupConfiguration(
    const Mordor::JSON::Value& json,
    uint32_t thisHostId,
    Mordor::Address::ptr groupMulticastAddress);

}  // namespace lightning
