#pragma once

#include "host_configuration.h"
#include "host_group_var.h"
#include "int_var.h"
#include "address_var.h"

namespace lightning {

//! TODO(skywalker): Make this saner.
class GroupConfiguration {
public:
    typedef boost::shared_ptr<GroupConfiguration> ptr;
    //! So that a quorum can be represented by a 64-bit bitmask.
    //  I think that 64 acceptors ought to be enough for anybody(TM).
    static const size_t kMaxGroupSize = 64;

    static const uint32_t kInvalidHostId = ~0;
    static const uint32_t kLearnerHostId = kInvalidHostId - 1;

    //! Sets up an acceptor configuration.
    GroupConfiguration(const AddressVar& groupAddress,
                       const HostGroupVar& hosts,
                       const IntVar& masterId,
                       uint32_t thisHostId);

    //! Sets up a learner configuration
    GroupConfiguration(const AddressVar& groupAddress,
                       const HostGroupVar& hosts,
                       const IntVar& masterId,
                       const std::string& datacenter);

    //! Number of acceptors in the group
    size_t size() const;

    //! Asserts on invalid indices.
    const HostConfiguration& host(size_t index) const;

    const Mordor::Address::ptr groupMulticastAddress() const;

    //! Returns kInvalidHostId for unknown addresses.
    uint32_t replyAddressToId(const Mordor::Address::ptr& address) const;

    //! For debug logging.
    std::string addressToServiceName(const Mordor::Address::ptr& address) const;

    uint32_t masterId(); 

    uint32_t thisHostId() const;

    const HostConfiguration& thisHostConfiguration() const;

    bool isLearner() const { return thisHostId_ == kLearnerHostId; }

    const std::string& datacenter() const { return datacenter_; }
private:
    void populateAddressMaps();

    Mordor::Address::ptr groupMulticastAddress_;
    HostGroupVar hosts_;
    IntVar masterId_;
    const uint32_t thisHostId_;

    std::string datacenter_;
    HostConfiguration thisHostConfiguration_;

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

}  // namespace lightning
