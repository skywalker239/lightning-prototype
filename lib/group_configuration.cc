#include "group_configuration.h"
#include <mordor/assert.h>
#include <sstream>

namespace lightning {

using Mordor::Address;
using std::make_pair;
using std::ostringstream;
using std::string;

GroupConfiguration::GroupConfiguration(const AddressVar& groupAddress,
                                       const HostGroupVar& hosts,
                                       const IntVar& masterId,
                                       uint32_t thisHostId)
    : hosts_(hosts), masterId_(masterId), thisHostId_(thisHostId)
{
    //! XXX for now assume it never changes.
    AddressVar groupAddressVar(groupAddress);
    groupAddressVar.update();
    groupMulticastAddress_ = groupAddressVar.get();
    hosts_.update();
    masterId_.update();

    datacenter_ = hosts_.host(thisHostId).datacenter;
    thisHostConfiguration_ = hosts_.host(thisHostId);

    populateAddressMaps();
}

GroupConfiguration::GroupConfiguration(const AddressVar& groupAddress,
                                       const HostGroupVar& hosts,
                                       const IntVar& masterId,
                                       const string& datacenter)
    : hosts_(hosts), masterId_(masterId), thisHostId_(kLearnerHostId)
{
    //! XXX See above.
    AddressVar groupAddressVar(groupAddress);
    groupAddressVar.update();
    groupMulticastAddress_ = groupAddressVar.get();
    hosts_.update();
    masterId_.update();

    datacenter_ = datacenter;
    thisHostConfiguration_ = hosts_.host(hosts_.size() - 1);

    populateAddressMaps();
}

size_t GroupConfiguration::size() const {
    return hosts_.size() - 1;
}

const HostConfiguration& GroupConfiguration::host(size_t index) const {
    MORDOR_ASSERT(index + 1< size());
    return hosts_.host(index);
}

const Address::ptr GroupConfiguration::groupMulticastAddress() const {
    return groupMulticastAddress_;
}

void GroupConfiguration::populateAddressMaps()
{
    for(size_t i = 0; i < size(); ++i) {
        auto hostIdResult = replyAddressToHostId_.insert(
            make_pair(hosts_.host(i).multicastReplyAddress, i));
        MORDOR_ASSERT(hostIdResult.second);
        // XXX HACK to allow ersatz ring for phase 2 to work
        auto ringResult = replyAddressToHostId_.insert(
            make_pair(hosts_.host(i).ringAddress, i));
        MORDOR_ASSERT(ringResult.second);
        auto rpcNameResult = addressToServiceName_.insert(
            make_pair(hosts_.host(i).multicastReplyAddress,
                      hosts_.host(i).name + ":MRPC"));
        MORDOR_ASSERT(rpcNameResult.second);
        auto ringNameResult = addressToServiceName_.insert(
            make_pair(hosts_.host(i).ringAddress,
                      hosts_.host(i).name + ":RING"));
        MORDOR_ASSERT(ringNameResult.second);
        auto rpcSrcResult = addressToServiceName_.insert(
            make_pair(hosts_.host(i).multicastSourceAddress,
                      hosts_.host(i).name + ":SRC"));
        MORDOR_ASSERT(rpcSrcResult.second);
        auto unicastResult = addressToServiceName_.insert(
            make_pair(hosts_.host(i).unicastAddress,
                      hosts_.host(i).name + ":URPC"));
        MORDOR_ASSERT(unicastResult.second);
    }
}

string GroupConfiguration::addressToServiceName(
    const Address::ptr& address) const
{
    ostringstream ss;
    auto serviceIter = addressToServiceName_.find(address);
    if(serviceIter != addressToServiceName_.end()) {
        ss << serviceIter->second << "[" << *address << "]";
    } else {
        ss << "unknown[" << *address << "]";
    }
    return ss.str();
}

uint32_t GroupConfiguration::replyAddressToId(
    const Address::ptr& address) const
{
    auto idIter = replyAddressToHostId_.find(address);
    return (idIter == replyAddressToHostId_.end()) ?
                                    kInvalidHostId :
                                    idIter->second;
}

uint32_t GroupConfiguration::masterId() {
    masterId_.update();
    return static_cast<uint32_t>(masterId_.get());
}

uint32_t GroupConfiguration::thisHostId() const {
    return thisHostId_;
}

const HostConfiguration& GroupConfiguration::thisHostConfiguration() const {
    return thisHostConfiguration_;
}


}  // namespace lightning
