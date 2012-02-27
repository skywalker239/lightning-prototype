#include "host_configuration.h"
#include <mordor/assert.h>
#include <sstream>

namespace lightning {

const size_t GroupConfiguration::kMaxGroupSize;
const uint32_t GroupConfiguration::kInvalidHostId;
const uint32_t GroupConfiguration::kMasterId;

using Mordor::Address;
namespace JSON = Mordor::JSON;
using std::make_pair;
using std::ostringstream;
using std::string;
using std::vector;

namespace {

Address::ptr lookupAddress(const string& address) {
    return Address::lookup(address).front();
}

string makeHostnameWithId(uint32_t hostId, const string& hostname) {
    ostringstream ss;
    ss << hostId;
    ss << ":";
    ss << hostname;
    return ss.str();
}

}  // anonymous namespace

GroupConfiguration::ptr parseGroupConfiguration(const JSON::Value& json,
                                                uint32_t thisHostId)
{
    vector<HostConfiguration> configuration;
    const JSON::Array hosts = json.get<JSON::Array>();

    for(size_t i = 0; i < hosts.size(); ++i) {
        const JSON::Array& hostData = hosts[i].get<JSON::Array>();
        MORDOR_ASSERT(hostData.size() == 6);
        const string& name = makeHostnameWithId(i, hostData[0].get<string>());
        const string& datacenter = hostData[1].get<string>();
        auto multicastListenAddress =
            lookupAddress(hostData[2].get<string>());
        auto multicastReplyAddress = 
            lookupAddress(hostData[3].get<string>());
        auto multicastSourceAddress =
            lookupAddress(hostData[4].get<string>());
        auto ringAddress =
            lookupAddress(hostData[5].get<string>());

        configuration.push_back(
            HostConfiguration(
                name,
                datacenter,
                multicastListenAddress,
                multicastReplyAddress,
                multicastSourceAddress,
                ringAddress));
    }
    return GroupConfiguration::ptr(
               new GroupConfiguration(configuration, thisHostId));
}

GroupConfiguration::GroupConfiguration(const vector<HostConfiguration>& hosts,
                                       uint32_t thisHostId)
    : hosts_(hosts),
      thisHostId_(thisHostId)
{
    MORDOR_ASSERT(hosts.size() <= kMaxGroupSize);
    MORDOR_ASSERT(thisHostId < hosts.size());
    for(size_t i = 0; i < hosts_.size(); ++i) {
        auto hostIdResult = replyAddressToHostId_.insert(
            make_pair(hosts_[i].multicastReplyAddress, i));
        MORDOR_ASSERT(hostIdResult.second);
        // XXX HACK to allow ersatz ring for phase 2 to work
        auto ringResult = replyAddressToHostId_.insert(
            make_pair(hosts_[i].ringAddress, i));
        MORDOR_ASSERT(ringResult.second);
        auto rpcNameResult = addressToServiceName_.insert(
            make_pair(hosts_[i].multicastReplyAddress,
                      hosts_[i].name + ":RPC"));
        MORDOR_ASSERT(rpcNameResult.second);
        auto ringNameResult = addressToServiceName_.insert(
            make_pair(hosts_[i].ringAddress,
                      hosts_[i].name + ":RING"));
        MORDOR_ASSERT(ringNameResult.second);
        auto rpcSrcResult = addressToServiceName_.insert(
            make_pair(hosts_[i].multicastSourceAddress,
                      hosts_[i].name + ":SRC"));
        MORDOR_ASSERT(rpcSrcResult.second);
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

size_t GroupConfiguration::size() const {
    return hosts_.size();
}

const HostConfiguration& GroupConfiguration::host(size_t index) const {
    MORDOR_ASSERT(index < hosts_.size());
    return hosts_[index];
}

uint32_t GroupConfiguration::replyAddressToId(const Address::ptr& address) const {
    auto idIter = replyAddressToHostId_.find(address);
    return (idIter == replyAddressToHostId_.end()) ?
                                    kInvalidHostId :
                                    idIter->second;
}

std::ostream& operator<<(std::ostream& os,
    const HostConfiguration& hostConfiguration)
{
    os << hostConfiguration.name;
    return os;
}

std::ostream& operator<<(std::ostream& os,
    const GroupConfiguration& groupConfiguration)
{
    os << "Group(Id(" << groupConfiguration.thisHostId_ << "), [";
    for(size_t i = 0; i < groupConfiguration.size(); ++i) {
        os << groupConfiguration.host(i);
        if(i + 1 < groupConfiguration.size()) {
            os << ", ";
        }
    }
    os << "])";
    return os;
}

}  // namespace lightning
