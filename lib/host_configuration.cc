#include "host_configuration.h"
#include <mordor/assert.h>
#include <sstream>

namespace lightning {

const size_t GroupConfiguration::kMaxGroupSize;
const uint32_t GroupConfiguration::kInvalidHostId;
const uint32_t GroupConfiguration::kLearnerHostId;
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

void GroupConfiguration::parseHostConfigurations(
    const Mordor::JSON::Value& json,
    vector<HostConfiguration>* acceptorConfigurations,
    HostConfiguration* learnerConfiguration)
{
    const JSON::Array hosts = json.get<JSON::Array>();
    // at least one acceptor and learner configuration
    MORDOR_ASSERT(hosts.size() >= 2);

    for(size_t i = 0; i + 1 < hosts.size(); ++i) {
        const JSON::Array& hostData = hosts[i].get<JSON::Array>();
        MORDOR_ASSERT(hostData.size() == 7);
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
        auto unicastAddress =
            lookupAddress(hostData[6].get<string>());

        acceptorConfigurations->push_back(
            HostConfiguration(
                name,
                datacenter,
                multicastListenAddress,
                multicastReplyAddress,
                multicastSourceAddress,
                ringAddress,
                unicastAddress));
    }

    const JSON::Array& learnerData = hosts.back().get<JSON::Array>();
    MORDOR_ASSERT(learnerData.size() == 7);
    learnerConfiguration->name = "LEARNER";
    learnerConfiguration->datacenter = "UNK";
    learnerConfiguration->multicastListenAddress =
        lookupAddress(learnerData[2].get<string>());
    learnerConfiguration->multicastReplyAddress =
        lookupAddress(learnerData[3].get<string>());
    learnerConfiguration->multicastSourceAddress =
        lookupAddress(learnerData[4].get<string>());
    learnerConfiguration->ringAddress =
        lookupAddress(learnerData[5].get<string>());
    learnerConfiguration->unicastAddress =
        lookupAddress(learnerData[6].get<string>());
}

GroupConfiguration::ptr GroupConfiguration::parseAcceptorConfig(
    const Mordor::JSON::Value& json,
    uint32_t thisHostId,
    Address::ptr groupMulticastAddress)
{
    vector<HostConfiguration> acceptorConfigurations;
    HostConfiguration learnerConfiguration;
    parseHostConfigurations(json,
                            &acceptorConfigurations,
                            &learnerConfiguration);
    return GroupConfiguration::ptr(
               new GroupConfiguration(groupMulticastAddress,
                                      acceptorConfigurations,
                                      thisHostId));
}

GroupConfiguration::ptr GroupConfiguration::parseLearnerConfig(
    const Mordor::JSON::Value& json,
    const string& datacenter,
    Address::ptr groupMulticastAddress)
{
    vector<HostConfiguration> acceptorConfigurations;
    HostConfiguration learnerConfiguration;
    parseHostConfigurations(json,
                            &acceptorConfigurations,
                            &learnerConfiguration);
    return GroupConfiguration::ptr(
               new GroupConfiguration(groupMulticastAddress,
                                      acceptorConfigurations,
                                      learnerConfiguration,
                                      datacenter));
}

GroupConfiguration::GroupConfiguration(Address::ptr groupMulticastAddress,
                                       const vector<HostConfiguration>& 
                                           acceptorConfigurations,
                                       uint32_t thisHostId)
    : groupMulticastAddress_(groupMulticastAddress),
      acceptorConfigurations_(acceptorConfigurations),
      thisHostId_(thisHostId)
{
    MORDOR_ASSERT(acceptorConfigurations.size() <= kMaxGroupSize);
    MORDOR_ASSERT(thisHostId < acceptorConfigurations.size());
    datacenter_ = acceptorConfigurations_[thisHostId_].datacenter;
    thisHostConfiguration_ = acceptorConfigurations_[thisHostId_];
    populateAddressMaps();
}

GroupConfiguration::GroupConfiguration(Address::ptr groupMulticastAddress,
                                       const vector<HostConfiguration>&
                                           acceptorConfigurations,
                                       const HostConfiguration&
                                           learnerConfiguration,
                                       const string& datacenter)
    : groupMulticastAddress_(groupMulticastAddress),
      acceptorConfigurations_(acceptorConfigurations),
      thisHostId_(kLearnerHostId),
      datacenter_(datacenter)
{
    MORDOR_ASSERT(acceptorConfigurations.size() <= kMaxGroupSize);
    thisHostConfiguration_ = learnerConfiguration;
    // TODO(skywalker): Ugly.
    thisHostConfiguration_.datacenter = datacenter_;
    populateAddressMaps();
}

void GroupConfiguration::populateAddressMaps()
{
    for(size_t i = 0; i < acceptorConfigurations_.size(); ++i) {
        auto hostIdResult = replyAddressToHostId_.insert(
            make_pair(acceptorConfigurations_[i].multicastReplyAddress, i));
        MORDOR_ASSERT(hostIdResult.second);
        // XXX HACK to allow ersatz ring for phase 2 to work
        auto ringResult = replyAddressToHostId_.insert(
            make_pair(acceptorConfigurations_[i].ringAddress, i));
        MORDOR_ASSERT(ringResult.second);
        auto rpcNameResult = addressToServiceName_.insert(
            make_pair(acceptorConfigurations_[i].multicastReplyAddress,
                      acceptorConfigurations_[i].name + ":MRPC"));
        MORDOR_ASSERT(rpcNameResult.second);
        auto ringNameResult = addressToServiceName_.insert(
            make_pair(acceptorConfigurations_[i].ringAddress,
                      acceptorConfigurations_[i].name + ":RING"));
        MORDOR_ASSERT(ringNameResult.second);
        auto rpcSrcResult = addressToServiceName_.insert(
            make_pair(acceptorConfigurations_[i].multicastSourceAddress,
                      acceptorConfigurations_[i].name + ":SRC"));
        MORDOR_ASSERT(rpcSrcResult.second);
        auto unicastResult = addressToServiceName_.insert(
            make_pair(acceptorConfigurations_[i].unicastAddress,
                      acceptorConfigurations_[i].name + ":URPC"));
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

size_t GroupConfiguration::size() const {
    return acceptorConfigurations_.size();
}

const HostConfiguration& GroupConfiguration::host(size_t index) const {
    MORDOR_ASSERT(index < acceptorConfigurations_.size());
    return acceptorConfigurations_[index];
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
