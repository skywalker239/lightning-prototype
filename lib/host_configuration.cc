#include "host_configuration.h"
#include <mordor/assert.h>

namespace lightning {

using Mordor::Address;
namespace JSON = Mordor::JSON;
using std::string;
using std::vector;

namespace {

Address::ptr lookupAddress(const string& address) {
    return Address::lookup(address).front();
}

}  // anonymous namespace

GroupConfiguration parseGroupConfiguration(const JSON::Value& json,
                                           uint32_t thisHostId)
{
    vector<HostConfiguration> configuration;
    const JSON::Array hosts = json.get<JSON::Array>();
    for(size_t i = 0; i < hosts.size(); ++i) {
        const JSON::Array& hostData = hosts[i].get<JSON::Array>();
        MORDOR_ASSERT(hostData.size() == 6);
        const string& name = hostData[0].get<string>();
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
    return GroupConfiguration(configuration, thisHostId);
}

}  // namespace lightning
