#include "host_group_var.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/socket.h>
#include <mordor/json.h>
#include <sstream>

namespace lightning {

using Mordor::Address;
using Mordor::Log;
using Mordor::Logger;
using std::string;
using std::ostringstream;
namespace JSON = Mordor::JSON;

static Logger::ptr g_log = Log::lookup("lightning:host_group_var");

namespace {

Address::ptr lookupAddress(const string& address) {
    return Address::lookup(address).front();
}

string makeHostnameWithId(uint32_t hostId, const string& hostname) {
    ostringstream ss;
    ss << hostname << "(" << hostId << ")";
    return ss.str();
}

}  // anonymous namespace

HostGroupVar::HostGroupVar(const string& key,
                           ConfigurationStore::ptr store)
    : ConfigurationVarBase(key, store)
{}

size_t HostGroupVar::size() const {
    return hosts_.size();
}

const HostConfiguration& HostGroupVar::host(size_t index) const {
    MORDOR_ASSERT(index < hosts_.size());
    return hosts_[index];
}

void HostGroupVar::updateImpl() {
    hosts_.clear();
    try {
        JSON::Value value = JSON::parse(value_);

        const JSON::Array& hostArray = value.get<JSON::Array>();
        // At least one acceptor and one learner.
        MORDOR_ASSERT(hostArray.size() >= 2);

        for(size_t i = 0; i < hostArray.size(); ++i) {
            const JSON::Array& hostData = hostArray[i].get<JSON::Array>();
            MORDOR_ASSERT(hostData.size() == 7);
            
            const string& name =
                makeHostnameWithId(i, hostData[0].get<string>());
            const string& dc =
                hostData[1].get<string>();
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

            hosts_.push_back(
                HostConfiguration(
                    name,
                    dc,
                    multicastListenAddress,
                    multicastReplyAddress,
                    multicastSourceAddress,
                    ringAddress,
                    unicastAddress));
        }
    } catch(std::invalid_argument& e) {
        MORDOR_LOG_ERROR(g_log) << this << " Failed to parse JSON '" <<
            value_ << "': " << e.what();
        throw;
    } catch(boost::bad_get& e) {
        MORDOR_LOG_ERROR(g_log) << this << " Failed to convert JSON: " <<
            e.what();
            throw;
    }
}

}  // namespace lightning
