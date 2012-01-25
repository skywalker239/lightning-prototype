#pragma once

#include <mordor/json.h>
#include <mordor/socket.h>
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

    HostConfiguration(const std::string& _name,
                      const std::string& _datacenter,
                      Mordor::Address::ptr _multicastListenAddress,
                      Mordor::Address::ptr _multicastReplyAddress,
                      Mordor::Address::ptr _multicastSourceAddress,
                      Mordor::Address::ptr _ringAddress)
        : name(_name),
          datacenter(_datacenter),
          multicastListenAddress(_multicastListenAddress),
          multicastReplyAddress(_multicastReplyAddress),
          multicastSourceAddress(_multicastSourceAddress),
          ringAddress(_ringAddress)
    {}
};

//! For the design with a fixed master, host 0 is always the master for
//  simplicity.
class GroupConfiguration {
public:
    GroupConfiguration(const std::vector<HostConfiguration>& hosts,
                       uint32_t thisHostId)
        : hosts_(hosts),
          thisHostId_(thisHostId)
    {}

    const std::vector<HostConfiguration>& hosts() const { return hosts_; }

    uint32_t thisHostId() const { return thisHostId_; }
private:
    const std::vector<HostConfiguration> hosts_;
    const uint32_t thisHostId_;
};

GroupConfiguration parseGroupConfiguration(
    const Mordor::JSON::Value& json,
    uint32_t thisHostId);

}  // namespace lightning
