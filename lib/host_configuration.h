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

    HostConfiguration() {}

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

}  // namespace lightning
