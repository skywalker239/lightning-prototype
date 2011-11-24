#pragma once

#include <mordor/socket.h>

namespace lightning {

//! IMPORTANT: Only works for IPv4 sockets for the moment.
void joinMulticastGroup(Mordor::Socket::ptr socket,
                        Mordor::Address::ptr multicastGroup);

}  // namespace lightning
