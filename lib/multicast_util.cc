#include "multicast_util.h"

namespace lightning {

using Mordor::Socket;
using Mordor::Address;

void joinMulticastGroup(Socket::ptr socket, Address::ptr multicastGroup) {
    struct ip_mreq mreq;
    // Ah, the royal ugliness.
    mreq.imr_multiaddr =
        ((struct sockaddr_in*)(multicastGroup->name()))->sin_addr;
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    socket->setOption(IPPROTO_IP, IP_ADD_MEMBERSHIP, mreq);
}

}  // namespace lightning
