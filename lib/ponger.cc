#include "ponger.h"
#include "ping_packet.h"
#include <mordor/log.h>
#include <netinet/in.h>

namespace lightning {

using Mordor::IOManager;
using Mordor::Socket;
using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;

static Logger::ptr g_log = Log::lookup("lightning:ponger");

Ponger::Ponger(IOManager* ioManager,
               Socket::ptr socket,
               Address::ptr multicastGroup)
    : ioManager_(ioManager),
      socket_(socket),
      multicastGroup_(multicastGroup)
{}

void Ponger::run() {
    setupSocket();
    MORDOR_LOG_TRACE(g_log) << this << " listening @" <<
                               *socket_->localAddress() <<
                               " for multicasts @" << *multicastGroup_;
    Address::ptr remoteAddress = socket_->emptyAddress();
    while(true) {
        PingPacket currentPacket(0, 0);
        ssize_t bytes = socket_->receiveFrom((void*)&currentPacket,
                                             sizeof(currentPacket),
                                             *remoteAddress);
        socket_->sendTo((const void*)&currentPacket,
                        bytes,
                        0,
                        remoteAddress);
        MORDOR_LOG_TRACE(g_log) << this << " got (" << currentPacket.id <<
                                   ", " << currentPacket.senderNow <<
                                   ") from " << *remoteAddress;
    }
}

void Ponger::setupSocket() {
    struct ip_mreq mreq;
    // Ah, the royal ugliness.
    mreq.imr_multiaddr =
        ((struct sockaddr_in*)(multicastGroup_->name()))->sin_addr;
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    socket_->setOption(IPPROTO_IP, IP_ADD_MEMBERSHIP, mreq);
}


}
