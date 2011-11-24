#include "ponger.h"
#include "multicast_util.h"
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
    joinMulticastGroup(socket_, multicastGroup_);
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

}  // namespace lightning
