#include "pong_receiver.h"
#include "ping_packet.h"
#include <mordor/log.h>
#include <mordor/timer.h>
#include <netinet/in.h>

namespace lightning {

using Mordor::IOManager;
using Mordor::Socket;
using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using Mordor::TimerManager;
using boost::function;

static Logger::ptr g_log = Log::lookup("lightning:pong_receiver");

PongReceiver::PongReceiver(IOManager* ioManager,
                           Socket::ptr socket,
                           function<
                               void (Mordor::Address::ptr,
                                     uint64_t,
                                     uint64_t,
                                     uint64_t)
                               > pongReceivedCallback)
    : ioManager_(ioManager),
      socket_(socket),
      pongReceivedCallback_(pongReceivedCallback)
{}

void PongReceiver::run() {
    MORDOR_LOG_TRACE(g_log) << this << " listening @" <<
                               *socket_->localAddress();
    while(true) {
        // Have to pass the address to the callback, so sadly
        // we need to allocate a fresh one every time.
        Address::ptr remoteAddress = socket_->emptyAddress();
        PingPacket currentPacket(0, 0);
        (void) socket_->receiveFrom((void*)&currentPacket,
                                    sizeof(currentPacket),
                                    *remoteAddress);
        uint64_t now = TimerManager::now();
        pongReceivedCallback_(remoteAddress,
                              currentPacket.id,
                              currentPacket.senderNow,
                              now);
        MORDOR_LOG_TRACE(g_log) << this << " got (" << currentPacket.id <<
                                   ", " << currentPacket.senderNow <<
                                   ") at " << now << " from " <<
                                   *remoteAddress;
    }
}

}  // namespace lightning
