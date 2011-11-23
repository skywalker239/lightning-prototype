#include "pinger.h"
#include "ping_packet.h"
#include <mordor/iomanager.h>
#include <mordor/log.h>
#include <mordor/timer.h>
#include <mordor/sleep.h>

namespace lightning {

using Mordor::Address;
using Mordor::IOManager;
using Mordor::TimerManager;
using Mordor::Socket;
using Mordor::Logger;
using Mordor::Log;
using Mordor::sleep;
using boost::function;

static Logger::ptr g_log = Log::lookup("lightning:pinger");

Pinger::Pinger(IOManager* ioManager,
               Socket::ptr socket,
               Address::ptr destinationAddress,
               uint64_t pingIntervalUs,
               uint64_t pingTimeoutUs,
               function<void (uint64_t, uint64_t)>
                   pingRegisterCallback,
               function<void (uint64_t)>
                   pingTimeoutCallback)
    : ioManager_(ioManager),
      socket_(socket),
      destinationAddress_(destinationAddress),
      pingIntervalUs_(pingIntervalUs),
      pingTimeoutUs_(pingTimeoutUs),
      pingRegisterCallback_(pingRegisterCallback),
      pingTimeoutCallback_(pingTimeoutCallback),
      currentId_(0)
{}

void Pinger::run() {
    setupSocket();
    MORDOR_LOG_TRACE(g_log) << this << " run @" << *socket_->localAddress() <<
                               " pinging " << *destinationAddress_ <<
                               " interval " << pingIntervalUs_ <<
                               " timeout " << pingTimeoutUs_;

    while(true) {
        PingPacket currentPacket(currentId_, TimerManager::now());
        ++currentId_;
        socket_->sendTo((const void*)&currentPacket,
                        sizeof(currentPacket),
                        0,
                        *destinationAddress_);
        pingRegisterCallback_(currentPacket.id, currentPacket.senderNow);
        ioManager_->schedule(boost::bind(&Pinger::waitAndTimeoutPing,
                                        this,
                                        currentPacket.id));
        MORDOR_LOG_TRACE(g_log) << this << " sent (" << currentPacket.id <<
                                   ", " << currentPacket.senderNow << ")";
        sleep(*ioManager_, pingIntervalUs_);
    }
}

void Pinger::setupSocket() {
    const int kMaxMulticastTtl = 255;
    socket_->setOption(IPPROTO_IP, IP_MULTICAST_TTL, kMaxMulticastTtl);
}

void Pinger::waitAndTimeoutPing(uint64_t id) {
    MORDOR_LOG_TRACE(g_log) << this << " waiting to time out ping " << id;
    sleep(*ioManager_, pingTimeoutUs_);
    pingTimeoutCallback_(id);
    MORDOR_LOG_TRACE(g_log) << this << " timed out ping " << id;
}

}  // namespace lightning
