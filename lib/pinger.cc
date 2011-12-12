#include "pinger.h"
#include "ping_requester.h"
#include <mordor/iomanager.h>
#include <mordor/log.h>
#include <mordor/timer.h>
#include <mordor/sleep.h>

namespace lightning {

using Mordor::Address;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using Mordor::TimerManager;
using Mordor::sleep;
using Mordor::Socket;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:pinger");

Pinger::Pinger(IOManager* ioManager,
               Socket::ptr socket,
               Address::ptr pingAddress,
               const vector<Address::ptr>& hosts,
               uint64_t pingIntervalUs,
               uint64_t pingTimeoutUs,
               PingTracker::ptr pingTracker)
    : ioManager_(ioManager),
      socket_(socket),
      pingAddress_(pingAddress),
      hosts_(hosts),
      pingIntervalUs_(pingIntervalUs),
      pingTimeoutUs_(pingTimeoutUs),
      pingTracker_(pingTracker),
      currentId_(0)
{}

void Pinger::run() {
    MORDOR_LOG_TRACE(g_log) << this << " run @" << *socket_->localAddress() <<
                               " pinging " << *pingAddress_ <<
                               " interval " << pingIntervalUs_ <<
                               " timeout " << pingTimeoutUs_;

    while(true) {
        ioManager_->schedule(boost::bind(&Pinger::doSinglePing,
                                         shared_from_this(),
                                         currentId_));
        MORDOR_LOG_TRACE(g_log) << this << " scheduled ping " << currentId_;
        ++currentId_;

        sleep(*ioManager_, pingIntervalUs_);
    }
}

void Pinger::doSinglePing(Pinger::ptr pinger, uint64_t id) {
    PingRequester requester(pinger->socket_,
                            pinger->pingAddress_,
                            pinger->hosts_,
                            pinger->pingTimeoutUs_,
                            pinger->pingTracker_);
    bool success = requester.ping(id);
    MORDOR_LOG_TRACE(g_log) << pinger.get() << " doSinglePing(" << id <<
                               ") = " << success;
}

}  // namespace lightning
