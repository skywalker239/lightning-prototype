#include "pinger.h"
#include "ping_request.h"
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
               SyncGroupRequester::ptr requester,
               const vector<Address::ptr>& hosts,
               uint64_t pingIntervalUs,
               PingTracker::ptr pingTracker)
    : ioManager_(ioManager),
      requester_(requester),
      hosts_(hosts),
      pingIntervalUs_(pingIntervalUs),
      pingTracker_(pingTracker),
      currentId_(0)
{}

void Pinger::run() {
    MORDOR_LOG_TRACE(g_log) << this << " requester=" << requester_ <<
                               " interval=" << pingIntervalUs_;

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
    SyncGroupRequest::ptr request(
        new PingRequest(pinger->hosts_,
                        id,
                        pinger->pingTracker_));
    SyncGroupRequest::Status status = pinger->requester_->request(request);
    MORDOR_LOG_TRACE(g_log) << pinger.get() << " doSinglePing(" << id <<
                               ") = " << status;
}

}  // namespace lightning
