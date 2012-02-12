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
               MulticastRpcRequester::ptr requester,
               GroupConfiguration::ptr groupConfiguration,
               uint64_t pingIntervalUs,
               uint64_t pingTimeoutUs,
               PingTracker::ptr pingTracker)
    : ioManager_(ioManager),
      requester_(requester),
      pingIntervalUs_(pingIntervalUs),
      pingTimeoutUs_(pingTimeoutUs),
      pingTracker_(pingTracker),
      currentId_(0)
{
    const uint32_t kDummyRingId = 239;
    vector<uint32_t> hostIds;
    for(size_t i = 0; i < groupConfiguration->size(); ++i) {
        if(i != groupConfiguration->thisHostId()) {
            hostIds.push_back(i);
        }
    }
    pingRing_.reset(new RingConfiguration(groupConfiguration,
                                          hostIds,
                                          kDummyRingId));
}

void Pinger::run() {
    MORDOR_LOG_TRACE(g_log) << this << " requester=" << requester_ <<
                               " interval=" << pingIntervalUs_ <<
                               " timeout=" << pingTimeoutUs_;

    while(true) {
        ioManager_->schedule(boost::bind(&Pinger::doSinglePing,
                                         shared_from_this(),
                                         currentId_));
        MORDOR_LOG_TRACE(g_log) << this << " scheduled ping " << currentId_;
        ++currentId_;

        sleep(*ioManager_, pingIntervalUs_);
    }
}

void Pinger::doSinglePing(uint64_t id) {
    MulticastRpcRequest::ptr request(
        new PingRequest(pingRing_,
                        id,
                        pingTracker_));
    MulticastRpcRequest::Status status =
        requester_->request(request, pingTimeoutUs_);
    MORDOR_LOG_TRACE(g_log) << this << " doSinglePing(" << id <<
                               ") = " << uint32_t(status);
}

}  // namespace lightning
