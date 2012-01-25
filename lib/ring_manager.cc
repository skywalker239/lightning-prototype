#include "ring_manager.h"
#include "MurmurHash3.h"
#include "set_ring_request.h"
#include <boost/lexical_cast.hpp>
#include <mordor/log.h>
#include <mordor/sleep.h>
#include <mordor/timer.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sstream>

namespace lightning {

using Mordor::IOManager;
using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Socket;
using Mordor::TimerManager;
using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using boost::shared_ptr;
using std::vector;
using std::string;
using std::map;
using std::memcpy;
using std::ostringstream;

static Logger::ptr g_log = Log::lookup("lightning:ring_manager");

RingManager::RingManager(const GroupConfiguration& groupConfiguration,
                         const Guid& hostGroupGuid,
                         IOManager* ioManager,
                         boost::shared_ptr<FiberEvent> hostDownEvent,
                         MulticastRpcRequester::ptr requester,
                         PingTracker::ptr acceptorPingTracker,
                         RingOracle::ptr ringOracle,
                         RingChangeNotifier::ptr ringChangeNotifier,
                         uint64_t setRingTimeoutUs,
                         uint64_t lookupRingRetryUs)
    : groupConfiguration_(groupConfiguration),
      hostGroupGuid_(hostGroupGuid),
      setRingTimeoutUs_(setRingTimeoutUs),
      lookupRingRetryUs_(lookupRingRetryUs),
      ioManager_(ioManager),
      hostDownEvent_(hostDownEvent),
      requester_(requester),
      acceptorPingTracker_(acceptorPingTracker),
      ringOracle_(ringOracle),
      ringChangeNotifier_(ringChangeNotifier),
      currentRing_(),
      nextRing_(),
      currentState_(LOOKING),
      mutex_()
{
    //XXX for now on master only
    MORDOR_ASSERT(groupConfiguration.thisHostId() == 0);
}

void RingManager::run() {
    MORDOR_LOG_TRACE(g_log) << this << " starting up in LOOKING";
    currentState_ = LOOKING;
    while(true) {
        switch(currentState_) {
            case LOOKING:
                lookupRing();
                MORDOR_LOG_TRACE(g_log) << this << " LOOKING -> WAIT_ACK";
                currentState_ = WAIT_ACK;
                break;
            case WAIT_ACK:
                if(trySetRing()) {
                    MORDOR_LOG_TRACE(g_log) << this << " WAIT_ACK -> OK";
                    currentState_ = OK;
                } else {
                    MORDOR_LOG_TRACE(g_log) << this << " WAIT_ACK -> LOOKING";
                    currentState_ = LOOKING;
                }
                break;
            case OK:
                waitForRingToBreak();
                MORDOR_LOG_TRACE(g_log) << this << " OK -> LOOKING";
                currentState_ = LOOKING;
                break;
            default:
                MORDOR_ASSERT(false);
                break;
        }
    }
}

void RingManager::lookupRing() {
    while(true) {
        MORDOR_LOG_TRACE(g_log) << this << " looking up ring";
        PingTracker::PingStatsMap pingStatsMap;
        vector<uint32_t> newRing;
        acceptorPingTracker_->snapshot(&pingStatsMap);
        if(ringOracle_->chooseRing(pingStatsMap, &newRing)) {
            FiberMutex::ScopedLock lk(mutex_);
            const uint32_t newRingId = generateRingId(newRing);
            nextRing_.reset(new RingConfiguration(
                                newRing,
                                newRingId));
            for(size_t i = 0; i < newRing.size(); ++i) {
                MORDOR_LOG_TRACE(g_log) << this << " #" << i << ": " <<
                                           newRing[i];
            }
            MORDOR_LOG_TRACE(g_log) << this << " found next ring id=" <<
                                       newRingId;
            return;
        } else {
            MORDOR_LOG_TRACE(g_log) << this <<
                                       " ring lookup failed, waiting to retry";
            Mordor::sleep(*ioManager_, lookupRingRetryUs_);
        }
    }
}

bool RingManager::trySetRing() {
    MORDOR_ASSERT(nextRing_.get());

    vector<Address::ptr> nextRingAddresses;
    generateReplyAddresses(nextRing_->ringHostIds(), &nextRingAddresses);
    MulticastRpcRequest::ptr request(
        new SetRingRequest(
            hostGroupGuid_,
            nextRingAddresses,
            nextRing_->ringHostIds(),
            nextRing_->ringId()));
    MORDOR_LOG_TRACE(g_log) << this << " try set ring id=" <<
                               nextRing_->ringId();
    if(requester_->request(request, setRingTimeoutUs_) !=
           MulticastRpcRequest::COMPLETED)
    {
        MORDOR_LOG_TRACE(g_log) << this << " set ring id=" <<
                                   nextRing_->ringId() << " failed";
        return false;
    } else {
        FiberMutex::ScopedLock lk(mutex_);
        currentRing_ = nextRing_;
        nextRing_.reset();
        ringChangeNotifier_->onRingChange(currentRing_);
        MORDOR_LOG_TRACE(g_log) << this << " set ring id=" <<
                                   currentRing_->ringId() << " successful";
        return true;
    }
}

uint32_t RingManager::generateRingId(const vector<uint32_t>& ring) const {
    string binGuid;
    hostGroupGuid_.serialize(&binGuid);
    MORDOR_ASSERT(binGuid.length() == sizeof(Guid));

    char hashBuffer[sizeof(Guid) + sizeof(uint32_t)];
    memcpy(hashBuffer, binGuid.c_str(), sizeof(Guid));
    MurmurHash3_x86_32(&ring[0],
                       ring.size() * sizeof(uint32_t),
                       kHashSeed,
                       (void*) (hashBuffer + sizeof(Guid)));
    uint32_t ringId;
    MurmurHash3_x86_32(hashBuffer, sizeof(hashBuffer), kHashSeed, &ringId);
    return ringId;
}

void RingManager::generateReplyAddresses(
    const vector<uint32_t>& hostIds,
    vector<Address::ptr>* addresses) const
{
    addresses->clear();
    const vector<HostConfiguration>& allHosts = groupConfiguration_.hosts();
    for(size_t i = 0; i < hostIds.size(); ++i) {
        if(hostIds[i] != groupConfiguration_.thisHostId()) {
            addresses->push_back(allHosts[hostIds[i]].multicastReplyAddress);
        }
    }
}

void RingManager::waitForRingToBreak() {
    MORDOR_ASSERT(currentRing_.get());
    MORDOR_LOG_TRACE(g_log) << this << " waiting for ring " <<
                               currentRing_->ringId() << " to break";
    while(true) {
        hostDownEvent_->wait();
        MORDOR_LOG_TRACE(g_log) << this << " host down signaled";

        PingTracker::PingStatsMap pingStatsMap;
        acceptorPingTracker_->snapshot(&pingStatsMap);
        const uint64_t noHeartbeatTimeoutUs =
            acceptorPingTracker_->noHeartbeatTimeoutUs();
        const uint64_t now = TimerManager::now();

        const vector<uint32_t> ringHostIds = currentRing_->ringHostIds();
        for(size_t i = 0; i < ringHostIds.size(); ++i) {
            auto pingStatsIter = pingStatsMap.find(ringHostIds[i]);
            if(pingStatsIter != pingStatsMap.end()) {
                const uint32_t hostId = pingStatsIter->first;
                const PingStats& pingStats = pingStatsIter->second;
                if(pingStats.maxReceivedPongSendTime() +
                       noHeartbeatTimeoutUs < now)
                {
                    MORDOR_LOG_WARNING(g_log) << this << " ring member host=" <<
                                                 hostId << " is down";
                    ringChangeNotifier_->onRingDown();
                    return;
                }
            } else {
                MORDOR_LOG_ERROR(g_log) << this << " no ping stats for " <<
                                           "ring member " <<
                                           ringHostIds[i];
            }
        }
        hostDownEvent_->reset();
    }
}

}  // namespace lightning
