#include "ring_manager.h"
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

using boost::lexical_cast;
using Mordor::IOManager;
using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Socket;
using Mordor::TimerManager;
using Mordor::Address;
using Mordor::IPv4Address;
using Mordor::Logger;
using Mordor::Log;
using boost::shared_ptr;
using std::vector;
using std::string;
using std::map;
using std::memcpy;
using std::ostringstream;

static Logger::ptr g_log = Log::lookup("lightning:ring_manager");

RingManager::RingManager(IOManager* ioManager,
                         boost::shared_ptr<FiberEvent> hostDownEvent,
                         SyncGroupRequester::ptr setRingRequester,
                         PingTracker::ptr acceptorPingTracker,
                         RingOracle::ptr ringOracle,
                         RingChangeNotifier::ptr ringChangeNotifier,
                         uint16_t ringReplyPort,
                         uint64_t lookupRingRetryUs)
    : ioManager_(ioManager),
      hostDownEvent_(hostDownEvent),
      setRingRequester_(setRingRequester),
      acceptorPingTracker_(acceptorPingTracker),
      ringOracle_(ringOracle),
      ringChangeNotifier_(ringChangeNotifier),
      ringReplyPort_(ringReplyPort),
      lookupRingRetryUs_(lookupRingRetryUs),
      currentRingId_(kInvalidRingId),
      currentState_(LOOKING),
      mutex_(),
      nextRingId_(kInvalidRingId)
{}

void RingManager::run() {
    MORDOR_LOG_TRACE(g_log) << this << " RingManager::run() requester=" <<
                               setRingRequester_;
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
        vector<string> nextRing;
        acceptorPingTracker_->snapshot(&pingStatsMap);
        if(ringOracle_->chooseRing(pingStatsMap, &nextRing)) {
            FiberMutex::ScopedLock lk(mutex_);
            nextRingId_ = generateRingId();
            nextRing_.swap(nextRing);
            for(size_t i = 0; i < nextRing_.size(); ++i) {
                MORDOR_LOG_TRACE(g_log) << this << " #" << i << ": " << nextRing_[i];
            }
            MORDOR_LOG_TRACE(g_log) << this << " found next ring " <<
                                       nextRingId_;
            return;
        } else {
            MORDOR_LOG_TRACE(g_log) << this <<
                                       " ring lookup failed, waiting to retry";
            Mordor::sleep(*ioManager_, lookupRingRetryUs_);
        }
    }
}

bool RingManager::trySetRing() {
    MORDOR_ASSERT(nextRingId_ != kInvalidRingId);

    vector<Address::ptr> nextRingAddresses;
    generateReplyAddresses(nextRing_, &nextRingAddresses);
    SyncGroupRequest::ptr request(
        new SetRingRequest(nextRingAddresses, nextRing_, nextRingId_));
    MORDOR_LOG_TRACE(g_log) << this << " try set ring id=" << nextRingId_;
    if(setRingRequester_->request(request) != SyncGroupRequest::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " set ring id=" << nextRingId_ <<
                                   " failed";
        return false;
    } else {
        FiberMutex::ScopedLock lk(mutex_);
        currentRingId_ = nextRingId_;
        currentRing_.swap(nextRing_);
        ringChangeNotifier_->onRingChange(currentRing_, currentRingId_);
        MORDOR_LOG_TRACE(g_log) << this << " set ring id=" << nextRingId_ <<
                                   " ok";
        return true;
    }
}

//! Only called under lock in lookupRing().
uint64_t RingManager::generateRingId() const {
    uint64_t currentRandom;
    do {
        currentRandom = random();
    } while(currentRandom == currentRingId_ ||
            currentRandom == kInvalidRingId);
    return currentRandom;
}

void RingManager::generateReplyAddresses(
    const vector<string>& hosts,
    vector<Address::ptr>* addresses) const
{
    addresses->clear();
    const string portString = ":" + lexical_cast<string>(ringReplyPort_);
    for(size_t i = 0; i < hosts.size(); ++i) {
        addresses->push_back(Address::lookup(hosts[i] + portString).front());
    }
}

void RingManager::waitForRingToBreak() {
    MORDOR_LOG_TRACE(g_log) << this << " waiting for ring " <<
                               currentRingId_ << " to break";
    while(true) {
        hostDownEvent_->wait();
        MORDOR_LOG_TRACE(g_log) << this << " host down signaled";

        PingTracker::PingStatsMap pingStatsMap;
        acceptorPingTracker_->snapshot(&pingStatsMap);
        const uint64_t noHeartbeatTimeoutUs =
            acceptorPingTracker_->noHeartbeatTimeoutUs();
        const uint64_t now = TimerManager::now();

        for(size_t i = 0; i < currentRing_.size(); ++i) {
            auto pingStatsIter = pingStatsMap.find(currentRing_[i]);
            if(pingStatsIter != pingStatsMap.end()) {
                const string& address = pingStatsIter->first;
                const PingStats& pingStats = pingStatsIter->second;
                if(pingStats.maxReceivedPongSendTime() +
                       noHeartbeatTimeoutUs < now)
                {
                    MORDOR_LOG_WARNING(g_log) << this << " ring member " <<
                                                 address << " is down";
                    ringChangeNotifier_->onRingDown();
                    return;
                }
            } else {
                MORDOR_LOG_ERROR(g_log) << this << " no ping stats for " <<
                                           "ring member " <<
                                           currentRing_[i];
            }
        }
        hostDownEvent_->reset();
    }
}

}  // namespace lightning
