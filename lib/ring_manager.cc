#include "ring_manager.h"
#include "set_ring_request.h"
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

namespace {

//! XXX HACK
Address::ptr mangleAddress(Address::ptr addr, uint16_t port) {
    Address::ptr newAddr = addr->clone();
    dynamic_cast<IPv4Address*>(newAddr.get())->port(port);
    return newAddr;
}

}  // anonymous namespace

RingManager::RingManager(IOManager* ioManager,
                         boost::shared_ptr<FiberEvent> hostDownEvent,
                         SyncGroupRequester::ptr setRingRequester,
                         const vector<Address::ptr>& acceptors,
                         PingTracker::ptr acceptorPingTracker,
                         RingOracle::ptr ringOracle,
                         uint64_t lookupRingRetryUs)
    : ioManager_(ioManager),
      hostDownEvent_(hostDownEvent),
      setRingRequester_(setRingRequester),
      acceptors_(acceptors),
      acceptorPingTracker_(acceptorPingTracker),
      ringOracle_(ringOracle),
      lookupRingRetryUs_(lookupRingRetryUs),
      currentRingId_(kInvalidRingId),
      currentState_(LOOKING),
      mutex_(),
      nextRingId_(kInvalidRingId)
{}

bool RingManager::currentRing(uint64_t* ringId,
                              vector<Address::ptr>* acceptors)
{
    FiberMutex::ScopedLock lk(mutex_);

    if(currentRingId_ == kInvalidRingId) {
        return false;
    } else {
        *ringId = currentRingId_;
        acceptors->assign(currentRing_.begin(), currentRing_.end());
        return true;
    }
}

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
        vector<Address::ptr> nextRing;
        acceptorPingTracker_->snapshot(&pingStatsMap);
        if(ringOracle_->chooseRing(pingStatsMap, &nextRing)) {
            FiberMutex::ScopedLock lk(mutex_);
            nextRingId_ = generateRingId();
            nextRing_.swap(nextRing);
            //! XXX HACK!
            const uint16_t ringPort = dynamic_cast<IPv4Address*>(acceptors_.front().get())->port();
            for(size_t i = 0; i < nextRing_.size(); ++i) {
                nextRing_[i] = mangleAddress(nextRing_[i], ringPort);
            }
            //! end of HACK
            for(size_t i = 0; i < nextRing_.size(); ++i) {
                MORDOR_LOG_TRACE(g_log) << this << " #" << i << ": " << *nextRing_[i];
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

    SyncGroupRequest::ptr request(new SetRingRequest(nextRing_, nextRingId_));
    MORDOR_LOG_TRACE(g_log) << this << " try set ring id=" << nextRingId_;
    if(setRingRequester_->request(request) != SyncGroupRequest::OK) {
        MORDOR_LOG_TRACE(g_log) << this << " set ring id=" << nextRingId_ <<
                                   " failed";
        return false;
    } else {
        FiberMutex::ScopedLock lk(mutex_);
        currentRingId_ = nextRingId_;
        currentRing_.swap(nextRing_);
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
            //! XXX HACK
            const uint16_t port = dynamic_cast<IPv4Address*>(pingStatsMap.begin()->first.get())->port();
            Address::ptr mangledAddr = mangleAddress(currentRing_[i], port);

            auto pingStatsIter = pingStatsMap.find(mangledAddr);
            if(pingStatsIter != pingStatsMap.end()) {
                const Address::ptr& address = pingStatsIter->first;
                const PingStats& pingStats = pingStatsIter->second;
                if(pingStats.maxReceivedPongSendTime() +
                       noHeartbeatTimeoutUs < now)
                {
                    MORDOR_LOG_WARNING(g_log) << this << " ring member " <<
                                                 *address << " is down";
                    return;
                }
            } else {
                MORDOR_LOG_ERROR(g_log) << this << " no ping stats for " <<
                                           "ring member " <<
                                           *currentRing_[i];
            }
        }
        hostDownEvent_->reset();
    }
}

}  // namespace lightning
