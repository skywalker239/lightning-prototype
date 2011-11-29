#include "ring_manager.h"
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
using Mordor::FiberCondition;
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
                         Socket::ptr socket,
                         Address::ptr multicastGroup,
                         const vector<Address::ptr>& acceptors,
                         PingTracker::ptr acceptorPingTracker,
                         RingOracle::ptr ringOracle,
                         uint64_t lookupRingRetryUs,
                         uint64_t setRingTimeoutUs)
    : ioManager_(ioManager),
      hostDownEvent_(hostDownEvent),
      socket_(socket),
      multicastGroup_(multicastGroup),
      acceptors_(acceptors),
      acceptorPingTracker_(acceptorPingTracker),
      ringOracle_(ringOracle),
      lookupRingRetryUs_(lookupRingRetryUs),
      setRingTimeoutUs_(setRingTimeoutUs),
      currentRingId_(kInvalidRingId),
      currentState_(LOOKING),
      mutex_(),
      setRingAckCondition_(mutex_),
      nextRingId_(kInvalidRingId),
      nextRingTimedOut_(false)
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
    setupSocket();
    MORDOR_LOG_TRACE(g_log) << this << " running @" <<
                               *socket_->localAddress() << " ctrl address " <<
                               multicastGroup_;
    ioManager_->schedule(boost::bind(&RingManager::receiveSetRingAcks,
                                     this));
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
            nextRingNotYetAcked_ =
                std::set<Address::ptr, AddressCompare>
                    (nextRing_.begin(), nextRing_.end());
            nextRingTimedOut_ = false;
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
    FiberMutex::ScopedLock lk_(mutex_);
    MORDOR_ASSERT(nextRingId_ != kInvalidRingId);

    const uint32_t kMaxPacketLength = 8000;
    MORDOR_ASSERT(sizeof(SetRingPacket) <= kMaxPacketLength);
    char packetBuffer[kMaxPacketLength];
    SetRingPacket* packet = reinterpret_cast<SetRingPacket*>(packetBuffer);

    packet->ringId = nextRingId_;
    auto ringString = generateRingString(nextRing_);
    packet->ringStringLength = ringString.length();
    uint64_t packetSize = sizeof(SetRingPacket) + packet->ringStringLength;
    MORDOR_ASSERT(packetSize + 1 <= kMaxPacketLength);
    memcpy(packet->ringString,
           ringString.c_str(),
           packet->ringStringLength + 1);

    MORDOR_LOG_TRACE(g_log) << this << " try set ring id=" << nextRingId_ <<
                               " ring='" << ringString << '\'';

    socket_->sendTo((const void*)packet, packetSize, 0, *multicastGroup_);

    shared_ptr<int> alivePtr(new int);
    ioManager_->schedule(
        boost::bind(&RingManager::timeoutSetRing,
                    this,
                    nextRingId_,
                    alivePtr));

    while(true) {
        setRingAckCondition_.wait();

        if(nextRingTimedOut_) {
            MORDOR_LOG_TRACE(g_log) << this << " set ring id=" << nextRingId_ <<
                                       " timed out";
            return false;
        } else {
            if(nextRingNotYetAcked_.empty()) {
                MORDOR_LOG_TRACE(g_log) << this << " got all acks for ring id=" <<
                                           nextRingId_;
                currentRingId_ = nextRingId_;
                currentRing_.assign(nextRing_.begin(), nextRing_.end());
                return true;
            }
            MORDOR_LOG_TRACE(g_log) << this << " set ring id=" << nextRingId_ <<
                                       " woken up";
        }
    }
}

void RingManager::receiveSetRingAcks() {
    MORDOR_LOG_TRACE(g_log) << this << " started receiveSetRingAcks";
    Address::ptr remoteAddress = socket_->emptyAddress();
    while(true) {
        RingAckPacket ringAckPacket;
        socket_->receiveFrom((void*)&ringAckPacket,
                             sizeof(ringAckPacket),
                             *remoteAddress);
        MORDOR_LOG_TRACE(g_log) << this << " got ring ack from " <<
                                   *remoteAddress << " for ring id=" <<
                                   ringAckPacket.ringId;
        {
            FiberMutex::ScopedLock lk(mutex_);
            if(currentState_ != WAIT_ACK) {
                MORDOR_LOG_WARNING(g_log) << this << " got ack while " <<
                                             " not in WAIT_ACK state";
                continue;
            }
            if(ringAckPacket.ringId != nextRingId_) {
                MORDOR_LOG_WARNING(g_log) << this << " got ack for id " <<
                                             ringAckPacket.ringId <<
                                             ", but nextRingId=" <<
                                             nextRingId_;
                continue;
            }

            nextRingNotYetAcked_.erase(remoteAddress);
            setRingAckCondition_.signal();
        }
    }
}

void RingManager::timeoutSetRing(uint64_t id, boost::weak_ptr<int> alivePtr) {
    sleep(*ioManager_, setRingTimeoutUs_);
    MORDOR_LOG_TRACE(g_log) << this << " timing out set ring id=" << id;
    {
        FiberMutex::ScopedLock lk(mutex_);
        if(alivePtr.expired()) {
            MORDOR_LOG_TRACE(g_log) << this << " alivePtr reset, " <<
                                       "nothing to time out";
            return;
        }
        if(id != nextRingId_) {
            MORDOR_LOG_WARNING(g_log) << this << " nextRing=" <<
                                         nextRingId_ <<
                                         " while timing out";
            return;
        }
        nextRingTimedOut_ = true;
        setRingAckCondition_.signal();
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

string RingManager::generateRingString(
    const vector<Address::ptr>& ring) const
{
    ostringstream ss;
    for(size_t i = 0; i < ring.size(); ++i) {
        ss << *ring[i];
        if(i + 1 < ring.size()) {
            ss << " ";
        }
    }
    return ss.str();
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

void RingManager::setupSocket() {
    const int kMaxMulticastTtl = 255;
    socket_->setOption(IPPROTO_IP, IP_MULTICAST_TTL, kMaxMulticastTtl);
}

}  // namespace lightning
