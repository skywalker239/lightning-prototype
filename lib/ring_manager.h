#pragma once

#include "ring_oracle.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <map>
#include <set>
#include <vector>

namespace lightning {

class PingTracker;

class RingManager {
public:
    RingManager(Mordor::IOManager* ioManager,
                Mordor::FiberEvent* hostDownEvent,
                Mordor::Socket::ptr socket,
                Mordor::Address::ptr multicastGroup,
                const std::vector<Mordor::Address::ptr>& acceptors,
                PingTracker* acceptorPingTracker,
                RingOracle* ringOracle,
                uint64_t lookupRingRetryUs,
                uint64_t setRingTimeoutUs);

    //! False if no current ring.
    bool currentRing(uint64_t* ringId,
                     std::vector<Mordor::Address::ptr>* acceptors);
private:
    void run();

    void lookupRing();

    bool trySetRing();

    void waitForRingToBreak();

    void receiveSetRingAcks();

    //! The weak pointer is a hack to monitor that the receiveSetRingAcks
    //  that launched this instance of timeoutSetRing
    //  is still running.
    void timeoutSetRing(uint64_t id,
                        boost::weak_ptr<int> alivePtr);

    uint64_t generateRingId() const;

    std::string generateRingString(
        const std::vector<Mordor::Address::ptr>& ring) const;

    //! Should redo this with ragel if it ever gets more states.
    enum State {
        LOOKING = 0,
        WAIT_ACK,
        OK
    };

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                        const Mordor::Address::ptr& rhs)
        {
            return *lhs < *rhs;
        }
    };

    struct SetRingPacket {
        uint64_t ringId;
        uint64_t ringStringLength;
        char ringString[0];
    } __attribute__((packed));

    struct RingAckPacket {
        uint64_t ringId;
    } __attribute__((packed));

    Mordor::IOManager* ioManager_;
    Mordor::FiberEvent* hostDownEvent_;
    Mordor::Socket::ptr socket_;
    Mordor::Address::ptr multicastGroup_;
    std::vector<Mordor::Address::ptr> acceptors_;
    PingTracker* acceptorPingTracker_;
    RingOracle* ringOracle_;
    const uint64_t lookupRingRetryUs_;
    const uint64_t setRingTimeoutUs_;

    static const uint64_t kInvalidRingId = 0;
    uint64_t currentRingId_;
    std::vector<Mordor::Address::ptr> currentRing_;

    State currentState_;
    Mordor::FiberMutex mutex_;
    Mordor::FiberCondition setRingAckCondition_;

    uint64_t nextRingId_;
    std::vector<Mordor::Address::ptr> nextRing_;
    std::set<Mordor::Address::ptr, AddressCompare> nextRingNotYetAcked_;
    bool nextRingTimedOut_;
};

}  // namespace lightning
