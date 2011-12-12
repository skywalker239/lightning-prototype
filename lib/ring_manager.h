#pragma once

#include "sync_group_requester.h"
#include "ring_oracle.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <boost/noncopyable.hpp>
#include <map>
#include <set>
#include <vector>

namespace lightning {

class PingTracker;

class RingManager : boost::noncopyable {
public:
    typedef boost::shared_ptr<RingManager> ptr;

    RingManager(Mordor::IOManager* ioManager,
                boost::shared_ptr<Mordor::FiberEvent> hostDownEvent,
                SyncGroupRequester::ptr setRingRequester,
                const std::vector<Mordor::Address::ptr>& acceptors,
                PingTracker::ptr acceptorPingTracker,
                RingOracle::ptr ringOracle,
                uint64_t lookupRingRetryUs);

    //! False if no current ring.
    bool currentRing(uint64_t* ringId,
                     std::vector<Mordor::Address::ptr>* acceptors);

    void run();
private:
    void lookupRing();

    bool trySetRing();

    void waitForRingToBreak();

    uint64_t generateRingId() const;

    //! Should redo this with ragel if it ever gets more states.
    enum State {
        LOOKING = 0,
        WAIT_ACK,
        OK
    };

    Mordor::IOManager* ioManager_;
    boost::shared_ptr<Mordor::FiberEvent> hostDownEvent_;
    SyncGroupRequester::ptr setRingRequester_;
    std::vector<Mordor::Address::ptr> acceptors_;
    PingTracker::ptr acceptorPingTracker_;
    RingOracle::ptr ringOracle_;
    const uint64_t lookupRingRetryUs_;

    static const uint64_t kInvalidRingId = 0;
    uint64_t currentRingId_;
    std::vector<Mordor::Address::ptr> currentRing_;

    State currentState_;
    Mordor::FiberMutex mutex_;

    uint64_t nextRingId_;
    std::vector<Mordor::Address::ptr> nextRing_;
};

}  // namespace lightning
