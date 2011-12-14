#pragma once

#include "sync_group_requester.h"
#include "ring_change_notifier.h"
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
                PingTracker::ptr acceptorPingTracker,
                RingOracle::ptr ringOracle,
                RingChangeNotifier::ptr ringChangeNotifier,
                uint16_t ringReplyPort,
                uint64_t lookupRingRetryUs);

    void run();
private:
    void lookupRing();

    bool trySetRing();

    void waitForRingToBreak();

    uint64_t generateRingId() const;

    void generateReplyAddresses(const std::vector<std::string>& hosts,
                                std::vector<Mordor::Address::ptr>* ring) const;

    //! Should redo this with ragel if it ever gets more states.
    enum State {
        LOOKING = 0,
        WAIT_ACK,
        OK
    };

    Mordor::IOManager* ioManager_;
    boost::shared_ptr<Mordor::FiberEvent> hostDownEvent_;
    SyncGroupRequester::ptr setRingRequester_;
    PingTracker::ptr acceptorPingTracker_;
    RingOracle::ptr ringOracle_;
    RingChangeNotifier::ptr ringChangeNotifier_;

    const uint16_t ringReplyPort_;
    const uint64_t lookupRingRetryUs_;

    static const uint64_t kInvalidRingId = 0;
    uint64_t currentRingId_;
    std::vector<std::string> currentRing_;

    State currentState_;
    Mordor::FiberMutex mutex_;

    uint64_t nextRingId_;
    std::vector<std::string> nextRing_;
};

}  // namespace lightning
