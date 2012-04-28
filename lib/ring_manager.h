#pragma once

#include "guid.h"
#include "rpc_requester.h"
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

//! This manages the ring configuration on master.
class RingManager : boost::noncopyable {
public:
    typedef boost::shared_ptr<RingManager> ptr;

    RingManager(GroupConfiguration::ptr groupConfiguration,
                const Guid& hostGroupGuid,
                Mordor::IOManager* ioManager,
                boost::shared_ptr<Mordor::FiberEvent> hostDownEvent,
                RpcRequester::ptr requester,
                PingTracker::ptr acceptorPingTracker,
                RingOracle::ptr ringOracle,
                RingChangeNotifier::ptr ringChangeNotifier,
                uint64_t setRingTimeoutUs,
                uint64_t lookupRingRetryUs,
                uint64_t ringBroadcastIntervalUs);

    void run();

    void broadcastRing();
private:
    void lookupRing();

    bool trySetRing();

    void waitForRingToBreak();

    uint32_t generateRingId(const std::vector<uint32_t>& ring) const;

    //! Should redo this with ragel if it ever gets more states.
    enum State {
        LOOKING = 0,
        WAIT_ACK,
        OK
    };

    GroupConfiguration::ptr groupConfiguration_;
    const Guid hostGroupGuid_;
    const uint64_t setRingTimeoutUs_;
    const uint64_t lookupRingRetryUs_;
    const uint64_t ringBroadcastIntervalUs_;

    Mordor::IOManager* ioManager_;
    boost::shared_ptr<Mordor::FiberEvent> hostDownEvent_;
    RpcRequester::ptr requester_;
    PingTracker::ptr acceptorPingTracker_;
    RingOracle::ptr ringOracle_;
    RingChangeNotifier::ptr ringChangeNotifier_;

    RingConfiguration::ptr currentRing_;
    RingConfiguration::ptr nextRing_;

    State currentState_;
    Mordor::FiberMutex mutex_;

    static const uint32_t kHashSeed = 239;
};

}  // namespace lightning
