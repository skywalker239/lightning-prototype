#pragma once

#include "guid.h"
#include "instance_sink.h"
#include "paxos_defs.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <queue>
#include <utility>

namespace lightning {

//! Buffers the committed Paxos instances and delivers them in the
//  instance id order.
//  Single consumer only.
class BlockingAbcast : public InstanceSink {
public:
    BlockingAbcast();

    //! Pushes the (instanceId, value) binding for a given epoch.
    //  Changing the epoch resets the internal state of BlockingAbcast.
    virtual void push(const Guid& epoch,
                      paxos::InstanceId instanceId,
                      paxos::BallotId ballotId,
                      paxos::Value value);

    //! If current epoch of BlockingAbcast is equal to expectedEpoch,
    //  blocks until the next value is available, sets value to it and
    //  returns true.
    //  Otherwise returns false.
    bool nextValue(const Guid& expectedEpoch, paxos::Value* value);

    //! The current epoch.
    const Guid epoch() const;
private:
    void updateEpoch(const Guid& epoch);

    bool nextValueAvailable() const;

    typedef std::pair<paxos::InstanceId, paxos::Value> BoundValue;

    //! Compare by instance id.
    struct BoundValueCompare {
        bool operator()(const BoundValue& a, const BoundValue& b) const {
            return a.first < b.first;
        }
    };

    typedef std::priority_queue<BoundValue,
                                std::vector<BoundValue>,
                                BoundValueCompare>
            BoundValueHeap;

    Guid epoch_;
    BoundValueHeap valueBuffer_;
    paxos::InstanceId nextIdToDeliver_;

    Mordor::FiberEvent nextValueAvailable_;
    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
