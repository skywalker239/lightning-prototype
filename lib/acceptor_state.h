#pragma once

#include "guid.h"
#include "acceptor_instance.h"
#include "paxos_defs.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <functional>
#include <map>
#include <set>

namespace lightning {

//! The acceptor keeps track of:
//    * up to pendingInstancesLimit pending Paxos instances.
//    * up to committedInstancesLimit committed instances.
//  When pendingInstancesLimit is reached, it starts refusing
//  commands which would create fresh instances.
//  When committedInstancesLimit is reached and a new instance
//  is committed, a committed instance with the lowest iid
//  is evicted from storage.
class AcceptorState {
    typedef paxos::AcceptorInstance AcceptorInstance;
    typedef paxos::InstanceId InstanceId;
    typedef paxos::BallotId   BallotId;
    typedef paxos::Value      Value;
public:
    typedef boost::shared_ptr<AcceptorState> ptr;

    enum Status {
        OK,     // Paxos command succeeded
        NACKED, // Paxos command failed
        REFUSED // Paxos command refused because
                // pendingInstancesLimit was reached.
    };

    AcceptorState(uint32_t pendingInstancesLimit,
                  uint32_t committedInstancesLimit);

    Status nextBallot(InstanceId instanceId,
                      BallotId  ballotId,
                      BallotId* highestPromised,
                      BallotId* highestVoted,
                      Value*    lastVote);

    Status beginBallot(InstanceId instanceId,
                       BallotId ballotId,
                       const Value& value);

    Status vote(InstanceId instanceId,
                BallotId ballotId,
                const Guid& valueId,
                BallotId* highestBallotPromised);

    Status commit(InstanceId instanceId,
                  const Guid& valueId);

    Status value(InstanceId instanceId,
                 Value* value) const;

    //! Lowest unknown-or-pending instance id.
    InstanceId lowestInstanceId() const;

    //! Reset the state to empty. Called on master epoch change.
    void reset();
private:
    typedef std::map<InstanceId, AcceptorInstance> InstanceMap;

    //! Looks up the instances in pending and committed instance
    //  maps.
    //  Returns NULL iff it was not found and pendingInstancesLimit
    //  has been reached.
    //  If it has been found, returns a pointer to it.
    //  Otherwise (not found, but pendingInstancesLimit not reached)
    //  inserts a fresh instance into pendingInstances_ and returns
    //  a pointer to it.
    AcceptorInstance* lookupInstance(InstanceId instanceId);

    Status boolToStatus(const bool boolean) const;

    std::map<InstanceId, AcceptorInstance> pendingInstances_;
    std::map<InstanceId, AcceptorInstance> committedInstances_;

    const uint32_t pendingInstancesLimit_;
    const uint32_t committedInstancesLimit_;

    //! Invariant: the instance ids not yet committed are
    //  notCommittedInstanceIds_ \cup
    //  [afterLastCommittedInstanceId_, infinity).
    void addCommittedInstanceId(InstanceId instanceId);
    std::set<InstanceId> notCommittedInstanceIds_;
    InstanceId afterLastCommittedInstanceId_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
