#pragma once

#include "guid.h"
#include "acceptor_instance.h"
#include "paxos_defs.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <functional>
#include <map>
#include <queue>
#include <set>

namespace lightning {

class Vote;

//! The acceptor keeps track of some sliding window of consecutive Paxos
//  instances. At any point in time, the state consists of:
//  - Some range F = [0, f) of forgotten instances.
//    These are the instances that were committed
//    once but their values were discarded to save memory.
//  - A range A = [f, l) of active instances. It may contain holes (instances
//    that this acceptor has never seen yet)
//  - An infinite range [l, +\infty) of yet unused instances.
//
//  The following constraints are enforced:
//  * There are no more than pendingInstancesLimit pending instances in
//    range A. Pending instances are the ones that have received at least
//    one Paxos command but are not yet committed.
//  * Pending instances are never forgotten.
//  * Range A is no longer than instanceWindowSize. When A is at maximal
//    size, its right boundary l can only be extended by forgetting a
//    needed number of committed instances and shifting f.
//
//  When a fresh instance cannot be initialized due to these constraints,
//  the acceptor refuses the corresponding Paxos command.
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
        REFUSED // Paxos command refused because of
                // storage constraints.
    };

    AcceptorState(uint32_t pendingInstancesLimit,
                  uint32_t instanceWindowSize);

    Status nextBallot(InstanceId instanceId,
                      BallotId  ballotId,
                      BallotId* highestPromised,
                      BallotId* highestVoted,
                      Value*    lastVote);

    //! This is called upon receiving the Phase 2 multicast
    //  packet.
    Status beginBallot(InstanceId instanceId,
                       BallotId ballotId,
                       const Value& value);

    //! If this acceptor has not yet received a beginBallot
    //  corresponding to this vote, the corresponding instance
    //  will buffer the vote to account for UDP reordering.
    Status vote(const Vote& vote,
                BallotId* highestBallotPromised);

    Status commit(InstanceId instanceId,
                  const Guid& valueId);

    Status value(InstanceId instanceId,
                 Value* value) const;

    //! The first instance after the last forgotten one.
    InstanceId firstNotForgottenInstance() const;

    //! The first not committed instance (to hint off batch phase 1)
    InstanceId firstNotCommittedInstance() const;

    //! If epoch differs from the last seen one, reset() and
    //  update the last seen one.
    void updateEpoch(const Guid& epoch);

private:
    //! Reset the state to empty. Called on master epoch change.
    void reset();

    typedef std::map<InstanceId, AcceptorInstance> InstanceMap;

    //! Looks up the instance by its id. If not found, inserts it if
    //  possible (pendingInstancesLimit not reached and it is possible
    //  to shift the window if needed).
    //  Returns NULL iff not found and impossible to insert.
    AcceptorInstance* lookupInstance(InstanceId instanceId);

    //! Tries to forget all instances with iid < upperLimit.
    //  Returns true on success, false on failure (iff there's
    //  an uncommitted instance with an iid < upperLimit).
    //  Forgets nothing on failure..
    bool tryForgetInstances(InstanceId upperLimit);

    //! Retrieve first not committed instance id without grabbing the lock.
    InstanceId firstNotCommittedInstanceInternal() const;

    Status boolToStatus(const bool boolean) const;

    const uint32_t pendingInstancesLimit_;
    const uint32_t instanceWindowSize_;

    //! The last known master epoch.
    Guid epoch_;

    //! Stores all pending and committed instances.
    //  Eviction and limits enforcement are handled by
    //  tracking committed instance ids and pending instances count.
    InstanceMap instances_;

    InstanceId firstNotForgottenInstanceId_;
    uint64_t pendingInstanceCount_;


    //! Invariant: the instance ids not yet committed are
    //  notCommittedInstanceIds_ \cup
    //  [afterLastCommittedInstanceId_, infinity).
    std::set<InstanceId> notCommittedInstanceIds_;
    InstanceId afterLastCommittedInstanceId_;
    //! Adjusts pendingInstanceCount and the not committed iid
    //  tracker.
    void addCommittedInstanceId(InstanceId instanceId);

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
