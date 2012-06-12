#pragma once

#include "guid.h"
#include "acceptor_instance.h"
#include "commit_tracker.h"
#include "paxos_defs.h"
#include "recovery_manager.h"
#include "value.h"
#include "value_cache.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>
#include <boost/enable_shared_from_this.hpp>
#include <functional>
#include <map>
#include <queue>
#include <set>

namespace lightning {

class Vote;

//! TODO(skywalker): document the new acceptor state logic.
class AcceptorState : public boost::enable_shared_from_this<AcceptorState> {
    typedef paxos::AcceptorInstance AcceptorInstance;
    typedef paxos::InstanceId InstanceId;
    typedef paxos::BallotId   BallotId;
    typedef paxos::Value      Value;
public:
    typedef boost::shared_ptr<AcceptorState> ptr;

    enum Status {
        OK,     // Paxos command succeeded
        NACKED, // Paxos command failed
        TOO_OLD,// instance was forgotten
        REFUSED // Paxos command refused because of
                // storage constraints.
    };

    AcceptorState(uint32_t pendingInstancesSpan,
                  Mordor::IOManager* ioManager,
                  RecoveryManager::ptr recoveryManager,
                  CommitTracker::ptr   commitTracker,
                  ValueCache::ptr      valueCache);

    Status nextBallot(const Guid& epoch,
                      InstanceId instanceId,
                      BallotId  ballotId,
                      BallotId* highestPromised,
                      BallotId* highestVoted,
                      Value*    lastVote);

    //! This is called upon receiving the Phase 2 multicast
    //  packet.
    Status beginBallot(const Guid& epoch,
                       InstanceId instanceId,
                       BallotId ballotId,
                       const Value& value);

    //! If this acceptor has not yet received a beginBallot
    //  corresponding to this vote, the corresponding instance
    //  will buffer the vote to account for UDP reordering.
    Status vote(const Guid& epoch,
                const Vote& vote,
                BallotId* highestBallotPromised);

    Status commit(const Guid& epoch,
                  InstanceId instanceId,
                  const Guid& valueId);

    InstanceId firstNotCommittedInstanceId(const Guid& epoch);
private:
    void updateEpoch(const Guid& epoch);
    ValueCache::QueryResult tryNextBallotOnCommitted(InstanceId instanceId,
                                                     BallotId   ballotId,
                                                     BallotId*  highestPromised,
                                                     BallotId*  highestVoted,
                                                     Value*     lastVote);

    ValueCache::QueryResult tryBeginBallotOnCommitted(InstanceId instanceId,
                                                      BallotId   ballotId,
                                                      const Value& value);

    ValueCache::QueryResult tryVoteOnCommitted(const Vote& vote,
                                               BallotId* highestBallotPromised);

    ValueCache::QueryResult tryCommitOnCommitted(InstanceId instanceId,
                                                 const Guid& valueId);

    //! Reset the state to empty. Called on master epoch change.
    void reset();

    typedef std::map<InstanceId, AcceptorInstance> InstanceMap;

    //! Looks up the instance by its id. If not found, inserts it if
    //  possible.
    //  Returns NULL iff not found and impossible to insert.
    AcceptorInstance* lookupInstance(InstanceId instanceId);

    Status boolToStatus(const bool boolean) const;

    //! Checks whether a new instance can be inserted
    //  without exceeding the pending instances span limit.
    bool canInsert(InstanceId instanceId) const;

    void startRecovery(const Guid epoch, InstanceId instanceId);

    const uint32_t pendingInstancesSpan_;

    //! The last known master epoch.
    Guid epoch_;

    //! Stores all pending (not committed) instances.
    InstanceMap pendingInstances_;

    Mordor::IOManager* ioManager_;
    RecoveryManager::ptr recoveryManager_;
    CommitTracker::ptr   commitTracker_;
    ValueCache::ptr      valueCache_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
