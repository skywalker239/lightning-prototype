#pragma once

#include "paxos_defs.h"
#include "value.h"
#include <boost/shared_ptr.hpp>

namespace lightning {
namespace paxos {

//! Represents a single consensus instance in a proposer.
//
//  The instance may be in the following states:
//    * EMPTY -- marks a fresh or invalid instance.
//    * P1_PENDING -- a used instance for which phase 1 of
//      Paxos is being executed.
//    * P1_OPEN -- phase 1 completed successfully, but there
//      is no value to bind to this instance yet.
//    * P2_PENDING -- Paxos has selected a value to be bound
//      to this instance and phase 2 is pending.
//    * CLOSED -- a value has been bound to this instance.
//
//   All possible state transitions are [TODO a pretty graph]:
//
//   * EMPTY -> P1_OPEN
//     For efficiency, we run phase 1 in batches because in the
//     normal course of operation (no process failures) all new
//     Paxos instances the proposer has started will be fresh
//     and will have no votes. More precisely, for a requested
//     window of instances the batch phase 1 will return a list
//     of empty instances and a list of instances for which phase 1
//     must be re-run (this might be due to them having reserved
//     values and/or the coordinator's ballot number being too low).
//     The former ones are immediately promoted to P1_OPEN, the
//     latter -- to P1_PENDING and phase 1 is scheduled to be
//     completely re-run on them.
//
//   * EMPTY -> P1_PENDING
//     Here go previously reserved instances and instances which have
//     nacked the proposer's ballot number. See the
//     EMPTY -> P1_OPEN description.
//
//   * P1_PENDING -> P1_PENDING [2 reasons]
//     Phase 1 may timeout. It may also be nacked.
//
//   * P1_PENDING -> P1_OPEN
//     If an individual phase 1 for an instance succeeds with no
//     reserved value, it becomes an open instance (available for
//     a client value)
//
//   * P1_PENDING -> P2_PENDING
//     Instances for which phase 1 has discovered a reserved value go this way.
//
//   * P1_OPEN -> P2_PENDING
//     Happens when we try to bind a fresh client value to an open instance.
//
//   * P2_PENDING -> P1_PENDING
//     When phase 2 timeouts, we have to rerun phase 1 individually and from
//     scratch.
//
//   * P2_PENDING -> CLOSED
//     The instance becomes closed.
//
//   * CLOSED -> EMPTY [implementation detail]
//     Happens when an instance is recycled.
//
//  Ring Paxos is advantageous in storing the proposer state in that
//  we do not need to keep track of individual P2 votes; they either
//  arrive in a batch from the second-last node in the ring or not at all,
//  causing P2 to timeout.
//  This however delegates the complexity of maintaining the quorum to
//  whatever manages the ring [TODO explain more].
//
//  This class is not (thread|fiber) safe.
class ProposerInstance {
public:
    typedef boost::shared_ptr<ProposerInstance> ptr;

    enum State {
        EMPTY,
        P1_PENDING,
        P1_OPEN,
        P2_PENDING,
        CLOSED
    };

    //! Creates an EMPTY instance.
    ProposerInstance(InstanceId instanceId);

    //! EMPTY -> P1_OPEN or P1_PENDING -> P1_OPEN
    //  We must only set the ballot id that passed phase 1.
    //  In case of P1_PENDING -> P1_OPEN, ballotId MUST be equal to
    //  the current ballot id.
    void phase1Open(BallotId ballotId);

    //! EMPTY -> P1_PENDING or P2_PENDING -> P1_PENDING
    //  We really only know the ballot id with which to (re)try, so we
    //  set it here.
    void phase1Pending(BallotId ballotId);

    //! P1_PENDING -> P1_PENDING
    //  A timeout or nack happened.
    //  Both are treated identically: we retry with a higher ballot id.
    //  Selecting it is delegated to the user.
    //  It MUST be greater than the current ballot id.
    void phase1Retry(BallotId nextBallotId);

    //! P1_PENDING -> P2_PENDING or P1_OPEN -> P2_PENDING
    //  Proceed to phase 2 with a known value.
    void phase2Pending(boost::shared_ptr<Value> value);

    //! P2_PENDING -> CLOSED
    void close();

    //! Current state of the instance.
    State state() const;

    //! Current instance id.
    InstanceId instanceId() const;

    //! Current ballot id.
    BallotId ballotId() const;

    //! The value bound to this instance.
    //  May be only called when state() == CLOSED.
    boost::shared_ptr<Value> value() const;

    //! CLOSED -> EMPTY
    void reset(InstanceId newInstanceId);

    //! Instances are totally ordered by their ids.
    bool operator<(const ProposerInstance& rhs) const;
private:
    State state_;
    InstanceId instanceId_;

    BallotId currentBallotId_;
    boost::shared_ptr<Value> value_;
};

}  // namespace paxos
}  // namespace lightning
