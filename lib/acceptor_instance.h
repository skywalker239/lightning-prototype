#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include "value.h"

namespace lightning {
namespace paxos {

//! Represents a single consensus instance in an acceptor.
//  This class is not (thread|fiber) safe.
class AcceptorInstance {
public:
    //! Creates an empty instance.
    AcceptorInstance();

    //! Attempt to start Phase 1 of Paxos on this instance with
    //  ballot ballotId.
    //  On success returns true and sets
    //  (highestBallotParticipated, highestBallotVoted, highestVotedValueId)
    //  to be returned to the proposer.
    //  On failure (promised not to participate in this ballot)
    //  returns false and sets highestBallotParticipated to inform the
    //  proposer.
    bool nextBallot(BallotId ballotId,
                    BallotId* highestBallotParticipated,
                    BallotId* highestBallotVoted,
                    Value*    lastVote);
    
    //! Attempt to start Phase 2 of Paxos for ballot ballotId
    //  and value id valueId.
    //  On success returns true, on failure returns false.
    //  This is called on all acceptors in the ring when the value
    //  is multicast, and at this point nobody but the first
    //  acceptor in the ring can NACK, so don't bother NACKing at
    //  all. It will be done during the ring vote.
    bool beginBallot(BallotId ballotId,
                     const Value& value);

    //! Vote in ballotId for valueId.
    //  Returns true on success and false on failure.
    //  It can fail for two reasons:
    //    * Promised not to vote in a higher ballot. Sets
    //      highestBallotPromised accordingly.
    //    * The multicast packet with the corresponding value has been lost
    //      so this instance doesn't know the value with valueId.
    //      Sets highestBallotPromised to kInvalidBallotId.
    bool vote(BallotId ballotId,
              const Guid& valueId,
              BallotId* highestBallotPromised);

    //! Commit value id valueId to this instance.
    //  Returns false if we don't have the corresponding value.
    bool commit(const Guid& valueId);

    //! If committed, retrieve value and return true,
    //  otherwise return false.
    bool value(Value* value) const;

    //! Reset this instance to the empty state.
    void reset();
private:
    BallotId highestBallotParticipated_;
    BallotId highestBallotVoted_;
    Value    lastVotedValue_;
    Guid     committedValueId_;
    bool     committed_;
};

}  // namespace paxos
}  // namespace lightning
