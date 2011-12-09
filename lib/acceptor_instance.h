#pragma once

#include "paxos_defs.h"

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
                    ValueId*  highestVotedValueId);
    
    //! Attempt to start Phase 2 of Paxos for ballot ballotId
    //  and value id valueId.
    //  On success returns true, on failure returns false and
    //  sets the highest promised ballot id for this instance.
    bool beginBallot(BallotId ballotId,
                     ValueId  valueId,
                     BallotId* highestBallotPromised);

    //! Commit value id valueId to this instance.
    void commit(ValueId valueId);

    //! If committed, set valueId to the committed value id and return true,
    //  otherwise return false.
    bool value(ValueId* valueId) const;

    //! Reset this instance to the empty state.
    void reset();
private:
    BallotId highestBallotParticipated_;
    BallotId highestBallotVoted_;
    ValueId  highestVotedValueId_;
    ValueId  valueId_;
    bool     committed_;
};

}  // namespace paxos
}  // namespace lightning
