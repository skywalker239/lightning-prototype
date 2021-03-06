#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include "value.h"
#include "vote.h"
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>

namespace lightning {
namespace paxos {

//! Represents a single consensus instance in an acceptor.
//  This class is not (thread|fiber) safe.
class AcceptorInstance {
public:
    typedef boost::shared_ptr<AcceptorInstance> ptr;
    //! Creates an empty instance.
    AcceptorInstance();

    //! Creates a committed instance (recovered elsewhere).
    AcceptorInstance(Value value,
                     BallotId ballotId);

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
                    Value* lastVote);
    
    //! Attempt to start Phase 2 of Paxos for ballot ballotId
    //  and value id valueId.
    //  On success returns true, on failure returns false.
    //  This is called on all acceptors in the ring when the value
    //  is multicast, and at this point nobody but the first
    //  acceptor in the ring can NACK, so don't bother NACKing at
    //  all. It will be done during the ring vote.
    bool beginBallot(BallotId ballotId,
                     Value value);

    //! Vote in ballotId for valueId.
    //  Returns true on success and false on failure.
    //  It can fail for two reasons:
    //    * Promised not to vote in a higher ballot. Sets
    //      highestBallotPromised accordingly.
    //    * The multicast packet with the corresponding value has been lost
    //      so this instance doesn't know the value with valueId.
    //      Sets highestBallotPromised to kInvalidBallotId.
    //      In this case the instance temporarily stores the vote so
    //      in the event of beginBallot and vote packets are reordered,
    //      the voting can still be resumed.
    bool vote(const Vote& voteData,
              BallotId* highestBallotPromised);

    //! Commit value id valueId to this instance.
    //  Returns false if we don't have the corresponding value.
    bool commit(const Guid& valueId);

    bool committed() const { return committed_; }

    //! If committed, retrieve value and the last voted ballot id
    //  and return true, otherwise return false.
    bool value(Value* value, BallotId* ballot) const;

    //! Reset this instance to the empty state.
    void reset();
private:
    BallotId highestPromisedBallot_;
    BallotId highestVotedBallot_;
    Value lastVotedValue_;
    boost::optional<Vote> pendingVote_;
    bool     committed_;
};

}  // namespace paxos
}  // namespace lightning
