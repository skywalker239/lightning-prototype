#include "ballot_generator.h"
#include <mordor/assert.h>

namespace lightning {
namespace paxos {

BallotGenerator::BallotGenerator(GroupConfiguration::ptr groupConfiguration)
    : hostId_(groupConfiguration->thisHostId()),
      hostNumber_(groupConfiguration->size())
{}

BallotId BallotGenerator::boostBallotId(BallotId ballotId) const {
    if(ballotId == kInvalidBallotId) {
        return 1 + hostId_;
    } else {
        // ballotId = 1 + otherHostId + k * hostNumber_
        uint32_t k = (ballotId - 1) / hostNumber_;
        BallotId ballot = 1 + hostId_ + (k + 1) * hostNumber_;
        MORDOR_ASSERT(ballot > ballotId);
        return ballot;
    }
}

}  // namespace paxos
}  // namespace lightning
