#pragma once

#include "host_configuration.h"
#include "paxos_defs.h"

namespace lightning {
namespace paxos {

//! Generates ballot ids for a given host.
//  Since the group configuration is static for now,
//  uniqueness is guaranteed simply by generating
//  ids of form 1 + hostId + k * numHosts for successive
//  values of k. (adding 1 is necessary because 0 is the
//  'never participated in any' ballot id.
class BallotGenerator {
public:
    BallotGenerator(GroupConfiguration::ptr groupConfiguration);

    //! Return the first 'our' ballot id greater than
    //  both ballotId.
    BallotId boostBallotId(BallotId ballotId) const;
private:
    const uint32_t hostId_;
    const uint32_t hostNumber_;
};
    

}  // namespace paxos
}  // namespace lightning
