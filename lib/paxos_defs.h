#pragma once

#include <stdint.h>
#include <iostream>

namespace lightning {
namespace paxos {

//! Atomic broadcast instance id. Currently it's just
//  an unsigned integer and we deliver an ordered sequence from 0
//  to infinity.
typedef uint64_t InstanceId;

//! Coordinator epoch id. Must be unique and increasing wrt restarts
//  of a single coordinator process.
typedef uint32_t EpochId;
const EpochId kInvalidEpochId = 0;

//! A ballot number for a single Paxos instance.
typedef uint32_t BallotId;
const BallotId kInvalidBallotId = 0;

//! A value identifier on which consensus is run.
struct ValueId {
    EpochId epochId;
    uint32_t sequence;

    ValueId() 
        : epochId(kInvalidEpochId),
          sequence(0)
    {}

    ValueId(EpochId _epochId,
            uint32_t _sequence)
        : epochId(_epochId),
          sequence(_sequence)
    {}

    bool operator==(const ValueId& rhs) const {
        return epochId == rhs.epochId && sequence == rhs.sequence;
    }
} __attribute__((packed));

inline std::ostream& operator<<(std::ostream& os,
                                const ValueId& valueId)
{
    os << "ValueId(" << valueId.epochId << ", " << valueId.sequence << ")";
    return os;
}

}  // namespace paxos
}  // namespace lightning
