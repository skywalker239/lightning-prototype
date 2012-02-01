#pragma once

#include <stdint.h>
#include <iostream>

namespace lightning {
namespace paxos {

//! Atomic broadcast instance id. Currently it's just
//  an unsigned integer and we deliver an ordered sequence from 0
//  to infinity.
typedef uint64_t InstanceId;

//! A ballot number for a single Paxos instance.
typedef uint32_t BallotId;
const BallotId kInvalidBallotId = 0;

}  // namespace paxos
}  // namespace lightning
