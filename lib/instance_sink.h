#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include "value.h"

namespace lightning {

//! Consumes committed instances.
class InstanceSink {
public:
    virtual ~InstanceSink() {}

    virtual void push(const Guid& epoch,
                      paxos::InstanceId instance,
                      paxos::BallotId   ballot,
                      paxos::Value value) = 0;
};

}  // namespace lightning
