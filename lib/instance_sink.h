#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include "value.h"
#include <boost/shared_ptr.hpp>

namespace lightning {

//! Consumes committed instances.
class InstanceSink {
public:
    typedef boost::shared_ptr<InstanceSink> ptr;

    virtual ~InstanceSink() {}

    virtual void updateEpoch(const Guid& newEpoch) = 0;

    virtual void push(paxos::InstanceId instanceId,
                      paxos::BallotId   ballotId,
                      paxos::Value value) = 0;
};

}  // namespace lightning
