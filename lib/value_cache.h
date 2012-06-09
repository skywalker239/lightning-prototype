#pragma once

#include "acceptor_instance.h"
#include "instance_sink.h"
#include "paxos_defs.h"
#include <mordor/fibersynchronization.h>
#include <boost/shared_ptr.hpp>
#include <map>

namespace lightning {

//! Caches committed values on an acceptor for recovery.
class ValueCache : public InstanceSink {
public:
    typedef boost::shared_ptr<ValueCache> ptr;

    ValueCache(uint64_t cacheSize);

    virtual void updateEpoch(const Guid& newEpoch);

    virtual void push(paxos::InstanceId instanceId,
                      paxos::BallotId   ballotId,
                      paxos::Value      value);

    enum QueryResult {
        TOO_OLD = 0,
        OK = 1,
        NOT_YET = 2,
        WRONG_EPOCH = 3
    };

    QueryResult query(const Guid& epoch,
                      paxos::InstanceId instanceId,
                      paxos::Value* value) const;

private:
    void forgetEarliestInstance();

    typedef std::map<paxos::InstanceId, paxos::Value> ValueMap;

    const uint64_t cacheSize_;

    Guid epoch_;

    paxos::InstanceId firstNotForgottenInstanceId_;
    ValueMap valueMap_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
