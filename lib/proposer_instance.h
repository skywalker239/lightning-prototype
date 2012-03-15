#pragma once

#include "paxos_defs.h"
#include "value.h"
#include <boost/shared_ptr.hpp>

namespace lightning {
namespace paxos {

class ProposerInstance {
public:
    typedef boost::shared_ptr<ProposerInstance> ptr;

    ProposerInstance(InstanceId instance);

    void phase1Open(BallotId ballot);

    void phase1Pending(BallotId ballot);

    void phase2Pending(Value::ptr value, bool isClientValue);

    void close();

    InstanceId instanceId() const;

    BallotId ballotId() const;

    Value::ptr value() const;

    bool hasClientValue() const;

    Value::ptr releaseValue();

    bool operator<(const ProposerInstance& rhs) const;
private:
    const InstanceId instanceId_;

    BallotId ballotId_;
    Value::ptr value_;
    bool isClientValue_;
};

}  // namespace paxos
}  // namespace lightning
