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

    void setBallotId(BallotId ballot);

    void setValue(Value value, bool isClientValue);

    InstanceId instanceId() const;

    BallotId ballotId() const;

    const Value& value() const;

    bool hasClientValue() const;

    Value releaseValue();

    bool operator<(const ProposerInstance& rhs) const;
private:
    const InstanceId instanceId_;

    BallotId ballotId_;
    Value value_;
    bool isClientValue_;
};

}  // namespace paxos
}  // namespace lightning
