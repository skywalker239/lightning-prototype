#include "proposer_instance.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {
namespace paxos {

using Mordor::Log;
using Mordor::Logger;

static Logger::ptr g_log = Log::lookup("lightning:proposer_instance");

ProposerInstance::ProposerInstance(InstanceId instanceId)
    : instanceId_(instanceId),
      ballotId_(kInvalidBallotId),
      isClientValue_(false)
{}

void ProposerInstance::setBallotId(BallotId ballot) {
    MORDOR_LOG_TRACE(g_log) << this << " iid=" << instanceId_ <<
                               " set ballot=" << ballot;
    ballotId_ = ballot;
}

void ProposerInstance::setValue(Value::ptr value, bool isClientValue) {
    MORDOR_ASSERT(!value_.get() || (value_->valueId == value->valueId));

    MORDOR_LOG_TRACE(g_log) << this << " iid=" << instanceId_ <<
                               " set value=" << value->valueId <<
                               ", client=" << isClientValue;
    value_ = value;
    isClientValue_ = isClientValue;
}

InstanceId ProposerInstance::instanceId() const {
    return instanceId_;
}

BallotId ProposerInstance::ballotId() const {
    return ballotId_;
}

Value::ptr ProposerInstance::value() const {
    return value_;
}

bool ProposerInstance::hasClientValue() const {
    return isClientValue_;
}

Value::ptr ProposerInstance::releaseValue() {
    MORDOR_ASSERT(value_.get());
    MORDOR_LOG_TRACE(g_log) << this << " iid=" << instanceId_ <<
                               " release value=" << value_->valueId <<
                               ", client=" << isClientValue_;
    Value::ptr value = value_;
    value_.reset();
    isClientValue_ = false;
    return value;
}

bool ProposerInstance::operator<(const ProposerInstance& rhs) const {
    return instanceId_ < rhs.instanceId_;
}

}  // namespace paxos
}  // namespace lightning
