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

void ProposerInstance::phase1Open(BallotId ballot) {
    MORDOR_ASSERT(ballot > ballotId_);

    MORDOR_LOG_TRACE(g_log) << this << " phase1Open(" << ballot << ")" <<
                               "iid=" << instanceId_;
    ballotId_ = ballot;
    MORDOR_ASSERT(!isClientValue_);
    value_.reset();
}

void ProposerInstance::phase1Pending(BallotId ballot) {
    MORDOR_ASSERT(ballot > ballotId_);

    MORDOR_LOG_TRACE(g_log) << this << " phase1Pending(" << ballot << ")" <<
                               " iid=" << instanceId_ << " isClientValue=" <<
                               isClientValue_;
    ballotId_ = ballot;
}

void ProposerInstance::phase2Pending(Value::ptr value, bool isClientValue) {
    MORDOR_ASSERT(!isClientValue_ || (value->valueId == value_->valueId));
    MORDOR_LOG_TRACE(g_log) << this << " phase2Pending(" << value->valueId <<
                               ", " << isClientValue << ") iid=" <<
                               instanceId_;
    value_ = value;
    isClientValue_ = isClientValue;
}

void ProposerInstance::close() {
    MORDOR_ASSERT(value_.get());
    MORDOR_LOG_TRACE(g_log) << this << " close iid=" << instanceId_ <<
                               " ballot=" << ballotId_ << " valueId=" <<
                               value_->valueId;
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
    Value::ptr value = value_;
    value_.reset();
    isClientValue_ = false;
    return value_;
}

bool ProposerInstance::operator<(const ProposerInstance& rhs) const {
    return instanceId_ < rhs.instanceId_;
}

}  // namespace paxos
}  // namespace lightning
