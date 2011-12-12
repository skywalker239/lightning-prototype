#include "proposer_instance.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {
namespace paxos {

using boost::shared_ptr;
using Mordor::Logger;
using Mordor::Log;

static Logger::ptr g_log = Log::lookup("lightning:proposer_instance");

ProposerInstance::ProposerInstance(InstanceId instanceId)
    : state_(EMPTY),
      instanceId_(instanceId),
      currentBallotId_(kInvalidBallotId),
      value_()
{}

void ProposerInstance::phase1Open(BallotId ballotId) {
    MORDOR_ASSERT(state_ == EMPTY ||
                  (state_ == P1_PENDING && ballotId == currentBallotId_));
    auto stateString = (state_ == EMPTY) ? "EMPTY" : "P1_PENDING";

    MORDOR_LOG_TRACE(g_log) << this << " iid = " << instanceId_ << " " <<
                            stateString << " -> P1_OPEN, " << "ballotId = "
                            << ballotId;
    state_ = P1_OPEN;
    currentBallotId_ = ballotId;
    value_.reset();
}

void ProposerInstance::phase1Pending(BallotId ballotId) {
    MORDOR_ASSERT(state_ == EMPTY ||
                  (state_ == P2_PENDING && ballotId > currentBallotId_));
    auto stateString = (state_ == EMPTY) ? "EMPTY" : "P2_PENDING";

    MORDOR_LOG_TRACE(g_log) << this << " iid = " << instanceId_ << " " <<
                               stateString << " -> P1_PENDING, " <<
                               "ballotId = " << ballotId;
    state_ = P1_PENDING;
    currentBallotId_ = ballotId;
    value_.reset();
}

void ProposerInstance::phase1Retry(BallotId nextBallotId) {
    MORDOR_ASSERT(state_ == P1_PENDING && nextBallotId > currentBallotId_);
    MORDOR_ASSERT(!value_.get());
    MORDOR_LOG_TRACE(g_log) << this << " iid = " << instanceId_ <<
                            " P1_PENDING -> P1_PENDING, oldBallotId = " <<
                            currentBallotId_ << ", nextBallotId = " <<
                            nextBallotId;
    currentBallotId_ = nextBallotId;
}

void ProposerInstance::phase2Pending(shared_ptr<Value> value) {
    MORDOR_ASSERT(state_ == P1_PENDING || state_ == P1_OPEN);
    MORDOR_ASSERT(value.get());
    auto stateString = (state_ == P1_PENDING) ? "P1_PENDING" : "P1_OPEN";

    MORDOR_LOG_TRACE(g_log) << this << " iid = " << instanceId_ << " " <<
                               stateString << " -> P2_PENDING, ballotId = " <<
                               currentBallotId_;
    state_ = P2_PENDING;
    value_ = value;
}

void ProposerInstance::close() {
    MORDOR_ASSERT(state_ == P2_PENDING);
    MORDOR_LOG_TRACE(g_log) << this << " iid = " << instanceId_ <<
                               "P2_PENDING -> CLOSED, ballotId = " <<
                               currentBallotId_;
    MORDOR_ASSERT(value_.get());
    state_ = CLOSED;
}

ProposerInstance::State ProposerInstance::state() const {
    return state_;
}

InstanceId ProposerInstance::instanceId() const {
    return instanceId_;
}

BallotId ProposerInstance::ballotId() const {
    return currentBallotId_;
}

shared_ptr<Value> ProposerInstance::value() const {
    MORDOR_ASSERT(state_ == CLOSED);
    return value_;
}

void ProposerInstance::reset(InstanceId newInstanceId) {
//    MORDOR_ASSERT(state_ == EMPTY || state_ == CLOSED);
    auto stateString = (state_ == EMPTY) ? "EMPTY" : "CLOSED";
    MORDOR_LOG_TRACE(g_log) << this << " iid = " << instanceId_ << " " <<
                               stateString << " -> EMPTY (reset), " <<
                               "new iid = " << newInstanceId;
    state_ = EMPTY;
    instanceId_ = newInstanceId;
    currentBallotId_ = kInvalidBallotId;
    value_.reset();
}

}  // namespace paxos
}  // namespace lightning