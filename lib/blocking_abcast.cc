#include "blocking_abcast.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::Value;
using std::make_pair;

static Logger::ptr g_log = Log::lookup("lightning:blocking_abcast");

BlockingAbcast::BlockingAbcast()
    : nextIdToDeliver_(0),
      nextValueAvailable_(false)
{
}

void BlockingAbcast::push(const Guid& epoch,
                          InstanceId instanceId,
                          BallotId,
                          Value value)
{
    FiberMutex::ScopedLock lk(mutex_);
    updateEpoch(epoch);

    valueBuffer_.push(make_pair(instanceId, value));
    if(instanceId == nextIdToDeliver_) {
        MORDOR_LOG_TRACE(g_log) << this << " next value " <<
                                   nextIdToDeliver_ << " now available";
        nextValueAvailable_.set();
    }

    MORDOR_LOG_TRACE(g_log) << this << " push(" << instanceId << ", " <<
                               value << "), nextIdToDeliver=" <<
                               nextIdToDeliver_;
}

bool BlockingAbcast::nextValue(const Guid& expectedEpoch,
                               Value* value)
{
    nextValueAvailable_.wait();
    FiberMutex::ScopedLock lk(mutex_);
    if(epoch_ != expectedEpoch) {
        MORDOR_LOG_TRACE(g_log) << this << " nextValue: expectedEpoch=" <<
                                   expectedEpoch << ", have " << epoch_;
        return false;
    }

    if(!nextValueAvailable()) {
        return false;
    }

    const auto& top = valueBuffer_.top();
    MORDOR_ASSERT(top.first == nextIdToDeliver_);
    MORDOR_LOG_TRACE(g_log) << this << " delivering (" << top.first << ", " <<
                               top.second << ")";
    *value = top.second;

    valueBuffer_.pop();
    ++nextIdToDeliver_;

    if(!nextValueAvailable()) {
        MORDOR_LOG_TRACE(g_log) << this << " next value " <<
                                   nextIdToDeliver_ << " not available";
        nextValueAvailable_.reset();
    }

    return true;
}

const Guid BlockingAbcast::epoch() const {
    FiberMutex::ScopedLock lk(mutex_);
    return epoch_;
}

void BlockingAbcast::updateEpoch(const Guid& epoch) {
    if(epoch != epoch_) {
        MORDOR_LOG_TRACE(g_log) << this << " epoch change: " << epoch_ <<
                                   " -> " << epoch;
    valueBuffer_ = BoundValueHeap();
    nextIdToDeliver_ = 0;
    nextValueAvailable_.reset();
    }
}

bool BlockingAbcast::nextValueAvailable() const {
    if(valueBuffer_.empty()) {
        return false;
    }
    return valueBuffer_.top().first == nextIdToDeliver_;
}

}  // namespace lightning
