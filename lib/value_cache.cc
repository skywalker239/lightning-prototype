#include "value_cache.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using std::make_pair;

static Logger::ptr g_log = Log::lookup("lightning:value_cache");

ValueCache::ValueCache(uint64_t cacheSize)
    : cacheSize_(cacheSize),
      firstNotForgottenInstanceId_(0)
{}

void ValueCache::updateEpoch(const Guid& newEpoch) {
    MORDOR_ASSERT(epoch_ != newEpoch);
    MORDOR_LOG_TRACE(g_log) << this << " epoch change: " <<
        epoch_ << " -> " << newEpoch;
    ValueMap emptyValueMap;
    {
        FiberMutex::ScopedLock lk(mutex_);
        firstNotForgottenInstanceId_ = 0;
        emptyValueMap.swap(valueMap_);
    }
}

void ValueCache::push(InstanceId instanceId,
                      BallotId,
                      Value      value)
{
    FiberMutex::ScopedLock lk(mutex_);
    MORDOR_LOG_TRACE(g_log) << this << " push(" << instanceId << ", " <<
        value << ")";
    if(instanceId < firstNotForgottenInstanceId_) {
        MORDOR_LOG_WARNING(g_log) << this << " iid " << instanceId <<
            " already forgotten";
        return;
    }

    MORDOR_ASSERT(valueMap_.find(instanceId) == valueMap_.end());
    if(valueMap_.size() == cacheSize_) {
        forgetEarliestInstance();
        MORDOR_ASSERT(valueMap_.size() < cacheSize_);
    }
    valueMap_.insert(make_pair(instanceId, value));
}

void ValueCache::forgetEarliestInstance() {
    MORDOR_ASSERT(!valueMap_.empty());
    auto iter = valueMap_.begin();
    MORDOR_ASSERT(iter->first == firstNotForgottenInstanceId_);
    valueMap_.erase(iter);
}

ValueCache::QueryResult ValueCache::query(const Guid& epoch,
                                          InstanceId instanceId,
                                          Value* value) const
{
    FiberMutex::ScopedLock lk(mutex_);
    if(epoch != epoch_) {
        MORDOR_LOG_TRACE(g_log) << this << " query(" <<
            epoch << ", " << instanceId << ") = WRONG_EPOCH(" <<
            epoch_ << ")";
        return WRONG_EPOCH;
    } else if(instanceId < firstNotForgottenInstanceId_) {
        MORDOR_LOG_TRACE(g_log) << this << " query(" <<
            epoch << ", " << instanceId << ") = TOO_OLD(" <<
            firstNotForgottenInstanceId_ << ")";
        return TOO_OLD;
    }

    auto iter = valueMap_.find(instanceId);
    if(iter != valueMap_.end()) {
        *value = iter->second;
        MORDOR_LOG_TRACE(g_log) << this << " query(" <<
            epoch << ", " << instanceId << ") = OK(" <<
            *value << ")";
        return OK;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " query(" <<
            epoch << ", " << instanceId << ") = NOT_YET";
        return NOT_YET;
    }
}

}  // namespace lightning
