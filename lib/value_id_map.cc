#include "value_id_map.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {
namespace paxos {

using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using std::make_pair;

static Logger::ptr g_log = Log::lookup("lightning:paxos:value_id_map");

ValueIdMap::ValueIdMap(const uint32_t maximumSize)
    : valueRingBuffer_(maximumSize),
      valueRingBufferSize_(maximumSize),
      bufferBeginIndex_(0),
      bufferEndIndex_(0)
{}

void ValueIdMap::addMapping(const ValueId& valueId,
                            const Value& value)
{
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << this << " add mapping for value id " <<
                               valueId;

    if(ringBufferFull()) {
        evictFirstElement();
    }

    const uint32_t insertIndex = arrayIndex(bufferEndIndex_);
    valueRingBuffer_[insertIndex] = make_pair(valueId, value);
    auto inserted = valueIndexMap_.insert(make_pair(valueId, bufferEndIndex_));
    //! We require ValueId's to be unique.
    MORDOR_ASSERT(inserted.second);
    ++bufferEndIndex_;
}

bool ValueIdMap::lookup(const ValueId& valueId) const {
    FiberMutex::ScopedLock lk(mutex_);

    auto valueIdIter = lookupUnlocked(valueId);
    return valueIdIter != valueIndexMap_.end();
}

bool ValueIdMap::fetch(const ValueId& valueId,
                       Value* value) const
{
    FiberMutex::ScopedLock lk(mutex_);

    auto valueIdIter = lookupUnlocked(valueId);
    if(valueIdIter == valueIndexMap_.end()) {
        return false;
    } else {
        const uint32_t index = valueIdIter->second;
        MORDOR_ASSERT(bufferBeginIndex_ <= index &&
                      index < bufferEndIndex_);
        MORDOR_ASSERT(valueId == valueRingBuffer_[arrayIndex(index)].first);
        *value = valueRingBuffer_[arrayIndex(index)].second;
        return true;
    }
}

size_t ValueIdMap::size() const {
    FiberMutex::ScopedLock lk(mutex_);
    return valueIndexMap_.size();
}

ValueIdMap::ValueIndexMap::const_iterator ValueIdMap::lookupUnlocked(
    const ValueId& valueId) const
{
    return valueIndexMap_.find(valueId);
}

bool ValueIdMap::ringBufferFull() const {
    return (bufferEndIndex_ - bufferBeginIndex_) == valueRingBufferSize_;
}

void ValueIdMap::evictFirstElement() {
    const uint32_t index = arrayIndex(bufferBeginIndex_);
    const ValueId& valueId = valueRingBuffer_[index].first;
    MORDOR_LOG_TRACE(g_log) << this << " evicting value id " << valueId;

    ++bufferBeginIndex_;
    valueIndexMap_.erase(valueId);
    
    MORDOR_ASSERT(bufferBeginIndex_ <= bufferEndIndex_ &&
                  bufferEndIndex_ - bufferBeginIndex_ <= valueRingBufferSize_);
}

inline uint32_t ValueIdMap::arrayIndex(const uint32_t index) const {
    return index % valueRingBufferSize_;
}

}  // namespace paxos
}  // namespace lightning
