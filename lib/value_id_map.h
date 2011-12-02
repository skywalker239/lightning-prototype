#pragma once

#include "paxos_defs.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <boost/noncopyable.hpp>
#include <vector>
#include <ext/hash_map>

namespace lightning {
namespace paxos {

//! Stores the mapping of value ids to values for
//  some window of value ids.
//  This class is fiber-safe.
class ValueIdMap : boost::noncopyable {
public:
    //! Store at most maximumSize values.
    ValueIdMap(const uint32_t maximumSize);

    //! Adds a new mapping. If there is already
    //  maximumSize elements in the map, the
    //  least recently added one is evicted.
    void addMapping(const ValueId& valueId,
                    const Value& value);
    
    //! True if valueId is present in the map.
    bool lookup(const ValueId& valueId) const;

    //! Returns false on lookup failure.
    bool fetch(const ValueId& valueId,
               Value* value) const;

    //! Number of items currently in the mapping.
    size_t size() const;
public:
    struct ValueIdHasher {
        __gnu_cxx::hash<uint64_t> hasher_;
        size_t operator()(const ValueId& valueId) const {
            const uint64_t value = (uint64_t(valueId.epochId) << 32) + valueId.sequence;
            return hasher_(value);
        }
    };

    typedef __gnu_cxx::hash_map<ValueId, uint32_t, ValueIdHasher>
            ValueIndexMap;
private:
    ValueIndexMap::const_iterator lookupUnlocked(
        const ValueId& valueId) const;

    //! True if we need to evict something in order to insert a
    //  new value.
    bool ringBufferFull() const;

    void evictFirstElement();

    uint32_t arrayIndex(const uint32_t index) const;
    
    std::vector<Value> valueRingBuffer_;
    const uint32_t valueRingBufferSize_;
    uint32_t bufferBeginIndex_;
    uint32_t bufferEndIndex_;
    //! ValueId -> index in the ring buffer.
    ValueIndexMap valueIndexMap_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace paxos
}  // namespace lightning
