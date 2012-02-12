#include "ring_configuration.h"
#include <mordor/assert.h>
#include <algorithm>

namespace lightning {

using Mordor::Address;
using std::find;
using std::ostream;
using std::vector;

RingConfiguration::RingConfiguration(
    GroupConfiguration::ptr groupConfiguration,
    const vector<uint32_t>& ringHostIds,
    uint32_t ringId)
    : groupConfiguration_(groupConfiguration),
      ringHostIds_(ringHostIds),
      ringId_(ringId),
      ringMask_(0)
{
    const uint32_t myId = groupConfiguration_->thisHostId();
    for(size_t i = 0; i < ringHostIds.size(); ++i) {
        MORDOR_ASSERT(ringHostIds[i] < groupConfiguration_->size());
        if(ringHostIds[i] != myId) {
            ringMask_ |= (1 << ringHostIds[i]);
        }
    }

    auto thisHostIter = find(ringHostIds.begin(), ringHostIds.end(), myId);
    if(thisHostIter != ringHostIds.end()) {
        ringIndex_ = thisHostIter - ringHostIds.begin();
        const size_t nextRingIndex = (ringIndex_ + 1) % ringHostIds.size();
        const uint32_t nextHostIndex = ringHostIds[nextRingIndex];
        if(nextRingIndex != 0) {
            nextRingAddress_ =
                groupConfiguration_->host(nextHostIndex).ringAddress;
        } else {
            // XXX The master waits for the vote on its multicast source address.
            nextRingAddress_ =
                groupConfiguration_->host(nextHostIndex).multicastSourceAddress;
        }
        const uint32_t lastHostId = ringHostIds.back();
        lastRingAddress_ = groupConfiguration_->host(lastHostId).ringAddress;
    } else {
        ringIndex_ = kInvalidRingIndex;
    }
}

ostream& operator<<(ostream& os, const RingConfiguration& ringConfiguration) {
    os << "Ring(" << ringConfiguration.ringId() << ", [";
    const GroupConfiguration::ptr& groupConfiguration =
        ringConfiguration.groupConfiguration_;
    const vector<uint32_t>& ringHostIds = ringConfiguration.ringHostIds_;

    for(size_t i = 0; i < ringHostIds.size(); ++i) {
        os << groupConfiguration->host(ringHostIds[i]).name;
        if(i + 1 < ringHostIds.size()) {
            os << ", ";
        }
    }
    os << "])";
    return os;
}

}  // namespace lightning
