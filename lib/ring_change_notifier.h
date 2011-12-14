#pragma once

#include "ring_holder.h"
#include <utility>
#include <vector>

namespace lightning {

//! Notifies individual ring holders that the ring configuration
//  has changed.
class RingChangeNotifier {
public:
    typedef boost::shared_ptr<RingChangeNotifier> ptr;

    //! For each ring holder stores the reply port for the corresponding
    //  requester.
    RingChangeNotifier(
        const std::vector<std::pair<RingHolder::ptr, uint16_t> >&
            ringHolders);

    void onRingChange(const std::vector<std::string>& newRing,
                      uint64_t newRingId) const;

    void onRingDown() const;
private:
    const std::vector<std::pair<RingHolder::ptr, uint16_t> > ringHolders_;
};

}  // namespace lightning
