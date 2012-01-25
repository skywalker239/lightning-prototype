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
    RingChangeNotifier(const std::vector<RingHolder::ptr>& ringHolders);

    void onRingChange(RingConfiguration::const_ptr newRing) const;

    void onRingDown() const;
private:
    const std::vector<RingHolder::ptr> ringHolders_;
};

}  // namespace lightning
