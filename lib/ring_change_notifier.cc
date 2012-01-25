#include "ring_change_notifier.h"
#include "ring_configuration.h"

namespace lightning {

using std::pair;
using std::string;
using std::vector;

RingChangeNotifier::RingChangeNotifier(
    const vector<RingHolder::ptr>& ringHolders)
    : ringHolders_(ringHolders)
{}

void RingChangeNotifier::onRingChange(
    RingConfiguration::const_ptr newRing) const 
{
    for(size_t i = 0; i < ringHolders_.size(); ++i) {
        ringHolders_[i]->resetRingConfiguration(newRing);
    }
}

void RingChangeNotifier::onRingDown() const {
    for(size_t i = 0; i < ringHolders_.size(); ++i) {
        ringHolders_[i]->resetRingConfiguration(
            RingConfiguration::const_ptr());
    }
}

}  // namespace lightning
