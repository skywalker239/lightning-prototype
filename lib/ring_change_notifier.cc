#include "ring_change_notifier.h"
#include <boost/lexical_cast.hpp>
#include <mordor/socket.h>

namespace lightning {

using boost::lexical_cast;
using Mordor::Address;
using std::pair;
using std::string;
using std::vector;

RingChangeNotifier::RingChangeNotifier(
    const vector<pair<RingHolder::ptr, uint16_t> >& ringHolders)
    : ringHolders_(ringHolders)
{}

void RingChangeNotifier::onRingChange(const vector<string>& newRing,
                                      uint64_t newRingId) const
{
    for(size_t i = 0; i < ringHolders_.size(); ++i) {
        const string portString = ":" +
            lexical_cast<string>(ringHolders_[i].second);
        vector<Address::ptr> ringAddresses;
        for(size_t j = 0; j < newRing.size(); ++j) {
            ringAddresses.push_back(
                Address::lookup(newRing[i] + portString).front());
        }
        ringHolders_[i].first->resetRingConfiguration(
            RingConfiguration::ptr(new RingConfiguration(ringAddresses,                                                                                 newRingId)));
    }
}

void RingChangeNotifier::onRingDown() const {
    for(size_t i = 0; i < ringHolders_.size(); ++i) {
        ringHolders_[i].first->resetRingConfiguration(
            RingConfiguration::ptr());
    }
}

}  // namespace lightning
