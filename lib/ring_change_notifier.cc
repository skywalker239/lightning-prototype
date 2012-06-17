#include "ring_change_notifier.h"
#include "ring_configuration.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Log;
using Mordor::Logger;
using std::pair;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ring_change_notifier");

RingChangeNotifier::RingChangeNotifier(
    const vector<RingHolder::ptr>& ringHolders)
    : ringHolders_(ringHolders)
{}

void RingChangeNotifier::onRingChange(
    RingConfiguration::const_ptr newRing) const 
{
    MORDOR_LOG_DEBUG(g_log) << this << " ring change " << *newRing;
    for(size_t i = 0; i < ringHolders_.size(); ++i) {
        ringHolders_[i]->resetRingConfiguration(newRing);
    }
}

void RingChangeNotifier::onRingDown() const {
    MORDOR_LOG_INFO(g_log) << this << " ring down";
    for(size_t i = 0; i < ringHolders_.size(); ++i) {
        ringHolders_[i]->resetRingConfiguration(
            RingConfiguration::const_ptr());
    }
}

}  // namespace lightning
