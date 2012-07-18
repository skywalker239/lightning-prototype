#include "ring_var.h"
#include <mordor/sleep.h>
#include <mordor/log.h>
#include <stdexcept>
#include <sstream>

namespace lightning {

using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using std::istringstream;
using std::logic_error;
using std::ostringstream;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ring_var");

RingVar::RingVar(const string& key,
                 ConfigurationStore::ptr store,
                 IOManager* ioManager)
    : ConfigurationVarBase(key, store),
      ringId_(kInvalidRingId),
      ioManager_(ioManager)
{}

bool RingVar::valid() const {
    return ringId_ != kInvalidRingId;
}

void RingVar::waitForValidRing(uint64_t pollIntervalUs) {
    while(!valid()) {
        MORDOR_LOG_TRACE(g_log) << this << " Ring is invalid, waiting for " <<
            pollIntervalUs << " us.";
        sleep(*ioManager_, pollIntervalUs);
        update();
    }
}

uint32_t RingVar::ringId() const {
    return ringId_;
}

const vector<uint32_t>& RingVar::ringHostIds() const {
    return ringHostIds_;
}

void RingVar::reset(uint32_t ringId,
                    const std::vector<uint32_t>& ringHostIds)
{
    ostringstream out;
    out << ringId << " " << ringHostIds.size() << " ";
    for(size_t i = 0; i < ringHostIds.size(); ++i) {
        out << ringHostIds[i];
        if(i + 1 < ringHostIds.size()) {
            out << " ";
        }
    }
    value_ = out.str();
    resetString();
    updateImpl();
}

void RingVar::clear() {
    value_ = "";
    resetString();
    updateImpl();
}

void RingVar::updateImpl() {
    if(value_.length() == 0) {
        MORDOR_LOG_TRACE(g_log) << this << " ring is now empty";
        ringId_ = kInvalidRingId;
        ringHostIds_.clear();
        return;
    }

    istringstream in(value_);

    try {
        uint32_t ringId;
        in >> ringId;
        if(!in) {
            throw logic_error("Cannot parse ringId");
        }
        uint32_t hostNum;
        in >> hostNum;
        if(!in) {
            throw logic_error("Cannot parse number of hosts in ring");
        }
        
        vector<uint32_t> ringHostIds;
        for(size_t i = 0; i < hostNum; ++i) {
            uint32_t hostId;
            in >> hostId;
            if(!in) {
                throw logic_error("Cannot parse host");
            }
            ringHostIds.push_back(hostId);
        }
        ringId_ = ringId;
        ringHostIds_.swap(ringHostIds);
    } catch(logic_error& e) {
        MORDOR_LOG_ERROR(g_log) << this << " cannot parse ring: " << e.what();
        throw;
    }
}

}  // namespace lightning
