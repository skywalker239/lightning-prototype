#include "set_ring_request.h"
#include "proto/set_ring.pb.h"
#include <mordor/log.h>
#include <boost/lexical_cast.hpp>
#include <string>

namespace lightning {

using boost::lexical_cast;
using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:set_ring_request");

SetRingRequest::SetRingRequest(const vector<Address::ptr>& ringAddresses,
                               const vector<string>& ringHosts,
                               uint64_t ringId)
    : ringHosts_(ringHosts),
      ringId_(ringId),
      notAcked_(ringAddresses.begin(), ringAddresses.end()),
      status_(IN_PROGRESS),
      event_(true)
{}

const string SetRingRequest::requestString() const {
    // No need to lock anything since all relevant fields are constant.
    SetRingData data;
    MORDOR_LOG_TRACE(g_log) << this << " ring_id=" << ringId_;
    data.set_ring_id(ringId_);
    for(size_t i = 0; i < ringHosts_.size(); ++i) {
        const string& host = ringHosts_[i];
        MORDOR_LOG_TRACE(g_log) << this << " [" << ringId_ << ":" << i <<
                                   "] " << host;
        data.add_ring_hosts(host);
    }

    string s;
    data.SerializeToString(&s);
    MORDOR_LOG_TRACE(g_log) << this << " serialized to " << s.length() <<
                               " bytes";
    return s;
}

void SetRingRequest::onReply(Address::ptr sourceAddress,
                             const string& reply)
{
    SetRingAckData data;
    if(!data.ParseFromString(reply)) {
        MORDOR_LOG_WARNING(g_log) << this << " malformed reply from " <<
                                     *sourceAddress;
        return;
    }

    if(data.ring_id() != ringId_) {
        MORDOR_LOG_WARNING(g_log) << this << " got ack for ring_id=" <<
                                     data.ring_id() << " from " <<
                                     *sourceAddress << ", our ring_id=" <<
                                     ringId_;
        return;
    }

    {
        FiberMutex::ScopedLock lk(mutex_);
        notAcked_.erase(sourceAddress);
        if(notAcked_.empty()) {
            status_ = OK;
            event_.set();
        }
    }
    MORDOR_LOG_TRACE(g_log) << this << " " << *sourceAddress <<
                               " acked ring_id=" << ringId_;
}

void SetRingRequest::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " ring_id=" << ringId_ <<
                                   " onTimeout() with no pending acks";
        status_ = OK;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " set ring_id=" << ringId_ <<
                                   " timed out";
        status_ = TIMED_OUT;
    }
    event_.set();
}

void SetRingRequest::wait() {
    event_.wait();
    FiberMutex::ScopedLock lk(mutex_);
}

SyncGroupRequest::Status SetRingRequest::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
