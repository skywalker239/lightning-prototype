#include "ping_request.h"
#include "proto/ping.pb.h"
#include <mordor/log.h>
#include <mordor/timer.h>
#include <string>
#include <vector>

namespace lightning {

using Mordor::Address;
using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using Mordor::TimerManager;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ping_request");

PingRequest::PingRequest(
    const vector<Address::ptr>& hosts,
    uint64_t pingId,
    PingTracker::ptr pingTracker)
    : pingId_(pingId),
      pingTracker_(pingTracker),
      notAcked_(hosts.begin(), hosts.end()),
      status_(IN_PROGRESS),
      event_(true)
{}

const string PingRequest::requestString() const {
    const uint64_t now = TimerManager::now();
    PingData pingData;
    pingData.set_id(pingId_);
    pingData.set_sender_now(now);
    MORDOR_LOG_TRACE(g_log) << this << " serialized (" << pingId_ << ", " <<
                               now << ")";

    {
        FiberMutex::ScopedLock lk(mutex_);
        pingTracker_->registerPing(pingId_, now);
    }

    string s;
    pingData.SerializeToString(&s);
    return s;
}

void PingRequest::onReply(Address::ptr sourceAddress,
                          const string& reply)
{
    PingData pingData;
    pingData.ParseFromString(reply);
    const uint64_t now = TimerManager::now();
    MORDOR_LOG_TRACE(g_log) << this << " pong (" << pingData.id() << ", " <<
                               pingData.sender_now() << ") from " <<
                               *sourceAddress << ", now = " << now;
    {
        FiberMutex::ScopedLock lk(mutex_);
        notAcked_.erase(sourceAddress);
        pingTracker_->registerPong(sourceAddress, pingData.id(), now);
        if(notAcked_.empty()) {
            status_ = OK;
            event_.set();
        }
    }
}

void PingRequest::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " ping " << pingId_ << " timed out, but " <<
                                   "all acks were received";
        status_ = OK;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " ping " << pingId_ << " timed out";
        status_ = TIMED_OUT;
    }
    event_.set();
}

void PingRequest::wait() {
    event_.wait();
    //! A bit hacky, but will be reached before returning from the synchronous
    //  command in any case.
    FiberMutex::ScopedLock lk(mutex_);
    pingTracker_->timeoutPing(pingId_);
}

SyncGroupRequest::Status PingRequest::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
