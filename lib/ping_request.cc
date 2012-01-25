#include "ping_request.h"
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
    : pingTracker_(pingTracker),
      notAcked_(hosts.begin(), hosts.end()),
      status_(IN_PROGRESS),
      event_(true)
{
    const uint64_t now = TimerManager::now();
    rpcMessageData_.set_type(RpcMessageData::PING);
    rpcMessageData_.mutable_ping()->set_id(pingId);
    rpcMessageData_.mutable_ping()->set_sender_now(now);
    pingTracker_->registerPing(pingId, now);
}

const RpcMessageData& PingRequest::request() const {
    MORDOR_LOG_TRACE(g_log) << this << " ping(" <<
                               rpcMessageData_.ping().id() << ", " <<
                               rpcMessageData_.ping().sender_now() << ")";
    return rpcMessageData_;
}

void PingRequest::onReply(Address::ptr sourceAddress,
                          const RpcMessageData& reply)
{
    MORDOR_ASSERT(reply.type() == RpcMessageData::PING);
    MORDOR_ASSERT(reply.has_ping());

    const uint64_t id = reply.ping().id();
    const uint64_t sender_now = reply.ping().sender_now();
    const uint64_t now = TimerManager::now();
    MORDOR_LOG_TRACE(g_log) << this << " pong (" << id << ", " <<
                               sender_now << ") from " <<
                               *sourceAddress << ", now = " << now;
    {
        FiberMutex::ScopedLock lk(mutex_);
        notAcked_.erase(sourceAddress);
        pingTracker_->registerPong(sourceAddress, id, now);
        if(notAcked_.empty()) {
            status_ = COMPLETED;
            event_.set();
        }
    }
}

void PingRequest::onTimeout() {
    const uint32_t pingId = rpcMessageData_.ping().id();
    FiberMutex::ScopedLock lk(mutex_);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << "ping " << pingId <<
                                   " timed out, but " <<
                                   "all acks were received";
        status_ = COMPLETED;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " ping " << pingId << " timed out";
        status_ = TIMED_OUT;
    }
    event_.set();
}

void PingRequest::wait() {
    event_.wait();
    //! A bit hacky, but will be reached before returning from the synchronous
    //  command in any case.
    FiberMutex::ScopedLock lk(mutex_);
    pingTracker_->timeoutPing(rpcMessageData_.ping().id());
}

MulticastRpcRequest::Status PingRequest::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
