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
    RingConfiguration::const_ptr ring,
    uint64_t pingId,
    PingTracker::ptr pingTracker,
    uint64_t timeoutUs)
    : MulticastRpcRequest(ring, timeoutUs),
      group_(ring->group()),
      pingTracker_(pingTracker)
{
    const uint64_t now = TimerManager::now();
    requestData_.set_type(RpcMessageData::PING);
    requestData_.mutable_ping()->set_id(pingId);
    requestData_.mutable_ping()->set_sender_now(now);
    pingTracker_->registerPing(pingId, now);
}

std::ostream& PingRequest::output(std::ostream& os) const {
    const PingData& request = requestData_.ping();
    os << "Ping(" << request.id() << ", " << request.sender_now() << ")";
    return os;
}

void PingRequest::applyReply(uint32_t hostId,
                             const RpcMessageData& reply)
{
    MORDOR_ASSERT(reply.type() == RpcMessageData::PING);
    MORDOR_ASSERT(reply.has_ping());

    const uint64_t id = reply.ping().id();
    const uint64_t sender_now = reply.ping().sender_now();
    const uint64_t now = TimerManager::now();
    MORDOR_LOG_TRACE(g_log) << this << " pong (" << id << ", " <<
                               sender_now << ") from " <<
                               group_->host(hostId) <<
                               ", now = " << now;
    pingTracker_->registerPong(hostId, id, now);
}

}  // namespace lightning
