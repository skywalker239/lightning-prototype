#include "ping_requester.h"
#include "proto/ping.pb.h"
#include <mordor/log.h>
#include <mordor/timer.h>
#include <string>
#include <vector>

namespace lightning {

using Mordor::Address;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using Mordor::TimerManager;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ping_requester");

PingRequester::PingRequester(Socket::ptr socket,
                             Address::ptr pingAddress,
                             const vector<Address::ptr>& hosts,
                             uint64_t timeoutUs,
                             PingTracker::ptr pingTracker)
    : SyncGroupRequester(socket, pingAddress, hosts, timeoutUs),
      pingTracker_(pingTracker)
{}

bool PingRequester::ping(uint64_t id) {
    PingData pingData;
    const uint64_t now = TimerManager::now();
    pingData.set_id(id);
    pingData.set_sender_now(now);

    MORDOR_LOG_TRACE(g_log) << this << " ping (" << id << ", " << now << ")";
    string requestString;
    pingData.SerializeToString(&requestString);

    pingTracker_->registerPing(id, now);
    bool success = doRequest(requestString);
    MORDOR_LOG_TRACE(g_log) << this << " doRequest(" << id << ", " << now <<
                               ") = " << success;
    // We must still invoke timeoutPing() to track hosts going down.
    pingTracker_->timeoutPing(id);
    return success;
}

bool PingRequester::onReply(Address::ptr source, const string& reply) {
    PingData pingData;
    pingData.ParseFromString(reply);
    const uint64_t now = TimerManager::now();

    MORDOR_LOG_TRACE(g_log) << this << " pong (" << pingData.id() << ", " <<
                               pingData.sender_now() << ") from " <<
                               *source << ", now = " << now;
    pingTracker_->registerPong(source, pingData.id(), now);
    return true;
}

void PingRequester::onTimeout() {
    // we call PingTracker::timeoutPing in ping(), nothing to do here.
}

}  // namespace lightning
