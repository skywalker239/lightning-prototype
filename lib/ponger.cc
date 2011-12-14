#include "ponger.h"
#include "proto/ping.pb.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Socket;
using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:ponger");

Ponger::Ponger(Socket::ptr listenSocket,
               Address::ptr multicastGroup,
               Socket::ptr replySocket)
    : SyncGroupResponder(listenSocket, multicastGroup, replySocket)
{}

bool Ponger::onRequest(Address::ptr sourceAddress,
                       const string& request,
                       string* reply)
{
    PingData pingData;
    if(!pingData.ParseFromString(request)) {
        MORDOR_LOG_WARNING(g_log) << this << " malformed ping";
        return false;
    }

    MORDOR_LOG_TRACE(g_log) << this << " got ping(" <<
                               pingData.id() << ", " <<
                               pingData.sender_now() << ")" <<
                               " from " << *sourceAddress;
    *reply = request;
    return true;
}

}  // namespace lightning
