#include "ponger.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Socket;
using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:ponger");

bool Ponger::handleRequest(Address::ptr sourceAddress,
                       const RpcMessageData& request,
                       RpcMessageData* reply)
{
    reply->set_type(RpcMessageData::PING);
    reply->mutable_ping()->MergeFrom(request.ping());

    MORDOR_LOG_TRACE(g_log) << this << " got ping(" <<
                               request.ping().id() << ", " <<
                               request.ping().sender_now() << ")" <<
                               " from " << *sourceAddress;
    return true;
}

}  // namespace lightning
