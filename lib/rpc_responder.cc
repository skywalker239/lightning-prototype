#include "rpc_responder.h"
#include "guid.h"
#include "multicast_util.h"
#include <mordor/log.h>

namespace lightning {

const size_t RpcResponder::kMaxDatagramSize;

using Mordor::Address;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using std::string;
using std::map;

static Logger::ptr g_log = Log::lookup("lightning:multicast_rpc_responder");

RpcResponder::RpcResponder(Socket::ptr listenSocket,
                                             Address::ptr multicastGroup,
                                             Socket::ptr replySocket)
    : listenSocket_(listenSocket),
      multicastGroup_(multicastGroup),
      replySocket_(replySocket)
{}

void RpcResponder::run() {
    joinMulticastGroup(listenSocket_, multicastGroup_);
    MORDOR_LOG_TRACE(g_log) << this << " listening @" <<
                               *listenSocket_->localAddress() <<
                               " for multicasts @" << *multicastGroup_ <<
                               ", replying @" <<
                               *replySocket_->localAddress();
    Address::ptr remoteAddress = listenSocket_->emptyAddress();
    while(true) {
        char buffer[kMaxDatagramSize];
        ssize_t bytes = listenSocket_->receiveFrom((void*)buffer,
                                                   sizeof(buffer),
                                                   *remoteAddress);
        RpcMessageData requestData;
        if(!requestData.ParseFromArray(buffer, bytes)) {
            MORDOR_LOG_WARNING(g_log) << this << " malformed " << bytes <<
                                         " bytes from " << *remoteAddress;
            continue;
        }
        Guid requestGuid = Guid::parse(requestData.uuid());
        MORDOR_LOG_TRACE(g_log) << this << " request id=" <<
                                   requestGuid << " from " <<
                                   *remoteAddress;

        auto handlerIter = handlers_.find(requestData.type());
        if(handlerIter == handlers_.end()) {
            MORDOR_LOG_WARNING(g_log) << this << " handler for type " <<
                                         uint32_t(requestData.type()) <<
                                         " at " <<  requestGuid <<
                                         " not found";
            continue;
        }

        RpcMessageData replyData;
        if(handlerIter->second->handleRequest(remoteAddress,
                                              requestData,
                                              &replyData))
        {
            requestGuid.serialize(replyData.mutable_uuid());
            if(!replyData.SerializeToArray(buffer, sizeof(buffer))) {
                MORDOR_LOG_WARNING(g_log) << this <<
                                             " failed to serialize reply " <<
                                             " id=" << requestGuid;
                continue;
            }
            replySocket_->sendTo((const void*) buffer,
                                 replyData.ByteSize(),
                                 0,
                                 remoteAddress);
            MORDOR_LOG_TRACE(g_log) << this << " sent reply for id=" <<
                                       requestGuid << " to " << *remoteAddress;
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " request id=" <<
                                       requestGuid << " ignored";
        }
    }
}

void RpcResponder::addHandler(RpcMessageData::Type type,
                                       RpcHandler::ptr handler)
{
    handlers_[type] = handler;
}

}  // namespace lightning
