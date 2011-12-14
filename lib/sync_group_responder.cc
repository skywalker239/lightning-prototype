#include "sync_group_responder.h"
#include "multicast_util.h"
#include "proto/sync_group_request.pb.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:sync_group_responder");

SyncGroupResponder::SyncGroupResponder(Socket::ptr listenSocket,
                                       Address::ptr multicastGroup,
                                       Socket::ptr replySocket)
    : listenSocket_(listenSocket),
      multicastGroup_(multicastGroup),
      replySocket_(replySocket)
{}

void SyncGroupResponder::run() {
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
        SyncGroupRequestData requestData;
        if(!requestData.ParseFromArray(buffer, bytes)) {
            MORDOR_LOG_WARNING(g_log) << this << " malformed " << bytes <<
                                         " bytes from " << *remoteAddress;
            continue;
        }
        MORDOR_LOG_TRACE(g_log) << this << " request id=" <<
                                   requestData.id() << " from " <<
                                   *remoteAddress;
        string reply;
        if(onRequest(remoteAddress, requestData.data(), &reply)) {
            SyncGroupRequestData replyData;
            replyData.set_id(requestData.id());
            replyData.set_data(reply);
            if(!replyData.SerializeToArray(buffer, sizeof(buffer))) {
                MORDOR_LOG_WARNING(g_log) << this <<
                                             " failed to serialize reply " <<
                                              " id=" << requestData.id();
                continue;
            }
            replySocket_->sendTo((const void*) buffer,
                                 replyData.ByteSize(),
                                 0,
                                 remoteAddress);
        } else {
            MORDOR_LOG_WARNING(g_log) << this << " request id=" <<
                                         requestData.id() << " ignored";
        }
    }
}

}  // namespace lightning
