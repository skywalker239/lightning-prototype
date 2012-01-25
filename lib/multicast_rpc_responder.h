#pragma once

#include "proto/rpc_messages.pb.h"
#include "rpc_handler.h"
#include <mordor/socket.h>
#include <map>

namespace lightning {

class MulticastRpcResponder {
public:
    typedef boost::shared_ptr<MulticastRpcResponder> ptr;

    MulticastRpcResponder(Mordor::Socket::ptr listenSocket,
                          Mordor::Address::ptr multicastGroup,
                          Mordor::Socket::ptr replySocket);
    
    //! Processes requests one by one, calling handlers and sending
    //  replies synchronously.
    void run();

    //! Registers a handler for a certain RPC type.
    //  Overrides the previously registered handler, if any.
    //  Adding handlers when run() is active is not safe.
    void addHandler(RpcMessageData::Type type,
                    RpcHandler::ptr handler);
private:
    static const size_t kMaxDatagramSize = 8950;

    Mordor::Socket::ptr listenSocket_;
    Mordor::Address::ptr multicastGroup_;
    Mordor::Socket::ptr replySocket_;

    std::map<RpcMessageData::Type, RpcHandler::ptr> handlers_;
};

}  // namespace lightning
