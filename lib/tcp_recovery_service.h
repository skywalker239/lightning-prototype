#pragma once

#include "acceptor_state.h"
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <boost/enable_shared_from_this.hpp>

namespace lightning {

class BatchRecoveryRequestData;
class BatchRecoveryReplyData;

class TcpRecoveryService
    : public boost::enable_shared_from_this<TcpRecoveryService>
{
public:
    typedef boost::shared_ptr<TcpRecoveryService> ptr;

    TcpRecoveryService(Mordor::IOManager* ioManager,
                       Mordor::Socket::ptr listenSocket,
                       AcceptorState::ptr acceptorState);

    void run();
private:
    void handleRecoveryConnection(Mordor::Socket::ptr socket);

    bool readRequest(Mordor::Socket::ptr socket,
                     BatchRecoveryRequestData* request);
    void handleRequest(Mordor::Socket::ptr socket,
                       const BatchRecoveryRequestData& request,
                       BatchRecoveryReplyData* reply);
    void sendReply(Mordor::Socket::ptr socket,
                   const BatchRecoveryReplyData& reply);

    void readFromSocket(Mordor::Socket::ptr socket,
                        size_t bytes,
                        char* destination);

    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr listenSocket_;
    AcceptorState::ptr acceptorState_;
};

}  // namespace lightning
