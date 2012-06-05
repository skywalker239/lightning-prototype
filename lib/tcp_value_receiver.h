#pragma once

#include "blocking_queue.h"
#include "proposer_state.h"
#include "value.h"
#include <mordor/iomanager.h>
#include <mordor/socket.h>

namespace lightning {

class TcpValueReceiver
    : public boost::enable_shared_from_this<TcpValueReceiver>
{
public:
    typedef boost::shared_ptr<TcpValueReceiver> ptr;

    TcpValueReceiver(size_t valueBufferSize,
                     ProposerState::ptr proposerState,
                     BlockingQueue<paxos::Value>::ptr submitQueue,
                     Mordor::IOManager* ioManager,
                     Mordor::Socket::ptr listenSocket);

    void run();
private:
    void handleValueStream(Mordor::Socket::ptr socket);

    bool readValue(Mordor::Socket::ptr socket,
                   paxos::Value* value);

    void readFromSocket(Mordor::Socket::ptr socket,
                        size_t bytes,
                        char* destination);

    const size_t valueBufferSize_;
    ProposerState::ptr proposer_;
    BlockingQueue<paxos::Value>::ptr submitQueue_;

    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr listenSocket_;
};

}  // namespace lightning
