#pragma once

#include <mordor/socket.h>

namespace lightning {

class PongReceiver {
public:
    PongReceiver(Mordor::IOManager* ioManager,
                 Mordor::Socket::ptr socket,
                 boost::function<
                    void (Mordor::Address::ptr, uint64_t, uint64_t, uint64_t)
                    > pongReceivedCallback);

    void run();
private:
    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr socket_;
    boost::function<void (Mordor::Address::ptr, uint64_t, uint64_t, uint64_t)>
        pongReceivedCallback_;
};

}  // namespace lightning
