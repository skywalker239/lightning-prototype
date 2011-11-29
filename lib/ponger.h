#pragma once

#include <mordor/socket.h>

namespace lightning {

class Ponger {
public:
    Ponger(Mordor::IOManager* ioManager,
           Mordor::Socket::ptr pingSocket,
           Mordor::Socket::ptr pongSocket,
           Mordor::Address::ptr multicastGroup);

    void run();
private:
    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr pingSocket_;
    Mordor::Socket::ptr pongSocket_;
    Mordor::Address::ptr multicastGroup_;
};

}  // namespace lightning
