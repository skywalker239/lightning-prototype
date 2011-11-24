#pragma once

#include <mordor/socket.h>

namespace lightning {

class Ponger {
public:
    Ponger(Mordor::IOManager* ioManager,
           Mordor::Socket::ptr socket,
           Mordor::Address::ptr multicastGroup);

    void run();
private:
    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr socket_;
    Mordor::Address::ptr multicastGroup_;
};

}  // namespace lightning
