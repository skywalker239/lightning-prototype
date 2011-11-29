#pragma once

#include <mordor/socket.h>
#include <boost/noncopyable.hpp>

namespace lightning {

class PongReceiver : boost::noncopyable {
public:
    typedef boost::shared_ptr<PongReceiver> ptr;

    PongReceiver(Mordor::IOManager* ioManager,
                 Mordor::Socket::ptr socket,
                 boost::function<
                    void (Mordor::Address::ptr, uint64_t, uint64_t)
                    > pongReceivedCallback);

    void run();
private:
    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr socket_;
    boost::function<void (Mordor::Address::ptr, uint64_t, uint64_t)>
        pongReceivedCallback_;
};

}  // namespace lightning
