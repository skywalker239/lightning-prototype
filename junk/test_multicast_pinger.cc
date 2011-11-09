#include <iostream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/socket.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>

using namespace std;
using namespace Mordor;

struct Ping {
    uint64_t senderNow;
};

void doRecv(Socket::ptr socket) {
    Address::ptr sourceAddress = socket->emptyAddress();
    Ping currentPing;
    while(true) {
        socket->receiveFrom((void*) &currentPing, sizeof(currentPing), *sourceAddress);
        uint64_t now = TimerManager::now();
        cout << "Pong from " << *sourceAddress << ": rtt " << now - currentPing.senderNow << " us" << endl;
    }
}

void doPing(Socket::ptr socket, const Address::ptr& address) {
    uint64_t now = TimerManager::now();
    socket->sendTo((const void*)&now, sizeof(now), 0, address);
}

Socket::ptr bindSocket(IOManager& ioManager) {
    vector<Address::ptr> localAddrs = Address::lookup("0.0.0.0", AF_INET);
    Address::ptr localAddr = localAddrs.front();
    Socket::ptr s = localAddr->createSocket(ioManager, SOCK_DGRAM);
    s->bind(localAddr);
    s->setOption(IPPROTO_IP, IP_MULTICAST_TTL, 255);
    return s;
}


int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 2) {
        cout << "Usage: ping multicast_addr:port" << endl;
        return 1;
    }
    IOManager ioManager;
    Socket::ptr s = bindSocket(ioManager);

    Address::ptr targetAddress = Address::lookup(argv[1]).front();

    ioManager.schedule(boost::bind(doRecv, s));
    ioManager.registerTimer(1000000, boost::bind(doPing, s, targetAddress), true);
    ioManager.dispatch();
}
