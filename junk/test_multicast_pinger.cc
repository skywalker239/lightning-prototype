#include <iostream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/socket.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>

using namespace std;
using namespace Mordor;
using boost::lexical_cast;

struct PingPacket {
    uint64_t sequence;
    uint64_t senderNow;
} __attribute__((packed));

void doRecv(Socket::ptr socket) {
    Address::ptr sourceAddress = socket->emptyAddress();
    PingPacket currentPing;

    while(true) {
        socket->receiveFrom((void*) &currentPing, sizeof(currentPing), *sourceAddress);
        uint64_t now = TimerManager::now();
        uint64_t rtt = now - currentPing.senderNow;
        cout << currentPing.sequence << '\t' << *sourceAddress << '\t' << rtt << endl;
    }
}

void doPing(Socket::ptr socket, const Address::ptr& address) {
    static uint64_t sequence = 0;
    PingPacket p = { sequence++, TimerManager::now() };
    socket->sendTo((const void*)&p, sizeof(p), 0, address);
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
    ioManager.registerTimer(200000, boost::bind(doPing, s, targetAddress), true);
    ioManager.dispatch();
}
