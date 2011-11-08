#include <iostream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/socket.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>

using namespace std;
using namespace Mordor;

void doPing(Socket::ptr socket, const Address::ptr& targetAddress) {
    static uint64_t iters = 0;
    uint64_t data[2];
    data[0] = iters++;
    data[1] = TimerManager::now();
    socket->sendTo((const void*)data, sizeof(data), 0, targetAddress);
}


Socket::ptr bindSocket(IOManager& ioManager, const char* addrString) {
    vector<Address::ptr> localAddrs = Address::lookup(addrString);
    Address::ptr localAddr = localAddrs.front();
    Socket::ptr s = localAddr->createSocket(ioManager, SOCK_DGRAM);
    s->bind(localAddr);
    return s;
}

void doReceive(Socket::ptr socket) {
    uint64_t lastReceived = 0;
    while(true) {
        uint64_t data[2];
        Address::ptr sourceAddress = socket->emptyAddress();
        socket->receiveFrom((void*) data, sizeof(data), *sourceAddress);
        uint64_t now = TimerManager::now();
        if(lastReceived + 1 != data[0]) {
            cout << "LOST [" << lastReceived + 1 << ", " << data[0] << ")" << endl;
        }
        lastReceived = data[0];
        cout << "Received " << data[0] << ", roundtrip " << now - data[1] << endl;
    }
}


int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 4) {
        cout << "Usage: ping host:port delay local_addr:local_port" << endl;
        return 1;
    }
    IOManager ioManager;
    Socket::ptr s = bindSocket(ioManager, argv[3]);

    vector<Address::ptr> addrs = Address::lookup(argv[1]);
    Address::ptr targetAddress = addrs.front();
    uint64_t delay = boost::lexical_cast<uint64_t>(argv[2]);

    ioManager.registerTimer(delay, boost::bind(doPing, s, targetAddress), true);
    ioManager.schedule(boost::bind(doReceive, s));
    ioManager.dispatch();
}
