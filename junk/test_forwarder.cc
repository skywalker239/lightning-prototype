#include <iostream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/socket.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>

using namespace std;
using namespace Mordor;

void doForward(Socket::ptr socket, const Address::ptr& targetAddress) {
    while(true) {
        uint64_t data[2];
        Address::ptr sourceAddress = socket->emptyAddress();
        ssize_t bytes = socket->receiveFrom((void*) data, sizeof(data), *sourceAddress);
        socket->sendTo((const void*) data, bytes, 0, targetAddress);
    }
}


Socket::ptr bindSocket(IOManager& ioManager, const char* addrString) {
    vector<Address::ptr> localAddrs = Address::lookup(addrString);
    Address::ptr localAddr = localAddrs.front();
    Socket::ptr s = localAddr->createSocket(ioManager, SOCK_DGRAM);
    s->bind(localAddr);
    return s;
}


int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 3) {
        cout << "Usage: forward host:port local_addr:local_port" << endl;
        return 1;
    }
    IOManager ioManager;
    Socket::ptr s = bindSocket(ioManager, argv[2]);

    vector<Address::ptr> addrs = Address::lookup(argv[1]);
    Address::ptr targetAddress = addrs.front();

    ioManager.schedule(boost::bind(doForward, s, targetAddress));
    ioManager.dispatch();
}
