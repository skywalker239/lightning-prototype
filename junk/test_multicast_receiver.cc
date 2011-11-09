#include <iostream>
#include <netinet/in.h>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/socket.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>

using namespace std;
using namespace Mordor;

struct packet {
    uint16_t messageLength;
    char messageData[0];
} __attribute__((packed));

void doRecv(Socket::ptr socket) {
    Address::ptr sourceAddress = socket->emptyAddress();
    char buffer[8950];
    while(true) {
        ssize_t bytes = socket->receiveFrom((void*) buffer, sizeof(buffer), *sourceAddress);
        cout << "Received " << bytes << " bytes from " << *sourceAddress << endl;
        packet* p = (packet*) buffer;
        cout << "Message of length " << p->messageLength << ": ";
        cout.write(p->messageData, p->messageLength);
        cout << endl;
    }
}

Socket::ptr initMulticastReceiver(IOManager& ioManager,
                                  const char* localAddress,
                                  const char* multicastAddress)
{
    vector<Address::ptr> localAddrs = Address::lookup(localAddress, AF_INET);
    Address::ptr localAddr = localAddrs.front();

    Socket::ptr s = localAddr->createSocket(ioManager, SOCK_DGRAM);
    s->bind(localAddr);

    struct ip_mreq mreq;
    // Ah, the royal ugliness.
    mreq.imr_multiaddr =
        ((struct sockaddr_in*)(IPv4Address(multicastAddress).name()))->sin_addr;
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    s->setOption(IPPROTO_IP, IP_ADD_MEMBERSHIP, mreq);
    return s;
}

int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 3) {
        cout << "Usage: mcast_recv local_addr:local_port mcast_addr" << endl;
        return 1;
    }
    IOManager ioManager;
    Socket::ptr s = initMulticastReceiver(ioManager, argv[1], argv[2]);

    ioManager.schedule(boost::bind(doRecv, s));
    ioManager.dispatch();
}
