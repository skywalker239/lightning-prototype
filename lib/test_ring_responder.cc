#include "multicast_util.h"
#include "ponger.h"
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
using namespace lightning;
using boost::lexical_cast;

    struct SetRingPacket {
        uint64_t ringId;
        uint64_t ringStringLength;
        char ringString[0];
    } __attribute__((packed));

    struct RingAckPacket {
        uint64_t ringId;
    } __attribute__((packed));

void ackSetRings(Socket::ptr inSocket, Socket::ptr outSocket) {
    Address::ptr remoteAddress = inSocket->emptyAddress();
    while(true) {
        char buffer[8000];
        inSocket->receiveFrom((void*)buffer, 8000, *remoteAddress);
        RingAckPacket ringAck;
        SetRingPacket* setPacket = (SetRingPacket*) buffer;
        ringAck.ringId = setPacket->ringId;
        outSocket->sendTo((const void*)&ringAck, sizeof(ringAck), 0, *remoteAddress);
        cout << "Got set ring id=" << setPacket->ringId << " ring='" << setPacket->ringString << "'" << endl;
    }
}

Socket::ptr makeSocket(IOManager& ioManager, const string& addr, uint16_t port) {
    string fullAddress = addr + ":" + lexical_cast<string>(port);
    Address::ptr sockAddr = Address::lookup(fullAddress, AF_INET).front();
    Socket::ptr s = sockAddr->createSocket(ioManager, SOCK_DGRAM);
    s->bind(sockAddr);
    return s;
}

int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 7) {
        cout << "Usage: mcast_recv bind_addr mcast_addr ping_req_port ping_rep_port ring_req_port ring_rep_port" << endl;
        return 1;
    }
    IOManager ioManager;

    Socket::ptr pingOutSocket = makeSocket(ioManager, argv[1], lexical_cast<uint16_t>(argv[4]));
    Socket::ptr ringOutSocket = makeSocket(ioManager, argv[1], lexical_cast<uint16_t>(argv[6]));
    Socket::ptr pingInSocket  = makeSocket(ioManager, "0.0.0.0", lexical_cast<uint16_t>(argv[3]));
    Socket::ptr ringInSocket  = makeSocket(ioManager, "0.0.0.0", lexical_cast<uint16_t>(argv[5]));

    Address::ptr mcastAddress = Address::lookup(argv[2], AF_INET).front();
    joinMulticastGroup(ringInSocket, mcastAddress);

    Ponger task(&ioManager, pingInSocket, pingOutSocket, mcastAddress);
    ioManager.schedule(boost::bind(&Ponger::run, &task));
    ioManager.schedule(boost::bind(&ackSetRings, ringInSocket, ringOutSocket));
    ioManager.dispatch();
}
