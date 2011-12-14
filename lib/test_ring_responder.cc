#include "multicast_util.h"
#include "ponger.h"
#include "sync_group_responder.h"
#include "proto/set_ring.pb.h"
#include "proto/sync_group_request.pb.h"
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

class SetRingResponder : public SyncGroupResponder {
public:
    SetRingResponder(Socket::ptr listen,
                     Address::ptr mcast,
                     Socket::ptr reply)
        : SyncGroupResponder(listen, mcast, reply)
    {}

    bool onRequest(Address::ptr from, const string& request, string* reply) {
        SetRingData data;
        data.ParseFromString(request);
        cout << "Got set ring id=" << data.ring_id() << " from " << *from << endl;
        for(int i = 0; i < data.ring_hosts_size(); ++i) {
            cout << "Ring host " << i << ": " << data.ring_hosts(i) << endl;
        }
        SetRingAckData ack;
        ack.set_ring_id(data.ring_id());
        ack.SerializeToString(reply);
        return true;
    }
};

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

    Ponger task(pingInSocket, mcastAddress, pingOutSocket);
    SetRingResponder task2(ringInSocket, mcastAddress, ringOutSocket);
    ioManager.schedule(boost::bind(&Ponger::run, &task));
    ioManager.schedule(boost::bind(&SetRingResponder::run, &task2));
    ioManager.dispatch();
}
