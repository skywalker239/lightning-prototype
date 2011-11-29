#include <iostream>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/json.h>
#include <mordor/socket.h>
#include <mordor/streams/file.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>
#include <map>
#include "ring_oracle.h"
#include "datacenter_aware_quorum_ring_oracle.h"
#include "ring_manager.h"
#include "pinger.h"
#include "pong_receiver.h"

using namespace std;
using namespace Mordor;
using namespace lightning;
using boost::lexical_cast;

RingManager::ptr ringManager;
Pinger::ptr pinger;
PongReceiver::ptr pongReceiver;

Address::ptr makeAddress(const string& addr, uint16_t port) {
    string fullAddress = addr + ":" + lexical_cast<string>(port);
    return Address::lookup(fullAddress, AF_INET).front();
}

map<string, vector<string> > parseTopology(const JSON::Value& value) {
    map<string, vector<string> > topology;
    const JSON::Object& acceptors = value.get<JSON::Object>();
    for(JSON::Object::const_iterator i = acceptors.begin(); i != acceptors.end(); ++i) {
        const string& dc = i->first;
        const JSON::Array& dcAcceptors = i->second.get<JSON::Array>();
        for(size_t j = 0; j < dcAcceptors.size(); ++j) {
            const string& acceptor = dcAcceptors[j].get<string>();
            topology[dc].push_back(acceptor);
        }
    }
    return topology;
}


void setupEverything(const string& configFile, IOManager* ioManager) {
    FileStream input(configFile, FileStream::READ);
    JSON::Value json = JSON::parse(input);

    auto acceptorMap = parseTopology(json["acceptors"]);
    uint16_t pingReplyPort = json["ping_reply_port"].get<long long>();
    uint16_t ringReplyPort = json["ring_reply_port"].get<long long>();

    vector<Address::ptr> allAcceptorsForPing;
    for(auto i = acceptorMap.begin(); i != acceptorMap.end(); ++i) {
        const auto& addrs = i->second;
        for(size_t j = 0; j < addrs.size(); ++j) {
            allAcceptorsForPing.push_back(makeAddress(addrs[j], pingReplyPort));
        }
    }

    uint64_t pingTimeout = json["ping_timeout"].get<long long>();
    uint64_t pingInterval = json["ping_interval"].get<long long>();
    uint64_t pingWindow   = json["ping_window"].get<long long>();
    uint64_t hostDownTimeout = json["host_down_timeout"].get<long long>();
    uint64_t ringLookupTimeout = json["ring_lookup_timeout"].get<long long>();
    uint64_t ringSetTimeout = json["ring_set_timeout"].get<long long>();

    boost::shared_ptr<FiberEvent> hostDownEvent(new FiberEvent);
    PingTracker::ptr pingTracker(new PingTracker(allAcceptorsForPing, pingWindow, pingTimeout, hostDownTimeout, hostDownEvent));

    DatacenterAwareQuorumRingOracle::ptr ringOracle(new DatacenterAwareQuorumRingOracle(true, hostDownTimeout));
    for(auto i = acceptorMap.begin(); i != acceptorMap.end(); ++i) {
        const string& dc = i->first;
        const auto& acceptors = i->second;
        ringOracle->addDatacenter(dc);
        for(size_t j = 0; j < acceptors.size(); ++j) {
            //! The oracle considers PingStats.
            ringOracle->addAcceptor(makeAddress(acceptors[j], pingReplyPort), dc);
        }
    }

    uint16_t pingRequestPort = json["ping_request_port"].get<long long>();
    uint16_t ringRequestPort = json["ring_request_port"].get<long long>();

    Address::ptr pingBindAddress = makeAddress(json["bind_address"].get<string>(), pingRequestPort);
    Socket::ptr pingSocket = pingBindAddress->createSocket(*ioManager, SOCK_DGRAM);
    pingSocket->bind(pingBindAddress);
    Address::ptr ringBindAddress = makeAddress(json["bind_address"].get<string>(), ringRequestPort);
    Socket::ptr ringSocket = ringBindAddress->createSocket(*ioManager, SOCK_DGRAM);
    ringSocket->bind(ringBindAddress);

    Address::ptr pingMcastAddress = makeAddress(json["ctrl_mcast_group"].get<string>(), pingRequestPort);
    Address::ptr ringMcastAddress = makeAddress(json["ctrl_mcast_group"].get<string>(), ringRequestPort);

    pinger.reset(new Pinger(ioManager,
                            pingSocket,
                            pingMcastAddress,
                            pingInterval,
                            pingTimeout,
                            boost::bind(&PingTracker::registerPing,
                                       pingTracker.get(),
                                       _1,
                                       _2),
                            boost::bind(&PingTracker::timeoutPing,
                                        pingTracker.get(),
                                        _1)));
    pongReceiver.reset(new PongReceiver(ioManager,
                                        pingSocket,
                                        boost::bind(&PingTracker::registerPong,
                                                    pingTracker.get(),
                                                    _1,
                                                    _2,
                                                    _3)));
    vector<Address::ptr> allAcceptorsForRing;
    for(auto i = acceptorMap.begin(); i != acceptorMap.end(); ++i) {
        const auto& addrs = i->second;
        for(size_t j = 0; j < addrs.size(); ++j) {
            allAcceptorsForRing.push_back(makeAddress(addrs[j], ringReplyPort));
        }
    }
    ringManager.reset(new RingManager(ioManager,
                                      hostDownEvent,
                                      ringSocket,
                                      ringMcastAddress,
                                      allAcceptorsForRing,
                                      pingTracker,
                                      ringOracle,
                                      ringLookupTimeout,
                                      ringSetTimeout));
}

int main(int argc, char** argv) {
  try {
    Config::loadFromEnvironment();
    if(argc != 2) {
        cout << "Usage: ping acceptors.json" << endl;
        return 1;
    }

    IOManager ioManager;
    setupEverything(argv[1], &ioManager);
    ioManager.schedule(boost::bind(&Pinger::run, pinger.get()));
    ioManager.schedule(boost::bind(&PongReceiver::run, pongReceiver.get()));
    ioManager.schedule(boost::bind(&RingManager::run, ringManager.get()));
    ioManager.dispatch();
  } catch(...) {
    cout << boost::current_exception_diagnostic_information() << endl;
  }
}
