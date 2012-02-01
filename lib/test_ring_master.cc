#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/config.h>
#include <mordor/json.h>
#include <mordor/socket.h>
#include <mordor/streams/file.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>
#include <map>
#include "guid.h"
#include "host_configuration.h"
#include "ring_oracle.h"
#include "datacenter_aware_quorum_ring_oracle.h"
#include "ring_manager.h"
#include "ring_holder.h"
#include "pinger.h"

using namespace std;
using namespace Mordor;
using namespace lightning;
using boost::lexical_cast;


void readConfig(const string& filename,
                Guid* configHash,
                JSON::Value* config)
{
    ifstream f(filename);
    string fileData;
    f.seekg(0, ios::end);
    fileData.reserve(f.tellg());
    f.seekg(0, ios::beg);
    fileData.assign(istreambuf_iterator<char>(f), istreambuf_iterator<char>());

    *configHash = Guid::fromData(fileData.c_str(), fileData.length());
    *config = JSON::parse(fileData);

    cout << *config << endl;
    cout << *configHash << endl;
}

MulticastRpcRequester::ptr setupRequester(IOManager* ioManager,
                                          GuidGenerator::ptr guidGenerator,
                                          const GroupConfiguration& groupConfiguration,
                                          Address::ptr mcastDestination)
{
    Address::ptr bindAddr = groupConfiguration.thisHostConfiguration().multicastSourceAddress;
    Socket::ptr s = bindAddr->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddr);
    return MulticastRpcRequester::ptr(new MulticastRpcRequester(ioManager, guidGenerator, s, mcastDestination));
}

class DummyRingHolder : public RingHolder {};

void setupEverything(uint32_t hostId,
                     const Guid& configHash,
                     const JSON::Value& config,
                     IOManager* ioManager,
                     Pinger::ptr* pinger,
                     RingManager::ptr* ringManager)
{
    GroupConfiguration groupConfiguration = parseGroupConfiguration(config["hosts"],
                                                                    hostId);
    const uint64_t pingWindow = config["ping_window"].get<long long>();
    const uint64_t pingTimeout = config["ping_timeout"].get<long long>();
    const uint64_t pingInterval = config["ping_interval"].get<long long>();
    const uint64_t hostTimeout = config["host_timeout"].get<long long>();
    const uint64_t ringTimeout = config["ring_timeout"].get<long long>();
    const uint64_t ringRetryInterval = config["ring_retry_interval"].get<long long>();
    Address::ptr mcastDestination =
        Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();

    GuidGenerator::ptr guidGenerator(new GuidGenerator);
    boost::shared_ptr<FiberEvent> event(new FiberEvent);

    MulticastRpcRequester::ptr requester = setupRequester(ioManager, guidGenerator, groupConfiguration, mcastDestination);
    ioManager->schedule(boost::bind(&MulticastRpcRequester::run, requester));

    PingTracker::ptr pingTracker(new PingTracker(groupConfiguration, pingWindow, pingTimeout, hostTimeout, event));
    *pinger = Pinger::ptr(new Pinger(ioManager, requester, groupConfiguration, pingInterval, pingTimeout, pingTracker));

    RingOracle::ptr oracle(new DatacenterAwareQuorumRingOracle(groupConfiguration, true, hostTimeout));

    RingHolder::ptr ringHolder(new DummyRingHolder);
    vector<RingHolder::ptr> holders(1, ringHolder);

    RingChangeNotifier::ptr notifier(new RingChangeNotifier(holders));

    *ringManager = RingManager::ptr(new RingManager(groupConfiguration, configHash, ioManager, event, requester, pingTracker, oracle, notifier, ringTimeout, ringRetryInterval));
}



int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 3) {
        cout << "Usage: master config.json host_id" << endl;
        return 0;
    }
    const uint32_t hostId = lexical_cast<uint32_t>(argv[2]);
    Guid configHash;
    JSON::Value config;
    readConfig(argv[1], &configHash, &config);

    IOManager ioManager;
    Pinger::ptr pinger;
    RingManager::ptr ringManager;
    setupEverything(hostId, configHash, config, &ioManager, &pinger, &ringManager);
    ioManager.schedule(boost::bind(&Pinger::run, pinger));
    ioManager.schedule(boost::bind(&RingManager::run, ringManager));
    ioManager.dispatch();
    return 0;
}

/*

class DummyRingHolder : public RingHolder {
};


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

PingTracker::ptr setupPingTracker(const vector<string>& allHosts,
                                  uint64_t window,
                                  uint64_t pingTimeout,
                                  uint64_t hostTimeout,
                                  uint16_t pingReplyPort,
                                  boost::shared_ptr<FiberEvent> event)
{
    PingTracker::HostnameMap hostnameMap;
    for(size_t i = 0; i < allHosts.size(); ++i) {
        hostnameMap[makeAddress(allHosts[i], pingReplyPort)] = allHosts[i];
    }
    return PingTracker::ptr(new PingTracker(hostnameMap, window, pingTimeout, hostTimeout, event));
}

SyncGroupRequester::ptr setupRequester(IOManager* ioManager,
                                       const string& bindAddress,
                                       uint16_t bindPort,
                                       const string& address,
                                       uint64_t timeout)
{
    Address::ptr bindAddr = makeAddress(bindAddress, bindPort);
    Socket::ptr s = bindAddr->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddr);
    Address::ptr mcastAddr = Address::lookup(address, AF_INET).front();
    return SyncGroupRequester::ptr(new SyncGroupRequester(ioManager, s, mcastAddr, timeout));
}

Pinger::ptr setupPinger(IOManager* ioManager,
                        SyncGroupRequester::ptr requester,
                        const vector<string>& hosts,
                        uint16_t pingReplyPort,
                        uint64_t pingInterval,
                        PingTracker::ptr tracker)
{
    vector<Address::ptr> addrs;
    for(size_t i = 0; i < hosts.size(); ++i) {
        addrs.push_back(makeAddress(hosts[i], pingReplyPort));
    }
    return Pinger::ptr(new Pinger(ioManager, requester, addrs, pingInterval, tracker));
}

RingOracle::ptr setupOracle(const map<string, vector<string> >& topology, uint64_t hostTimeout) {
    DatacenterAwareQuorumRingOracle::ptr oracle(new DatacenterAwareQuorumRingOracle(true, hostTimeout));
    for(auto i = topology.begin(); i != topology.end(); ++i) {
        oracle->addDatacenter(i->first);
        for(auto j = i->second.begin(); j != i->second.end(); ++j) {
            oracle->addAcceptor(*j, i->first);
        }
    }
    return oracle;
}

RingManager::ptr setupRingManager(IOManager* ioManager,
                                  boost::shared_ptr<FiberEvent> event,
                                  SyncGroupRequester::ptr requester,
                                  PingTracker::ptr pingTracker,
                                  RingOracle::ptr ringOracle,
                                  RingChangeNotifier::ptr ringChangeNotifier,
                                  uint16_t ringReplyPort,
                                  uint64_t lookupRingRetry)
{
    return RingManager::ptr(new RingManager(ioManager, event, requester, pingTracker, ringOracle, ringChangeNotifier, ringReplyPort, lookupRingRetry));
}

void setupEverything(const string& configFile, IOManager* ioManager, Pinger::ptr* pinger, RingManager::ptr* ringManager) {
    FileStream input(configFile, FileStream::READ);
    JSON::Value json = JSON::parse(input);

    auto acceptorMap = parseTopology(json["acceptors"]);
    vector<string> allAcceptors;
    for(auto i = acceptorMap.begin(); i != acceptorMap.end(); ++i) {
        const auto& addrs = i->second;
        for(size_t j = 0; j < addrs.size(); ++j) {
            allAcceptors.push_back(addrs[j]);
        }
    }

    boost::shared_ptr<FiberEvent> event(new FiberEvent);

    const string& bindAddress = json["bind_address"].get<string>();
    auto pingRequesterConfig = json["ping_requester"];
    const uint16_t pingBindPort = pingRequesterConfig["bind_port"].get<long long>();
    const string& pingAddress = pingRequesterConfig["address"].get<string>();
    const uint64_t pingTimeout = pingRequesterConfig["timeout"].get<long long>();

    SyncGroupRequester::ptr pingRequester = setupRequester(ioManager, bindAddress, pingBindPort, pingAddress, pingTimeout);
    ioManager->schedule(boost::bind(&SyncGroupRequester::run, pingRequester));
    PingTracker::ptr pingTracker = setupPingTracker(allAcceptors,
                                                    json["ping_tracker"]["window"].get<long long>(),
                                                    json["ping_tracker"]["ping_timeout"].get<long long>(),
                                                    json["ping_tracker"]["host_down_timeout"].get<long long>(),
                                                    json["pinger"]["reply_port"].get<long long>(),
                                                    event);
    *pinger = setupPinger(ioManager, pingRequester, allAcceptors, json["pinger"]["reply_port"].get<long long>(), json["pinger"]["interval"].get<long long>(), pingTracker);
    RingOracle::ptr oracle = setupOracle(acceptorMap, json["ping_tracker"]["host_down_timeout"].get<long long>());

    RingHolder::ptr ringHolder(new DummyRingHolder);
    vector<pair<RingHolder::ptr, uint16_t> > holders;
    holders.push_back(make_pair(ringHolder, uint16_t(json["ring_manager"]["reply_port"].get<long long>())));
    RingChangeNotifier::ptr notifier(new RingChangeNotifier(holders));

    SyncGroupRequester::ptr ringRequester = setupRequester(ioManager, bindAddress, uint16_t(json["ring_requester"]["bind_port"].get<long long>()), json["ring_requester"]["address"].get<string>(), json["ring_requester"]["timeout"].get<long long>());
    ioManager->schedule(boost::bind(&SyncGroupRequester::run, ringRequester));
    *ringManager = setupRingManager(ioManager, event, ringRequester, pingTracker, oracle, notifier, json["ring_manager"]["reply_port"].get<long long>(), json["ring_manager"]["lookup_retry"].get<long long>());
}

int main(int argc, char** argv) {
  try {
    Config::loadFromEnvironment();
    if(argc != 2) {
        cout << "Usage: ping acceptors.json" << endl;
        return 1;
    }

    IOManager ioManager;
    Pinger::ptr pinger;
    RingManager::ptr ringManager;
    setupEverything(argv[1], &ioManager, &pinger, &ringManager);
    ioManager.schedule(boost::bind(&Pinger::run, pinger));
    ioManager.schedule(boost::bind(&RingManager::run, ringManager));
    ioManager.dispatch();
  } catch(...) {
    cout << boost::current_exception_diagnostic_information() << endl;
  }
}
*/
