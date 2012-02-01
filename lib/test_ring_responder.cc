#include "guid.h"
#include "host_configuration.h"
#include "multicast_rpc_responder.h"
#include "ring_holder.h"
#include "ring_change_notifier.h"
#include "set_ring_handler.h"
#include "ponger.h"
#include <iostream>
#include <fstream>
#include <streambuf>
#include <netinet/in.h>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/json.h>
#include <mordor/config.h>
#include <mordor/socket.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>

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

class DummyRingHolder : public RingHolder {};

SetRingHandler::ptr setupHandler(const Guid& configHash, const GroupConfiguration& groupConfiguration) {
    vector<RingHolder::ptr> holders(1, RingHolder::ptr(new DummyRingHolder));
    RingChangeNotifier::ptr notifier(new RingChangeNotifier(holders));
    return SetRingHandler::ptr(new SetRingHandler(configHash, notifier, groupConfiguration));
}


Socket::ptr bindSocket(Address::ptr bindAddress, IOManager* ioManager) {
    Socket::ptr s = bindAddress->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddress);
    return s;
}

MulticastRpcResponder::ptr setupResponder(IOManager* ioManager, 
                                          const Guid& configHash,
                                          const JSON::Value& config,
                                          uint32_t ourId)
{
    GroupConfiguration groupConfig = parseGroupConfiguration(config["hosts"], ourId);
    RpcHandler::ptr ponger(new Ponger);
    RpcHandler::ptr setRingHandler = setupHandler(configHash, groupConfig);

    const HostConfiguration& hostConfig = groupConfig.hosts()[ourId];

    Address::ptr multicastGroup = Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();
    Socket::ptr listenSocket = bindSocket(hostConfig.multicastListenAddress, ioManager);
    Socket::ptr replySocket = bindSocket(hostConfig.multicastReplyAddress, ioManager);

    MulticastRpcResponder::ptr responder(new MulticastRpcResponder(listenSocket, multicastGroup, replySocket));
    responder->addHandler(RpcMessageData::PING, ponger);
    responder->addHandler(RpcMessageData::SET_RING, setRingHandler);
    return responder;
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
    if(argc != 3) {
        cout << "Usage: slave config.json id" << endl;
        return 1;
    }
    Guid configGuid;
    JSON::Value config;
    readConfig(argv[1], &configGuid, &config);
    const uint32_t id = lexical_cast<uint32_t>(argv[2]);
    IOManager ioManager;

    MulticastRpcResponder::ptr responder = setupResponder(&ioManager, configGuid, config, id);
    ioManager.schedule(boost::bind(&MulticastRpcResponder::run, responder));
    ioManager.dispatch();
}
