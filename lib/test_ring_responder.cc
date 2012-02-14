#include "acceptor_state.h"
#include "batch_phase1_handler.h"
#include "phase1_handler.h"
#include "phase2_handler.h"
#include "guid.h"
#include "host_configuration.h"
#include "multicast_rpc_responder.h"
#include "ring_holder.h"
#include "ring_voter.h"
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

Socket::ptr bindSocket(Address::ptr bindAddress, IOManager* ioManager) {
    Socket::ptr s = bindAddress->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddress);
    return s;
}

void setupEverything(IOManager* ioManager, 
                     const Guid& configHash,
                     const JSON::Value& config,
                     uint32_t ourId,
                     AcceptorState::ptr acceptorState,
                     MulticastRpcResponder::ptr* responder,
                     RingVoter::ptr* ringVoter)
{
    GroupConfiguration::ptr groupConfig = parseGroupConfiguration(config["hosts"], ourId);

    Socket::ptr ringSocket = bindSocket(groupConfig->host(groupConfig->thisHostId()).ringAddress, ioManager);
    *ringVoter = RingVoter::ptr(new RingVoter(ringSocket, acceptorState));

    RpcHandler::ptr ponger(new Ponger);
    boost::shared_ptr<BatchPhase1Handler> batchPhase1Handler(new BatchPhase1Handler(acceptorState));
    boost::shared_ptr<Phase1Handler> phase1Handler(new Phase1Handler(acceptorState));
    boost::shared_ptr<Phase2Handler> phase2Handler(new Phase2Handler(acceptorState, *ringVoter));

    vector<RingHolder::ptr> holders;
    holders.push_back(batchPhase1Handler);
    holders.push_back(phase1Handler);
    holders.push_back(phase2Handler);
    holders.push_back(*ringVoter);
    RingChangeNotifier::ptr notifier(new RingChangeNotifier(holders));
    RpcHandler::ptr setRingHandler(new SetRingHandler(configHash, notifier, groupConfig));

    const HostConfiguration& hostConfig = groupConfig->host(groupConfig->thisHostId());

    Address::ptr multicastGroup = Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();
    Socket::ptr listenSocket = bindSocket(hostConfig.multicastListenAddress, ioManager);
    Socket::ptr replySocket = bindSocket(hostConfig.multicastReplyAddress, ioManager);

    *responder = MulticastRpcResponder::ptr(new MulticastRpcResponder(listenSocket, multicastGroup, replySocket));
    (*responder)->addHandler(RpcMessageData::PING, ponger);
    (*responder)->addHandler(RpcMessageData::SET_RING, setRingHandler);
    (*responder)->addHandler(RpcMessageData::PAXOS_BATCH_PHASE1, batchPhase1Handler);
    (*responder)->addHandler(RpcMessageData::PAXOS_PHASE1, phase1Handler);
    (*responder)->addHandler(RpcMessageData::PAXOS_PHASE2, phase2Handler);
}

AcceptorState::ptr createAcceptor(const JSON::Value& config) {
    uint64_t pendingLimit = config["acceptor_max_pending_instances"].get<long long>();
    uint64_t committedLimit = config["acceptor_max_committed_instances"].get<long long>();
    return AcceptorState::ptr(new AcceptorState(pendingLimit, committedLimit));
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

    AcceptorState::ptr acceptorState = createAcceptor(config);
    MulticastRpcResponder::ptr responder;
    RingVoter::ptr ringVoter;
    setupEverything(&ioManager, configGuid, config, id, acceptorState, &responder, &ringVoter);
    ioManager.schedule(boost::bind(&MulticastRpcResponder::run, responder));
    ioManager.schedule(boost::bind(&RingVoter::run, ringVoter));
    ioManager.dispatch();
}
