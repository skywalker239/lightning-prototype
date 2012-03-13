#include "acceptor_state.h"
#include "batch_phase1_handler.h"
#include "phase1_handler.h"
#include "phase2_handler.h"
#include "guid.h"
#include "host_configuration.h"
#include "rpc_responder.h"
#include "ring_holder.h"
#include "ring_voter.h"
#include "ring_change_notifier.h"
#include "set_ring_handler.h"
#include "ponger.h"
#include "udp_sender.h"
#include <iostream>
#include <fstream>
#include <streambuf>
#include <netinet/in.h>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <mordor/json.h>
#include <mordor/streams/socket.h>
#include <mordor/streams/memory.h>
#include <mordor/http/server.h>
#include <mordor/config.h>
#include <mordor/sleep.h>
#include <mordor/socket.h>
#include <mordor/statistics.h>
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
    ifstream f(filename.c_str());
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

static Logger::ptr g_log = Log::lookup("lightning:main");

void httpRequest(HTTP::ServerRequest::ptr request) {
    ostringstream ss;
    ss << Statistics::dump();
    MemoryStream::ptr responseStream(new MemoryStream);
    string response(ss.str());
    responseStream->write(response.c_str(), response.length());
    responseStream->seek(0);
    HTTP::respondStream(request, responseStream);
}

void serveStats(IOManager* ioManager) {
    Socket s(*ioManager, AF_INET, SOCK_STREAM);
    IPv4Address address(INADDR_ANY, 8080);

    s.bind(address);
    s.listen();

    while(true) {
        Socket::ptr socket = s.accept();
        Stream::ptr stream(new SocketStream(socket));
        HTTP::ServerConnection::ptr conn(new HTTP::ServerConnection(stream, &httpRequest));
        conn->processRequests();
    }
}

void setupEverything(IOManager* ioManager, 
                     const Guid& configHash,
                     const JSON::Value& config,
                     uint32_t ourId,
                     AcceptorState::ptr acceptorState,
                     RpcResponder::ptr* responder,
                     RingVoter::ptr* ringVoter)
{
    Address::ptr multicastGroup = Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();
    GroupConfiguration::ptr groupConfig = parseGroupConfiguration(config["hosts"], ourId, multicastGroup);

    Socket::ptr ringSocket = bindSocket(groupConfig->host(groupConfig->thisHostId()).ringAddress, ioManager);
    UdpSender::ptr udpSender(new UdpSender("ring_voter", ringSocket));
    ioManager->schedule(boost::bind(&UdpSender::run, udpSender));
    *ringVoter = RingVoter::ptr(new RingVoter(ringSocket, udpSender, acceptorState));

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

    Socket::ptr listenSocket = bindSocket(hostConfig.multicastListenAddress, ioManager);
    Socket::ptr replySocket = bindSocket(hostConfig.multicastReplyAddress, ioManager);

    *responder = RpcResponder::ptr(new RpcResponder(listenSocket, multicastGroup, replySocket));
    (*responder)->addHandler(RpcMessageData::PING, ponger);
    (*responder)->addHandler(RpcMessageData::SET_RING, setRingHandler);
    (*responder)->addHandler(RpcMessageData::PAXOS_BATCH_PHASE1, batchPhase1Handler);
    (*responder)->addHandler(RpcMessageData::PAXOS_PHASE1, phase1Handler);
    (*responder)->addHandler(RpcMessageData::PAXOS_PHASE2, phase2Handler);
}

AcceptorState::ptr createAcceptor(const JSON::Value& config) {
    uint64_t pendingLimit = config["acceptor_max_pending_instances"].get<long long>();
    uint64_t committedLimit = config["acceptor_instance_window_size"].get<long long>();
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
    RpcResponder::ptr responder;
    RingVoter::ptr ringVoter;
    setupEverything(&ioManager, configGuid, config, id, acceptorState, &responder, &ringVoter);
    ioManager.schedule(boost::bind(&RpcResponder::run, responder));
    ioManager.schedule(boost::bind(&RingVoter::run, ringVoter));
    ioManager.schedule(boost::bind(serveStats, &ioManager));
    ioManager.dispatch();
}
