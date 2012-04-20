#include "acceptor_state.h"
#include "batch_phase1_handler.h"
#include "phase1_handler.h"
#include "phase2_handler.h"
#include "recovery_handler.h"
#include "guid.h"
#include "host_configuration.h"
#include "instance_sink.h"
#include "recovery_manager.h"
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

class DummySink : public InstanceSink {
public:
    void push(const Guid&, paxos::InstanceId iid, paxos::BallotId, paxos::Value v)
    {
        cout << "Committed iid " << iid << ": " << v << endl;
    }
};

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

RpcRequester::ptr setupRequester(IOManager* ioManager,
                                 GuidGenerator::ptr guidGenerator,
                                 GroupConfiguration::ptr groupConfiguration,
                                 MulticastRpcStats::ptr rpcStats)
{
    Address::ptr bindAddr = groupConfiguration->host(groupConfiguration->thisHostId()).multicastSourceAddress;
    Socket::ptr s = bindAddr->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddr);
    UdpSender::ptr sender(new UdpSender("rpc_requester", s));
    ioManager->schedule(boost::bind(&UdpSender::run, sender));
    return RpcRequester::ptr(new RpcRequester(ioManager, guidGenerator, sender, s, groupConfiguration, rpcStats));
}

void setupEverything(IOManager* ioManager, 
                     const Guid& configHash,
                     const JSON::Value& config,
                     const string& datacenter, 
                     RpcResponder::ptr* responder,
                     RingVoter::ptr* ringVoter)
{
    Address::ptr multicastGroup = Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();
    GroupConfiguration::ptr groupConfig = GroupConfiguration::parseLearnerConfig(config["hosts"], datacenter, multicastGroup);

    //-------------------------------------------------------------------------
    // rpc requester
    GuidGenerator::ptr guidGenerator(new GuidGenerator);
    const uint64_t sendWindowUs = config["send_window"].get<long long>();
    const uint64_t recvWindowUs = config["recv_window"].get<long long>();
    MulticastRpcStats::ptr rpcStats(new MulticastRpcStats(sendWindowUs, recvWindowUs));
    RpcRequester::ptr requester = setupRequester(ioManager, guidGenerator, groupConfig, rpcStats);
    ioManager->schedule(boost::bind(&RpcRequester::processReplies, requester));

    //-------------------------------------------------------------------------
    // recovery manager
    const uint64_t recoveryInterval = config["recovery_interval"].get<long long>();
    const uint64_t recoveryTimeout  = config["recovery_timeout"].get<long long>();
    const uint64_t initialBackoff   = config["initial_backoff"].get<long long>();
    const uint64_t maxBackoff       = config["max_backoff"].get<long long>();
    RecoveryManager::ptr recoveryManager(new RecoveryManager(groupConfig, requester, ioManager, recoveryInterval, recoveryTimeout, initialBackoff, maxBackoff));
    ioManager->schedule(boost::bind(&RecoveryManager::recoverInstances, recoveryManager));

    //-------------------------------------------------------------------------
    // acceptor state
    uint64_t pendingLimit = config["acceptor_max_pending_instances"].get<long long>();
    uint64_t committedLimit = config["acceptor_instance_window_size"].get<long long>();
    uint64_t recoveryGracePeriod = config["recovery_grace_period"].get<long long>();
    boost::shared_ptr<InstanceSink> sink(new DummySink);
    AcceptorState::ptr acceptorState(new AcceptorState(pendingLimit, committedLimit, recoveryGracePeriod, ioManager, recoveryManager, sink));

    //-------------------------------------------------------------------------
    // ring voter
    Socket::ptr ringSocket = bindSocket(groupConfig->host(groupConfig->thisHostId()).ringAddress, ioManager);
    UdpSender::ptr udpSender(new UdpSender("ring_voter", ringSocket));
    ioManager->schedule(boost::bind(&UdpSender::run, udpSender));
    *ringVoter = RingVoter::ptr(new RingVoter(ringSocket, udpSender, acceptorState));

    //-------------------------------------------------------------------------
    // RPC handlers
    RpcHandler::ptr ponger(new Ponger);
    boost::shared_ptr<BatchPhase1Handler> batchPhase1Handler(new BatchPhase1Handler(acceptorState));
    boost::shared_ptr<Phase1Handler> phase1Handler(new Phase1Handler(acceptorState));
    boost::shared_ptr<Phase2Handler> phase2Handler(new Phase2Handler(acceptorState, *ringVoter));
    boost::shared_ptr<RecoveryHandler> recoveryHandler(new RecoveryHandler(acceptorState));

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
    (*responder)->addHandler(RpcMessageData::RECOVERY, recoveryHandler);
}

int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 3) {
        cout << "Usage: learner config.json datacenter" << endl;
        return 1;
    }
    Guid configGuid;
    JSON::Value config;
    readConfig(argv[1], &configGuid, &config);
    const string datacenter(argv[2]);
    IOManager ioManager;

    RpcResponder::ptr responder;
    RingVoter::ptr ringVoter;
    setupEverything(&ioManager, configGuid, config, datacenter, &responder, &ringVoter);
    ioManager.schedule(boost::bind(&RpcResponder::run, responder));
    ioManager.schedule(boost::bind(&RingVoter::run, ringVoter));
    ioManager.schedule(boost::bind(serveStats, &ioManager));
    ioManager.dispatch();
}
