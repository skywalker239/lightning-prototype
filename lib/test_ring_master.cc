#include "guid.h"
#include "ballot_generator.h"
#include "proposer_state.h"
#include "phase1_batcher.h"
#include "sleep_helper.h"
#include "tcp_recovery_service.h"
#include "tcp_value_receiver.h"
#include "udp_sender.h"
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
#include <mordor/sleep.h>
#include <mordor/streams/memory.h>
#include <mordor/streams/socket.h>
#include <mordor/http/server.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>
#include <map>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
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
using namespace paxos;
using boost::lexical_cast;
using boost::shared_ptr;

static Logger::ptr g_log = Log::lookup("lightning:main");

void readConfig(const char* configString,
                Guid* configHash,
                JSON::Value* config)
{
    size_t configLength = strlen(configString);
    *configHash = Guid::fromData(configString, configLength);
    *config = JSON::parse(configString);

    MORDOR_LOG_INFO(g_log) << " running with config " << configString;
    MORDOR_LOG_INFO(g_log) << " config hash is " << *configHash;
}

RpcRequester::ptr setupRequester(IOManager* ioManager,
                                          GuidGenerator::ptr guidGenerator,
                                          GroupConfiguration::ptr groupConfiguration,
                                          MulticastRpcStats::ptr rpcStats)
{
    Address::ptr bindAddr = groupConfiguration->thisHostConfiguration().multicastSourceAddress;
    Socket::ptr s = bindAddr->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddr);
    UdpSender::ptr sender(new UdpSender("rpc_requester", s));
    ioManager->schedule(boost::bind(&UdpSender::run, sender));
    return RpcRequester::ptr(new RpcRequester(ioManager, guidGenerator, sender, s, groupConfiguration, rpcStats));
}

Socket::ptr bindSocket(Address::ptr bindAddress, IOManager* ioManager, int protocol = SOCK_DGRAM) {
    Socket::ptr s = bindAddress->createSocket(*ioManager, protocol);
    int option = 1;
    s->setOption(SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
    s->bind(bindAddress);
    return s;
}

class DummyRingHolder : public RingHolder {};

void setupEverything(uint32_t hostId,
                     const Guid& configHash,
                     const JSON::Value& config,
                     IOManager* ioManager,
                     Pinger::ptr* pinger,
                     RingManager::ptr* ringManager,
                     Phase1Batcher::ptr* phase1Batcher,
                     ProposerState::ptr* proposerState,
                     BlockingQueue<Value>::ptr* valueQueue,
                     TcpValueReceiver::ptr* tcpValueReceiver,
                     uint16_t* monPort)
{
    Address::ptr groupMcastAddress =
        Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();;
    GroupConfiguration::ptr groupConfiguration = GroupConfiguration::parseAcceptorConfig(config["hosts"],
                                                                                         hostId,
                                                                                         groupMcastAddress);

    *monPort = uint16_t(config["monitoring_port"].get<long long>());

    const uint64_t pingWindow = config["ping_window"].get<long long>();
    const uint64_t pingTimeout = config["ping_timeout"].get<long long>();
    const uint64_t pingInterval = config["ping_interval"].get<long long>();
    const uint64_t hostTimeout = config["host_timeout"].get<long long>();
    const uint64_t ringTimeout = config["ring_timeout"].get<long long>();
    const uint64_t ringRetryInterval = config["ring_retry_interval"].get<long long>();
    const uint64_t ringBroadcastInterval = config["ring_broadcast_interval"].get<long long>();

    GuidGenerator::ptr guidGenerator(new GuidGenerator);
    boost::shared_ptr<FiberEvent> event(new FiberEvent);

    const uint64_t sendWindowUs = config["send_window"].get<long long>();
    const uint64_t recvWindowUs = config["recv_window"].get<long long>();
    MulticastRpcStats::ptr rpcStats(new MulticastRpcStats(sendWindowUs, recvWindowUs));

    RpcRequester::ptr requester = setupRequester(ioManager, guidGenerator, groupConfiguration, rpcStats);
    ioManager->schedule(boost::bind(&RpcRequester::processReplies, requester));

    PingTracker::ptr pingTracker(new PingTracker(groupConfiguration, pingWindow, pingTimeout, hostTimeout, event, ioManager));
    *pinger = Pinger::ptr(new Pinger(ioManager, requester, groupConfiguration, pingInterval, pingTimeout, pingTracker));

    RingOracle::ptr oracle(new DatacenterAwareQuorumRingOracle(groupConfiguration, true, hostTimeout));

    const uint64_t maxP1OpenInstances = config["instance_pool_open_limit"].get<long long>();
    const uint64_t maxP1ReservedInstances = config["instance_pool_reserved_limit"].get<long long>();
    const uint64_t phase1BatchSize = config["phase1_batch_size"].get<long long>();
    const uint64_t phase1BatchTimeout = config["batch_phase1_timeout"].get<long long>();
    BallotGenerator ballotGenerator(groupConfiguration);

    const Guid epoch = guidGenerator->generate();
    MORDOR_LOG_INFO(g_log) << " Master epoch = " << epoch;

    boost::shared_ptr<FiberEvent> batchPhase1SyncEvent(new FiberEvent(false));
    batchPhase1SyncEvent->set();
    InstancePool::ptr instancePool(new InstancePool(maxP1OpenInstances, maxP1ReservedInstances, batchPhase1SyncEvent));
    *phase1Batcher = Phase1Batcher::ptr(
                         new Phase1Batcher(epoch,
                                           phase1BatchTimeout,
                                           phase1BatchSize,
                                           ballotGenerator,
                                           instancePool,
                                           requester,
                                           batchPhase1SyncEvent));

    const uint64_t phase1TimeoutUs =
        config["phase1_timeout"].get<long long>();
    const uint64_t phase1IntervalUs =
        config["phase1_interval"].get<long long>();
    const uint64_t phase2TimeoutUs =
        config["phase2_timeout"].get<long long>();
    const uint64_t phase2IntervalUs =
        config["phase2_interval"].get<long long>();
    const uint64_t commitFlushIntervalUs =
        config["commit_flush_interval"].get<long long>();
    const uint64_t valueCacheSize =
        config["value_cache_size"].get<long long>();
    ValueCache::ptr valueCache(new ValueCache(valueCacheSize));

    valueQueue->reset(new BlockingQueue<Value>("client_value_queue"));
    *proposerState =
        ProposerState::ptr(new ProposerState(groupConfiguration,
                                             epoch,
                                             instancePool,
                                             requester,
                                             *valueQueue,
                                             valueCache,
                                             ioManager,
                                             phase1TimeoutUs,
                                             phase1IntervalUs,
                                             phase2TimeoutUs,
                                             phase2IntervalUs,
                                             commitFlushIntervalUs));

    Socket::ptr recoverySocket = bindSocket(groupConfiguration->thisHostConfiguration().unicastAddress, ioManager, SOCK_STREAM);
    recoverySocket->listen();
    TcpRecoveryService::ptr tcpRecoveryService(
        new TcpRecoveryService(ioManager, recoverySocket, valueCache));
    ioManager->schedule(boost::bind(&TcpRecoveryService::run, tcpRecoveryService));

    const uint16_t masterValuePort = config["master_value_port"].get<long long>();
    const uint64_t valueBufferSize = config["value_buffer_size"].get<long long>();
    Socket::ptr valueSocket(new Socket(*ioManager, AF_INET, SOCK_STREAM));
    IPv4Address valueAddress(INADDR_ANY, masterValuePort);
    valueSocket->bind(valueAddress);
    valueSocket->listen();

    tcpValueReceiver->reset(new TcpValueReceiver(valueBufferSize, *proposerState, *valueQueue, ioManager, valueSocket));

    vector<RingHolder::ptr> ringHolders;
    ringHolders.push_back(*phase1Batcher);
    ringHolders.push_back(*proposerState);
    RingChangeNotifier::ptr notifier(new RingChangeNotifier(ringHolders));
    *ringManager = RingManager::ptr(new RingManager(groupConfiguration, configHash, ioManager, event, requester, pingTracker, oracle, notifier, ringTimeout, ringRetryInterval, ringBroadcastInterval));
}

//void dumpStats(IOManager* ioManager) {
//    while(true) {
//        cerr << Statistics::dump() << endl;
//        sleep(*ioManager, 1000000);
//    }
//}

void httpRequest(HTTP::ServerRequest::ptr request) {
    ostringstream ss;
    ss << Statistics::dump();
    MemoryStream::ptr responseStream(new MemoryStream);
    string response(ss.str());
    responseStream->write(response.c_str(), response.length());
    responseStream->seek(0);
    HTTP::respondStream(request, responseStream);
}

void serveStats(IOManager* ioManager, uint16_t port) {
    Socket s(*ioManager, AF_INET, SOCK_STREAM);
    IPv4Address address(INADDR_ANY, port);

    s.bind(address);
    s.listen();

    while(true) {
        Socket::ptr socket = s.accept();
        Stream::ptr stream(new SocketStream(socket));
        HTTP::ServerConnection::ptr conn(new HTTP::ServerConnection(stream, &httpRequest));
        conn->processRequests();
    }
}

//void submitValues(IOManager* ioManager,
//                  BlockingQueue<Value>::ptr valueQueue,
//                  GuidGenerator::ptr guidGenerator)
//{
//    const int64_t kSleepPrecision = 1000;
//    const int64_t kSleepInterval = 64; // 64;
//    sleep(*ioManager, 3500000); // let the ring selection happen
//    const size_t kValuesToSubmit = 468750; // 60 sec @ 1 Gbps
//
//    SleepHelper sleeper(ioManager, kSleepInterval, kSleepPrecision);
//    for(size_t i = 0; i < kValuesToSubmit; ++i) {
//        shared_ptr<string> data(new string(8000, ' '));
//        auto valueId = guidGenerator->generate();
//        Value v(valueId, data);
//        valueQueue->push(v);
//        MORDOR_LOG_DEBUG(g_log) << " pushed value " << v;
//        sleeper.wait();
//    }
//}

int getPidFd(const char* pidFile) {
    int fd = open(pidFile, O_CREAT | O_EXCL | O_WRONLY, 0644);
    return fd;
}

void writePidFile(int fd) {
    dprintf(fd, "%d", getpid());
    close(fd);
}

int main(int argc, char** argv) {
    Config::loadFromEnvironment();
    if(argc != 4) {
        cout << "Usage: master host_id pid_file config_json" << endl;
        return 0;
    }
    int pidFd = getPidFd(argv[2]);
    if(pidFd < 0) {
        MORDOR_LOG_INFO(g_log) << " Another instance running, not starting.";
        return 1;
    }
    const uint32_t hostId = lexical_cast<uint32_t>(argv[1]);
    Guid configHash;
    JSON::Value config;
    readConfig(argv[3], &configHash, &config);
    if(daemon(0, 0)) {
        MORDOR_LOG_ERROR(g_log) << " Failed to daemonize";
        return 1;
    }
    writePidFile(pidFd);

    try {
        IOManager ioManager;
        Pinger::ptr pinger;
        RingManager::ptr ringManager;
        Phase1Batcher::ptr phase1Batcher;
        ProposerState::ptr proposerState;
        BlockingQueue<Value>::ptr clientValueQueue;
        TcpValueReceiver::ptr tcpValueReceiver;
        uint16_t monPort;
        setupEverything(hostId, configHash, config, &ioManager, &pinger, &ringManager, &phase1Batcher, &proposerState, &clientValueQueue, &tcpValueReceiver, &monPort);
        GuidGenerator::ptr guidGenerator(new GuidGenerator);
        ioManager.schedule(boost::bind(&serveStats, &ioManager, monPort));
        ioManager.schedule(boost::bind(&Pinger::run, pinger));
        ioManager.schedule(boost::bind(&RingManager::run, ringManager));
        ioManager.schedule(boost::bind(&RingManager::broadcastRing, ringManager));
        ioManager.schedule(boost::bind(&Phase1Batcher::run, phase1Batcher));
        ioManager.schedule(boost::bind(&ProposerState::processReservedInstances, proposerState));
        ioManager.schedule(boost::bind(&ProposerState::processClientValues, proposerState));
        ioManager.schedule(boost::bind(&ProposerState::flushCommits, proposerState));
        ioManager.schedule(boost::bind(&TcpValueReceiver::run, tcpValueReceiver));
//        ioManager.schedule(boost::bind(dumpStats, &ioManager));
        MORDOR_LOG_INFO(g_log) << " Master starting.";
        ioManager.dispatch();
        return 0;
    } catch(...) {
        cout << boost::current_exception_diagnostic_information() << endl;
    }
}

