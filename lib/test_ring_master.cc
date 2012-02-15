#include "guid.h"
#include "ballot_generator.h"
#include "proposer_state.h"
#include "client_value_queue.h"
#include "phase1_batcher.h"
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
using namespace paxos;
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
                                          GroupConfiguration::ptr groupConfiguration,
                                          Address::ptr mcastDestination)
{
    Address::ptr bindAddr = groupConfiguration->host(groupConfiguration->thisHostId()).multicastSourceAddress;
    Socket::ptr s = bindAddr->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddr);
    return MulticastRpcRequester::ptr(new MulticastRpcRequester(ioManager, guidGenerator, s, mcastDestination, groupConfiguration));
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
                     ClientValueQueue::ptr* valueQueue)
{
    GroupConfiguration::ptr groupConfiguration = parseGroupConfiguration(config["hosts"],
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
    ioManager->schedule(boost::bind(&MulticastRpcRequester::processReplies, requester));
    ioManager->schedule(boost::bind(&MulticastRpcRequester::sendRequests, requester));

    PingTracker::ptr pingTracker(new PingTracker(groupConfiguration, pingWindow, pingTimeout, hostTimeout, event, ioManager));
    *pinger = Pinger::ptr(new Pinger(ioManager, requester, groupConfiguration, pingInterval, pingTimeout, pingTracker));

    RingOracle::ptr oracle(new DatacenterAwareQuorumRingOracle(groupConfiguration, true, hostTimeout));

    const uint64_t maxP1OpenInstances = config["instance_pool_open_limit"].get<long long>();
    const uint64_t maxP1ReservedInstances = config["instance_pool_reserved_limit"].get<long long>();
    const uint64_t phase1BatchSize = config["phase1_batch_size"].get<long long>();
    const uint64_t phase1BatchTimeout = config["batch_phase1_timeout"].get<long long>();
    BallotGenerator ballotGenerator(groupConfiguration);

    const Guid epoch = guidGenerator->generate();

    boost::shared_ptr<FiberEvent> batchPhase1SyncEvent(new FiberEvent(false));
    batchPhase1SyncEvent->set();
    InstancePool::ptr instancePool(new InstancePool(maxP1OpenInstances, maxP1ReservedInstances, batchPhase1SyncEvent));
    *phase1Batcher = Phase1Batcher::ptr(
                         new Phase1Batcher(epoch,
                                           phase1BatchTimeout,
                                           phase1BatchSize,
                                           ballotGenerator.boostBallotId(kInvalidBallotId),
                                           instancePool,
                                           requester,
                                           batchPhase1SyncEvent));

    const uint64_t phase1TimeoutUs =
        config["phase1_timeout"].get<long long>();
    const uint64_t phase2TimeoutUs =
        config["phase2_timeout"].get<long long>();

    *valueQueue = ClientValueQueue::ptr(new ClientValueQueue);
    *proposerState =
        ProposerState::ptr(new ProposerState(groupConfiguration,
                                             epoch,
                                             instancePool,
                                             requester,
                                             *valueQueue,
                                             ioManager,
                                             phase1TimeoutUs,
                                             phase2TimeoutUs));

    vector<RingHolder::ptr> ringHolders;
    ringHolders.push_back(*phase1Batcher);
    ringHolders.push_back(*proposerState);
    RingChangeNotifier::ptr notifier(new RingChangeNotifier(ringHolders));
    *ringManager = RingManager::ptr(new RingManager(groupConfiguration, configHash, ioManager, event, requester, pingTracker, oracle, notifier, ringTimeout, ringRetryInterval));
}

static Logger::ptr g_log = Log::lookup("lightning:main");

void submitValues(IOManager* ioManager,
                  ClientValueQueue::ptr valueQueue,
                  GuidGenerator::ptr guidGenerator)
{
    sleep(*ioManager, 3500000);
    const size_t kValuesToSubmit = 1300;
    for(size_t i = 0; i < kValuesToSubmit; ++i) {
        Value::ptr v(new Value);
        v->size = Value::kMaxValueSize;
        v->valueId = guidGenerator->generate();
        valueQueue->push(v);
        MORDOR_LOG_INFO(g_log) << " pushed value id=" << v->valueId << " size=" << v->size;
        sleep(*ioManager, 64);
    }
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

    try {
        IOManager ioManager;
        Pinger::ptr pinger;
        RingManager::ptr ringManager;
        Phase1Batcher::ptr phase1Batcher;
        ProposerState::ptr proposerState;
        ClientValueQueue::ptr clientValueQueue;
        setupEverything(hostId, configHash, config, &ioManager, &pinger, &ringManager, &phase1Batcher, &proposerState, &clientValueQueue);
        GuidGenerator::ptr guidGenerator(new GuidGenerator);
        ioManager.schedule(boost::bind(&Pinger::run, pinger));
        ioManager.schedule(boost::bind(&RingManager::run, ringManager));
        ioManager.schedule(boost::bind(&Phase1Batcher::run, phase1Batcher));
        ioManager.schedule(boost::bind(&ProposerState::processReservedInstances, proposerState));
        ioManager.schedule(boost::bind(&ProposerState::processClientValues, proposerState));
        ioManager.schedule(boost::bind(submitValues, &ioManager, clientValueQueue, guidGenerator));
        ioManager.dispatch();
        return 0;
    } catch(...) {
        cout << boost::current_exception_diagnostic_information() << endl;
    }
}

