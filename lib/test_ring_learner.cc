#include "acceptor_state.h"
#include "batch_phase1_handler.h"
#include "phase1_handler.h"
#include "phase2_handler.h"
#include "guid.h"
#include "host_configuration.h"
#include "instance_sink.h"
#include "recovery_manager.h"
#include "rpc_responder.h"
#include "ring_holder.h"
#include "ring_voter.h"
#include "ring_change_notifier.h"
#include "set_ring_handler.h"
#include "stream_reassembler.h"
#include "ponger.h"
#include "udp_sender.h"
#include "value_cache.h"
#include "commit_tracker.h"
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
#include <mordor/streams/std.h>
#include <mordor/iomanager.h>
#include <mordor/timer.h>
#include <stdlib.h>

using namespace std;
using namespace Mordor;
using namespace lightning;
using boost::lexical_cast;

static Logger::ptr g_log = Log::lookup("lightning:main");

class SnapshotLearnerSink : public InstanceSink {
public:
    SnapshotLearnerSink(uint64_t snapshotId,
                        uint64_t transferTimeoutUs,
                        StreamReassembler::ptr streamReassembler,
                        IOManager* ioManager)
        : snapshotId_(snapshotId),
          transferTimeoutUs_(transferTimeoutUs),
          streamReassembler_(streamReassembler),
          ioManager_(ioManager)
    {
        timeoutTimer_ = ioManager_->registerTimer(transferTimeoutUs_,
                                                  &SnapshotLearnerSink::onTransferTimeout);
    }

    void updateEpoch(const Guid& newEpoch) {
        FiberMutex::ScopedLock lk(mutex_);
        MORDOR_LOG_INFO(g_log) << " new epoch " << newEpoch;
        MORDOR_ASSERT(epoch_.empty());
        epoch_ = newEpoch;
    }

    void push(paxos::InstanceId iid, paxos::BallotId ballot, paxos::Value v) {
        {
            FiberMutex::ScopedLock lk(mutex_);
            if(timeoutTimer_) {
                timeoutTimer_->cancel();
            }

            timeoutTimer_ = ioManager_->registerTimer(transferTimeoutUs_,
                                                      &SnapshotLearnerSink::onTransferTimeout);
        }

        MORDOR_LOG_TRACE(g_log) << " (" << iid << ", " << ballot << ", " << v << ")";
        if(v.valueId().empty()) {
            return;
        }
        Guid valueId;
        boost::shared_ptr<string> valueData;
        v.release(&valueId, &valueData);
        SnapshotStreamData snapshotStreamData;
        if(snapshotStreamData.ParseFromString(*valueData.get())) {
            if(snapshotStreamData.snapshot_id() == snapshotId_) {
                if(!snapshotStreamData.has_data()) {
                    streamReassembler_->setEnd(snapshotStreamData.position());
                } else {
                    boost::shared_ptr<string> chunkData(new string(snapshotStreamData.data()));
                    streamReassembler_->addChunk(snapshotStreamData.position(), chunkData);
                }
            }
        }
    }

    static void onTransferTimeout() {
        MORDOR_LOG_INFO(g_log) << " snapshot timed out, exiting.";
        exit(1);
    }
private:
    const uint64_t snapshotId_;
    const uint64_t transferTimeoutUs_;
    Guid epoch_;
    StreamReassembler::ptr streamReassembler_;
    IOManager* ioManager_;

    Timer::ptr timeoutTimer_;

    FiberMutex mutex_;
};
    
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

class DummyRingHolder : public RingHolder {};

Socket::ptr bindSocket(Address::ptr bindAddress, IOManager* ioManager) {
    Socket::ptr s = bindAddress->createSocket(*ioManager, SOCK_DGRAM);
    s->bind(bindAddress);
    return s;
}


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

void dumpStream(IOManager* ioManager,
                StreamReassembler::ptr streamReassembler)
{
    StdoutStream outputStream(*ioManager);
    bool gotData = false;
    uint64_t startTime = 0;
    uint64_t written = 0;
    while(true) {
        boost::shared_ptr<string> chunk = streamReassembler->nextChunk();
        if(!chunk) {
            ioManager->stop();
            cerr << Statistics::dump() << endl;
            uint64_t endTime = TimerManager::now();
            MORDOR_LOG_INFO(g_log) << " Received " << written << " data bytes";
            cerr << "Received " << written << " bytes" << endl;
            cerr << "Time: " << endTime - startTime << " us" << endl;
            cerr << "Speed: " << int(written / ((endTime - startTime) / 1000000.)) << " bps.";
            exit(0); // HACK
        }
        if(!gotData) {
            gotData = true;
            startTime = TimerManager::now();
        }
        size_t writtenNow = 0;
        while(writtenNow < chunk->length()) {
            size_t bytes =
                outputStream.write(chunk->c_str() + writtenNow,
                                   chunk->length() - writtenNow);
            MORDOR_ASSERT(bytes > 0);
            writtenNow += bytes;
        }
        written += chunk->length();
        Scheduler::yield();
    }
}


void setupEverything(IOManager* ioManager, 
                     const Guid& configHash,
                     const JSON::Value& config,
                     const string& datacenter,
                     uint64_t snapshotId,
                     uint64_t timeoutUs,
                     StreamReassembler::ptr streamReassembler,
                     RpcResponder::ptr* responder,
                     RingVoter::ptr* ringVoter,
                     uint16_t* monPort)
{
    Address::ptr multicastGroup = Address::lookup(config["mcast_group"].get<string>(), AF_INET).front();
    GroupConfiguration::ptr groupConfig = GroupConfiguration::parseLearnerConfig(config["hosts"], datacenter, multicastGroup);

    *monPort = uint16_t(config["monitoring_port"].get<long long>());

    //-------------------------------------------------------------------------
    // recovery manager
    const uint32_t localMetric      = config["recovery_local_metric"].get<long long>();
    const uint32_t remoteMetric     = config["recovery_remote_metric"].get<long long>();
    const uint64_t queuePollIntervalUs = config["recovery_queue_poll_interval"].get<long long>();
    const uint64_t reconnectDelayUs = config["recovery_reconnect_delay"].get<long long>();
    const uint64_t socketTimeoutUs  = config["recovery_socket_timeout"].get<long long>();
    const uint64_t retryDelayUs     = config["recovery_retry_delay"].get<long long>();
    RecoveryManager::ptr recoveryManager(new RecoveryManager);
    ioManager->schedule(boost::bind(&RecoveryManager::processMainQueue, recoveryManager));
    ioManager->schedule(boost::bind(&RecoveryManager::processRandomDestinationQueue, recoveryManager));
    recoveryManager->setupConnections(groupConfig,
                                      ioManager,
                                      localMetric,
                                      remoteMetric,
                                      queuePollIntervalUs,
                                      reconnectDelayUs,
                                      socketTimeoutUs,
                                      retryDelayUs);
    //-------------------------------------------------------------------------
    // commit tracker
    uint64_t recoveryGracePeriod = config["recovery_grace_period"].get<long long>();
    boost::shared_ptr<InstanceSink> sink(new SnapshotLearnerSink(snapshotId, timeoutUs, streamReassembler, ioManager));
    CommitTracker::ptr commitTracker(new CommitTracker(recoveryGracePeriod, sink, recoveryManager, ioManager));
    recoveryManager->setCommitTracker(commitTracker);

    //-------------------------------------------------------------------------
    // acceptor state
    uint64_t pendingSpan = config["acceptor_pending_instances_span"].get<long long>();
    AcceptorState::ptr acceptorState(new AcceptorState(pendingSpan, ioManager, recoveryManager, commitTracker, ValueCache::ptr()));

    //-------------------------------------------------------------------------
    // ring voter
    Socket::ptr ringSocket = bindSocket(groupConfig->thisHostConfiguration().ringAddress, ioManager);
    UdpSender::ptr udpSender(new UdpSender("ring_voter", ringSocket));
    ioManager->schedule(boost::bind(&UdpSender::run, udpSender));
    *ringVoter = RingVoter::ptr(new RingVoter(ringSocket, udpSender, acceptorState));

    //-------------------------------------------------------------------------
    // RPC handlers
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

    const HostConfiguration& hostConfig = groupConfig->thisHostConfiguration();
    //cout << hostConfig << endl;

    Socket::ptr listenSocket = bindSocket(hostConfig.multicastListenAddress, ioManager);
    Socket::ptr replySocket = bindSocket(hostConfig.multicastReplyAddress, ioManager);

    *responder = RpcResponder::ptr(new RpcResponder(listenSocket, multicastGroup, replySocket));
//  Learners don't have to respond to pings
//    (*responder)->addHandler(RpcMessageData::PING, ponger);
    (*responder)->addHandler(RpcMessageData::SET_RING, setRingHandler);
    (*responder)->addHandler(RpcMessageData::PAXOS_BATCH_PHASE1, batchPhase1Handler);
    (*responder)->addHandler(RpcMessageData::PAXOS_PHASE1, phase1Handler);
    (*responder)->addHandler(RpcMessageData::PAXOS_PHASE2, phase2Handler);
}

int main(int argc, char** argv) {
    google::protobuf::LogSilencer logSilencer;
    try {
        Config::loadFromEnvironment();
        if(argc != 5) {
            cout << "Usage: learner datacenter snapshot_id timeout_sec config_json" << endl;
            return 1;
        }
        Guid configGuid;
        JSON::Value config;
        readConfig(argv[4], &configGuid, &config);
        const string datacenter(argv[1]);
        const uint64_t snapshotId = boost::lexical_cast<uint64_t>(argv[2]);
        const uint64_t timeoutUs = 1000000 * boost::lexical_cast<uint64_t>(argv[3]);
        IOManager ioManager;

        StreamReassembler::ptr streamReassembler(new StreamReassembler);
        RpcResponder::ptr responder;
        RingVoter::ptr ringVoter;
        uint16_t monPort;
        setupEverything(&ioManager, configGuid, config, datacenter, snapshotId, timeoutUs, streamReassembler, &responder, &ringVoter, &monPort);
        ioManager.schedule(boost::bind(&RpcResponder::run, responder));
        ioManager.schedule(boost::bind(&RingVoter::run, ringVoter));
        ioManager.schedule(boost::bind(serveStats, &ioManager, monPort));
        ioManager.schedule(boost::bind(dumpStream, &ioManager, streamReassembler));
        MORDOR_LOG_INFO(g_log) << " Learner starting.";
        ioManager.dispatch();
    } catch(...) {
        cerr << boost::current_exception_diagnostic_information() << endl;
    }
}
