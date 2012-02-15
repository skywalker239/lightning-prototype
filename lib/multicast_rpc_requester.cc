#include "multicast_rpc_requester.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <mordor/timer.h>
#include <set>
#include <sstream>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Socket;
using Mordor::Statistics;
using Mordor::CountStatistic;
using Mordor::Timer;
using Mordor::TimerManager;
using std::ostringstream;
using std::set;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:multicast_rpc_requester");

static CountStatistic<uint64_t>& g_outPackets =
    Statistics::registerStatistic("multicast_rpc_requester.out_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_inPackets =
    Statistics::registerStatistic("multicast_rpc_requester.in_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_outBytes =
    Statistics::registerStatistic("multicast_rpc_requester.out_bytes",
                                  CountStatistic<uint64_t>("bytes"));
static CountStatistic<uint64_t>& g_inBytes =
    Statistics::registerStatistic("multicast_rpc_requester.in_bytes",
                                  CountStatistic<uint64_t>("bytes"));

MulticastRpcRequester::MulticastRpcRequester(
    IOManager* ioManager,
    GuidGenerator::ptr guidGenerator,
    Socket::ptr socket,
    Address::ptr groupMulticastAddress,
    GroupConfiguration::ptr groupConfiguration)
    : ioManager_(ioManager),
      guidGenerator_(guidGenerator),
      socket_(socket),
      groupMulticastAddress_(groupMulticastAddress),
      groupConfiguration_(groupConfiguration)
{
    MORDOR_LOG_TRACE(g_log) << this << " init group='" <<
                            *groupMulticastAddress_;
    setupSocket();
}

void MulticastRpcRequester::setupSocket() {
    const int kMaxMulticastTtl = 255;
    socket_->setOption(IPPROTO_IP, IP_MULTICAST_TTL, kMaxMulticastTtl);
}

void MulticastRpcRequester::processReplies() {
    Address::ptr currentSourceAddress = socket_->emptyAddress();
    while(true) {
        char buffer[kMaxCommandSize + 1];
        ssize_t bytes = socket_->receiveFrom((void*) buffer,
                                             kMaxCommandSize,
                                             *currentSourceAddress);
        g_inPackets.increment();
        g_inBytes.add(bytes);

        RpcMessageData reply;
        if(!reply.ParseFromArray(buffer, bytes)) {
            MORDOR_LOG_WARNING(g_log) << this << " failed to parse reply " <<
                                         "from " <<
                                         groupConfiguration_->addressToServiceName(currentSourceAddress);
            continue;
        }

        Guid replyGuid = Guid::parse(reply.uuid());
        MulticastRpcRequest::ptr request;
        {
            FiberMutex::ScopedLock lk(mutex_);
            auto requestIter = pendingRequests_.find(replyGuid);
            if(requestIter != pendingRequests_.end()) {
                request = requestIter->second;
            }
        }
        if(!request) {
            MORDOR_LOG_WARNING(g_log) << this << " stale reply for request " <<
                                         replyGuid << " from " <<
                                         groupConfiguration_->addressToServiceName(currentSourceAddress);
            continue;
        }
        MORDOR_LOG_TRACE(g_log) << this << " got reply for request " <<
                                   replyGuid << " from " <<
                                   groupConfiguration_->addressToServiceName(currentSourceAddress);
        request->onReply(currentSourceAddress, reply);
    }
}

void MulticastRpcRequester::timeoutRequest(const Guid& requestId) {
    MulticastRpcRequest::ptr request;
    {
        FiberMutex::ScopedLock lk(mutex_);
        auto requestIter = pendingRequests_.find(requestId);
        if(requestIter == pendingRequests_.end()) {
            MORDOR_LOG_TRACE(g_log) << this << " timeout request " <<
                                       requestId << " not found";
            return;
        }
        request = requestIter->second;
        pendingRequests_.erase(requestIter);
    }
    MORDOR_LOG_TRACE(g_log) << this << " timed out request " << requestId;
    request->onTimeout();
}

void MulticastRpcRequester::sendRequests() {
    while(true) {
        MulticastRpcRequest::ptr request = sendQueue_.pop();
        const Guid& requestGuid = request->rpcGuid();
        MORDOR_LOG_TRACE(g_log) << this << " popped request " <<
                                   requestGuid;
        RpcMessageData requestData;
        requestData.MergeFrom(request->request());
        requestGuid.serialize(requestData.mutable_uuid());

        char buffer[kMaxCommandSize];
        uint64_t commandSize = requestData.ByteSize();
        MORDOR_LOG_TRACE(g_log) << this << " command size is " << commandSize;
        MORDOR_ASSERT(commandSize <= kMaxCommandSize);
        if(!requestData.SerializeToArray(buffer, kMaxCommandSize)) {
            MORDOR_LOG_WARNING(g_log) << this << " failed to serialize request " <<
                                         requestGuid;
            request->onTimeout();
            continue;
        }

        {
            FiberMutex::ScopedLock lk(mutex_);
            pendingRequests_[requestGuid] = request;
            request->setRpcGuid(requestGuid);
        }

        socket_->sendTo((const void*) buffer,
                        commandSize,
                        0,
                        *groupMulticastAddress_);
        g_outPackets.increment();
        g_outBytes.add(commandSize);
        Timer::ptr timeoutTimer =
            ioManager_->registerTimer(request->timeoutUs(),
                                      boost::bind(
                                          &MulticastRpcRequester::timeoutRequest,
                                          this,
                                          requestGuid));
        request->setTimeoutTimer(timeoutTimer);
    }
}

MulticastRpcRequest::Status MulticastRpcRequester::request(
    MulticastRpcRequest::ptr request)
{
    Guid requestGuid = guidGenerator_->generate();
    request->setRpcGuid(requestGuid);

    MORDOR_LOG_TRACE(g_log) << this << " new request " << requestGuid;
    sendQueue_.push(request);

    request->wait();
    request->cancelTimeoutTimer();
    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_.erase(requestGuid);
        MORDOR_LOG_TRACE(g_log) << this << " removed request " <<
                                   requestGuid << " from pending";
    }
    MulticastRpcRequest::Status status = request->status();
    MORDOR_ASSERT(status != MulticastRpcRequest::IN_PROGRESS);
    return request->status();
}

}  // namespace lightning
