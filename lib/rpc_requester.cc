#include "rpc_requester.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <mordor/timer.h>
#include <set>
#include <sstream>

namespace lightning {

const size_t RpcRequester::kMaxDatagramSize;

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

static Logger::ptr g_log = Log::lookup("lightning:rpc_requester");

static CountStatistic<uint64_t>& g_inPackets =
    Statistics::registerStatistic("rpc_requester.in_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_inBytes =
    Statistics::registerStatistic("rpc_requester.in_bytes",
                                  CountStatistic<uint64_t>("bytes"));

RpcRequester::RpcRequester(
    IOManager* ioManager,
    GuidGenerator::ptr guidGenerator,
    UdpSender::ptr udpSender,
    Socket::ptr socket,
    GroupConfiguration::ptr groupConfiguration,
    MulticastRpcStats::ptr rpcStats)
    : ioManager_(ioManager),
      guidGenerator_(guidGenerator),
      udpSender_(udpSender),
      socket_(socket),
      groupConfiguration_(groupConfiguration),
      rpcStats_(rpcStats)
{
}

void RpcRequester::processReplies() {
    Address::ptr currentSourceAddress = socket_->emptyAddress();
    while(true) {
        char buffer[kMaxDatagramSize + 1];
        ssize_t bytes = socket_->receiveFrom((void*) buffer,
                                             kMaxDatagramSize,
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

        rpcStats_->receivedPacket(bytes);

        Guid replyGuid = Guid::parse(reply.uuid());
        RpcRequest::ptr request;
        {
            FiberMutex::ScopedLock lk(mutex_);
            auto requestIter = pendingRequests_.find(replyGuid);
            if(requestIter != pendingRequests_.end()) {
                request = requestIter->second;
            }
        }
        if(!request) {
            MORDOR_LOG_DEBUG(g_log) << this << " stale reply for request " <<
                                       replyGuid << " from " <<
                                       groupConfiguration_->addressToServiceName(currentSourceAddress);
            continue;
        }
        MORDOR_LOG_TRACE(g_log) << this << " got reply for request (" <<
                                   replyGuid << ", " << *request << ") from " <<
                                   groupConfiguration_->addressToServiceName(currentSourceAddress);
        request->onReply(currentSourceAddress, reply);
    }
}

void RpcRequester::timeoutRequest(const Guid& requestId) {
    RpcRequest::ptr request;
    {
        FiberMutex::ScopedLock lk(mutex_);
        auto requestIter = pendingRequests_.find(requestId);
        if(requestIter == pendingRequests_.end()) {
            MORDOR_LOG_TRACE(g_log) << this << " timeout request " <<
                                       requestId << " not found";
            return;
        }
        request = requestIter->second;
        MORDOR_LOG_TRACE(g_log) << this << " timed out request " <<
                                   requestId << " = " <<
                                   *request;
        pendingRequests_.erase(requestIter);
    }
    request->onTimeout();
}

void RpcRequester::startTimeoutTimer(
    RpcRequest::ptr request)
{
    MORDOR_LOG_TRACE(g_log) << this << " request (" <<
                               request->rpcGuid() << ", " << *request <<
                               ") sent";
    Timer::ptr timeoutTimer = ioManager_->registerTimer(request->timeoutUs(),
                                  boost::bind(
                                  &RpcRequester::timeoutRequest,
                                  this,
                                  request->rpcGuid()));
    request->setTimeoutTimer(timeoutTimer);
}

void RpcRequester::onSendFail(RpcRequest::ptr request) {
    MORDOR_LOG_TRACE(g_log) << this << " failed to send " << *request;
    request->onTimeout();
}

RpcRequest::Status RpcRequester::request(
    RpcRequest::ptr request)
{
    Guid requestGuid = guidGenerator_->generate();
    request->setRpcGuid(requestGuid);

    MORDOR_LOG_TRACE(g_log) << this << " new request (" << requestGuid <<
                               ", " << *request << ")";
    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_[requestGuid] = request;
    }

    udpSender_->send(request->destination(),
                     boost::shared_ptr<const RpcMessageData>(
                         request,
                         request->requestData()),
                     boost::bind(&RpcRequester::startTimeoutTimer,
                                 this,
                                 request),
                     boost::bind(&RpcRequester::onSendFail,
                                 this,
                                 request));
    request->wait();
    request->cancelTimeoutTimer();
    rpcStats_->sentPacket(request->requestData()->ByteSize());

    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_.erase(requestGuid);
        MORDOR_LOG_TRACE(g_log) << this << " removed request (" <<
                                   requestGuid << ", " << *request <<
                                   ") from pending";
    }
    RpcRequest::Status status = request->status();
    MORDOR_ASSERT(status != RpcRequest::IN_PROGRESS);
    return request->status();
}

}  // namespace lightning
