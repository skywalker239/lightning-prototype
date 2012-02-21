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

static CountStatistic<uint64_t>& g_inPackets =
    Statistics::registerStatistic("multicast_rpc_requester.in_packets",
                                  CountStatistic<uint64_t>("packets"));
static CountStatistic<uint64_t>& g_inBytes =
    Statistics::registerStatistic("multicast_rpc_requester.in_bytes",
                                  CountStatistic<uint64_t>("bytes"));

MulticastRpcRequester::MulticastRpcRequester(
    IOManager* ioManager,
    GuidGenerator::ptr guidGenerator,
    UdpSender::ptr udpSender,
    Socket::ptr socket,
    Address::ptr groupMulticastAddress,
    GroupConfiguration::ptr groupConfiguration)
    : ioManager_(ioManager),
      guidGenerator_(guidGenerator),
      udpSender_(udpSender),
      socket_(socket),
      groupMulticastAddress_(groupMulticastAddress),
      groupConfiguration_(groupConfiguration)
{
    MORDOR_LOG_TRACE(g_log) << this << " init group='" <<
                            *groupMulticastAddress_;
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
        MORDOR_LOG_TRACE(g_log) << this << " got reply for request (" <<
                                   replyGuid << ", " << *request << ") from " <<
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
        MORDOR_LOG_TRACE(g_log) << this << " timed out request " <<
                                   requestId << " = " <<
                                   *request;
        pendingRequests_.erase(requestIter);
    }
    request->onTimeout();
}

void MulticastRpcRequester::startTimeoutTimer(
    MulticastRpcRequest::ptr request)
{
    MORDOR_LOG_TRACE(g_log) << this << " request (" <<
                               request->rpcGuid() << ", " << *request <<
                               ") sent";
    Timer::ptr timeoutTimer = ioManager_->registerTimer(request->timeoutUs(),
                                  boost::bind(
                                  &MulticastRpcRequester::timeoutRequest,
                                  this,
                                  request->rpcGuid()));
    request->setTimeoutTimer(timeoutTimer);
}

void MulticastRpcRequester::onSendFail(MulticastRpcRequest::ptr request) {
    MORDOR_LOG_TRACE(g_log) << this << " failed to send " << *request;
    request->onTimeout();
}

MulticastRpcRequest::Status MulticastRpcRequester::request(
    MulticastRpcRequest::ptr request)
{
    Guid requestGuid = guidGenerator_->generate();
    request->setRpcGuid(requestGuid);

    MORDOR_LOG_TRACE(g_log) << this << " new request (" << requestGuid <<
                               ", " << *request << ")";
    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_[requestGuid] = request;
    }

    udpSender_->send(groupMulticastAddress_,
                     boost::shared_ptr<const RpcMessageData>(
                         request,
                         request->requestData()),
                     boost::bind(&MulticastRpcRequester::startTimeoutTimer,
                                 this,
                                 request),
                     boost::bind(&MulticastRpcRequester::onSendFail,
                                 this,
                                 request));
    request->wait();
    request->cancelTimeoutTimer();
    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_.erase(requestGuid);
        MORDOR_LOG_TRACE(g_log) << this << " removed request (" <<
                                   requestGuid << ", " << *request <<
                                   ") from pending";
    }
    MulticastRpcRequest::Status status = request->status();
    MORDOR_ASSERT(status != MulticastRpcRequest::IN_PROGRESS);
    return request->status();
}

}  // namespace lightning
