#include "multicast_rpc_requester.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/assert.h>
#include <mordor/log.h>
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
using Mordor::Timer;
using Mordor::TimerManager;
using std::ostringstream;
using std::set;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:multicast_rpc_requester");

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

void MulticastRpcRequester::run() {
    MORDOR_LOG_TRACE(g_log) << this << " MulticastRpcRequester::run()";
    Address::ptr currentSourceAddress = socket_->emptyAddress();
    while(true) {
        char buffer[kMaxCommandSize + 1];
        ssize_t bytes = socket_->receiveFrom((void*) buffer,
                                             kMaxCommandSize,
                                             *currentSourceAddress);
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

MulticastRpcRequest::Status MulticastRpcRequester::request(
    MulticastRpcRequest::ptr requestPtr,
    uint64_t timeoutUs)
{
    Guid requestGuid = guidGenerator_->generate();
    MORDOR_LOG_TRACE(g_log) << this << " new request id=" << requestGuid <<
                               " request=" << requestPtr;
    RpcMessageData requestData;
    requestData.MergeFrom(requestPtr->request());
    requestGuid.serialize(requestData.mutable_uuid());


    char buffer[kMaxCommandSize];
    uint64_t commandSize = requestData.ByteSize();
    MORDOR_LOG_TRACE(g_log) << this << " command size is " << commandSize;
    MORDOR_ASSERT(commandSize <= kMaxCommandSize);
    if(!requestData.SerializeToArray(buffer, kMaxCommandSize)) {
        MORDOR_LOG_WARNING(g_log) << this << " failed to serialize request " <<
                                     requestGuid;
        // TODO handle this better.
        return MulticastRpcRequest::TIMED_OUT;
    }

    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_[requestGuid] = requestPtr;
    }

    socket_->sendTo((const void*) buffer,
                    commandSize,
                    0,
                    *groupMulticastAddress_);
    Timer::ptr timeoutTimer =
        ioManager_->registerTimer(timeoutUs,
                                  boost::bind(
                                      &MulticastRpcRequester::timeoutRequest,
                                      this,
                                      requestGuid));
    requestPtr->wait();
    timeoutTimer->cancel();
    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_.erase(requestGuid);
    }
    MulticastRpcRequest::Status status = requestPtr->status();
    MORDOR_ASSERT(status != MulticastRpcRequest::IN_PROGRESS);
    return requestPtr->status();
}

}  // namespace lightning
