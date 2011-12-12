#include "sync_group_requester.h"
#include "proto/sync_group_request.pb.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/timer.h>
#include <set>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Socket;
using Mordor::Timer;
using Mordor::TimerManager;
using std::set;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:sync_group_requester");

SyncGroupRequester::SyncGroupRequester(IOManager* ioManager,
                                       Socket::ptr socket,
                                       Address::ptr groupMulticastAddress,
                                       uint64_t timeoutUs)
    : ioManager_(ioManager),
      socket_(socket),
      groupMulticastAddress_(groupMulticastAddress),
      timeoutUs_(timeoutUs),
      currentRequestId_(0)
{
    MORDOR_LOG_TRACE(g_log) << this << " init group='" <<
                            *groupMulticastAddress_ <<  "' timeout=" << timeoutUs_;
    setupSocket();
}

void SyncGroupRequester::setupSocket() {
    const int kMaxMulticastTtl = 255;
    socket_->setOption(IPPROTO_IP, IP_MULTICAST_TTL, kMaxMulticastTtl);
}

void SyncGroupRequester::run() {
    MORDOR_LOG_TRACE(g_log) << this << " SyncGroupRequester::run()";
    Address::ptr currentSourceAddress = socket_->emptyAddress();
    while(true) {
        char buffer[kMaxCommandSize + 1];
        ssize_t bytes = socket_->receiveFrom((void*) buffer,
                                             kMaxCommandSize,
                                             *currentSourceAddress);
        SyncGroupRequestData reply;
        if(!reply.ParseFromArray(buffer, bytes)) {
            MORDOR_LOG_WARNING(g_log) << this << " failed to parse reply " <<
                                         "from " << *currentSourceAddress;
            continue;
        }

        const uint64_t id = reply.id();
        SyncGroupRequest::ptr request;
        {
            FiberMutex::ScopedLock lk(mutex_);
            auto requestIter = pendingRequests_.find(id);
            request = requestIter->second;
        }
        if(!request) {
            MORDOR_LOG_WARNING(g_log) << this << " stale reply: request " <<
                                         id << " not pending";
        }
        MORDOR_LOG_TRACE(g_log) << this << " got reply for request " <<
                                   id << " from " << *currentSourceAddress;
        request->onReply(currentSourceAddress, reply.data());
    }
}

void SyncGroupRequester::timeoutRequest(uint64_t requestId) {
    SyncGroupRequest::ptr request;
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
    MORDOR_LOG_TRACE(g_log) << this << " timing out request " << requestId;
    request->onTimeout();
}

SyncGroupRequest::Status SyncGroupRequester::request(
    SyncGroupRequest::ptr requestPtr)
{
    uint64_t requestId = ++currentRequestId_;
    MORDOR_LOG_TRACE(g_log) << this << " new request id=" << requestId <<
                               " request=" << requestPtr;
    SyncGroupRequestData requestData;
    requestData.set_id(requestId);
    requestData.set_data(requestPtr->requestString());

    char buffer[kMaxCommandSize];
    uint64_t commandSize = requestData.ByteSize();
    MORDOR_ASSERT(commandSize <= kMaxCommandSize);
    if(!requestData.SerializeToArray(buffer, kMaxCommandSize)) {
        MORDOR_LOG_WARNING(g_log) << this << " failed to serialize request " <<
                                     requestId;
        // TODO handle this better.
        return SyncGroupRequest::NACKED;
    }

    {
        FiberMutex::ScopedLock lk(mutex_);
        pendingRequests_[requestId] = requestPtr;
    }

    socket_->sendTo((const void*) buffer,
                    commandSize,
                    0,
                    *groupMulticastAddress_);
    Timer::ptr timeoutTimer =
        ioManager_->registerTimer(timeoutUs_,
                                  boost::bind(
                                      &SyncGroupRequester::timeoutRequest,
                                      this,
                                      requestId));
    requestPtr->wait();
    timeoutTimer->cancel();
    SyncGroupRequest::Status status = requestPtr->status();
    MORDOR_ASSERT(status != SyncGroupRequest::IN_PROGRESS);
    return requestPtr->status();
}

}  // namespace lightning
