#include "set_ring_request.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <boost/lexical_cast.hpp>
#include <string>

namespace lightning {

using Mordor::Address;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:set_ring_request");

SetRingRequest::SetRingRequest(
    const Guid& hostGroupGuid,
    const vector<Address::ptr>& ringAddresses,
    const vector<uint32_t>& ringHostIds,
    uint32_t ringId)
    : notAcked_(ringAddresses.begin(), ringAddresses.end()),
      status_(IN_PROGRESS),
      event_(true)
{
    rpcMessageData_.set_type(RpcMessageData::SET_RING);
    hostGroupGuid.serialize(
        rpcMessageData_.mutable_set_ring()->mutable_group_guid());
    rpcMessageData_.mutable_set_ring()->set_ring_id(ringId);
    for(size_t i = 0; i < ringHostIds.size(); ++i) {
        rpcMessageData_.mutable_set_ring()->add_ring_host_ids(ringHostIds[i]);
    }
}

const RpcMessageData& SetRingRequest::request() const {
    return rpcMessageData_;
}

void SetRingRequest::onReply(Address::ptr sourceAddress,
                             const RpcMessageData& reply)
{
    MORDOR_ASSERT(reply.type() == RpcMessageData::SET_RING);
    {
        FiberMutex::ScopedLock lk(mutex_);
        notAcked_.erase(sourceAddress);
        if(notAcked_.empty()) {
            status_ = COMPLETED;
            event_.set();
        }
    }
    MORDOR_LOG_TRACE(g_log) << this << " " << *sourceAddress <<
                               " acked ring_id=" <<
                               rpcMessageData_.set_ring().ring_id();
}

void SetRingRequest::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    const uint32_t ringId = rpcMessageData_.set_ring().ring_id();
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " ring_id=" << ringId <<
                                   " onTimeout() with no pending acks";
        status_ = COMPLETED;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " set ring_id=" << ringId <<
                                   " timed out";
        status_ = TIMED_OUT;
    }
    event_.set();
}

void SetRingRequest::wait() {
    event_.wait();
    FiberMutex::ScopedLock lk(mutex_);
}

MulticastRpcRequest::Status SetRingRequest::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
