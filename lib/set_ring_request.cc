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
    RingConfiguration::const_ptr ring,
    uint64_t timeoutUs)
    : ring_(ring),
      timeoutUs_(timeoutUs),
      notAckedMask_(ring->ringMask()),
      status_(IN_PROGRESS),
      event_(true)
{
    rpcMessageData_.set_type(RpcMessageData::SET_RING);
    hostGroupGuid.serialize(
        rpcMessageData_.mutable_set_ring()->mutable_group_guid());
    rpcMessageData_.mutable_set_ring()->set_ring_id(ring_->ringId());
    const vector<uint32_t>& ringHostIds = ring_->ringHostIds();
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
    FiberMutex::ScopedLock lk(mutex_);
    MORDOR_ASSERT(reply.type() == RpcMessageData::SET_RING);
    const uint32_t hostId = ring_->replyAddressToId(sourceAddress);

    if(hostId == GroupConfiguration::kInvalidHostId) {
        MORDOR_LOG_TRACE(g_log) << this << " reply from unknown address " <<
                                   *sourceAddress;
        return;
    }

    notAckedMask_ &= ~(1 << hostId);
    MORDOR_LOG_TRACE(g_log) << this << " " << *sourceAddress << "(" <<
                               hostId << ") acked ring_id=" <<
                               rpcMessageData_.set_ring().ring_id();
    if(notAckedMask_ == 0) {
        status_ = COMPLETED;
        event_.set();
    }
}

void SetRingRequest::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    const uint32_t ringId = rpcMessageData_.set_ring().ring_id();
    if(notAckedMask_ == 0) {
        MORDOR_LOG_TRACE(g_log) << this << " ring_id=" << ringId <<
                                   " onTimeout() with no pending acks";
        status_ = COMPLETED;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " set ring_id=" << ringId <<
                                   " timed out, notAckedMask=" <<
                                   notAckedMask_;
        status_ = TIMED_OUT;
    }
    event_.set();
}

uint64_t SetRingRequest::timeoutUs() const {
    return timeoutUs_;
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
