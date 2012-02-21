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
    : MulticastRpcRequest(ring, timeoutUs),
      ring_(ring)
{
    requestData_.set_type(RpcMessageData::SET_RING);
    hostGroupGuid.serialize(
        requestData_.mutable_set_ring()->mutable_group_guid());
    requestData_.mutable_set_ring()->set_ring_id(ring_->ringId());
    const vector<uint32_t>& ringHostIds = ring_->ringHostIds();
    for(size_t i = 0; i < ringHostIds.size(); ++i) {
        requestData_.mutable_set_ring()->add_ring_host_ids(ringHostIds[i]);
    }
}

std::ostream& SetRingRequest::output(std::ostream& os) const {
    os << "SetRing(" << *ring_ << ")";
    return os;
}

void SetRingRequest::applyReply(uint32_t hostId,
                                const RpcMessageData& /* reply */)
{
    MORDOR_LOG_TRACE(g_log) << this << " " << ring_->group()->host(hostId) <<
                               " acked " << *ring_;
}

}  // namespace lightning
