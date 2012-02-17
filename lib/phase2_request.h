#pragma once

#include "guid.h"
#include "multicast_rpc_request.h"
#include "paxos_defs.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <map>
#include <set>

namespace lightning {

class Phase2Request : public MulticastRpcRequest {
public:
    typedef boost::shared_ptr<Phase2Request> ptr;

    //! Here the ring parameter is actually a surrogate ring
    //  containing only the last host of the current ring.
    Phase2Request(const Guid& epoch,
                  uint32_t ringId,
                  paxos::InstanceId instance,
                  paxos::BallotId ballot,
                  paxos::Value::ptr value,
                  const std::vector<std::pair<paxos::InstanceId, Guid> >&
                      commits,
                  RingConfiguration::const_ptr ring,
                  uint64_t timeoutUs);

    enum Result {
        PENDING,
        SUCCESS
    };

    Result result() const;

private:
    const RpcMessageData& request() const;

    std::ostream& output(std::ostream& os) const;

    void applyReply(uint32_t hostId,
                    const RpcMessageData& reply);

    void serializeValue(paxos::Value::ptr value, ValueData* valueData) const;

    void serializeCommits(
        const std::vector<std::pair<paxos::InstanceId, Guid> >& commits,
        PaxosPhase2RequestData *request) const;

    RpcMessageData requestData_;
    const Guid valueId_;
    const GroupConfiguration::ptr& group_;
    Result result_;
};

} // namespace lightning
