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
    
    Phase2Request(const Guid& epoch,
                  uint32_t ringId,
                  paxos::InstanceId instance,
                  paxos::BallotId ballot,
                  paxos::Value::ptr value,
                  const std::vector<std::pair<paxos::InstanceId, Guid> >&
                      commits,
                  Mordor::Address::ptr lastRingHost,
                  uint64_t timeoutUs);

    enum Result {
        PENDING,
        SUCCESS
    };

    Result result() const;

    Status status() const;

private:
    const RpcMessageData& request() const;

    void onReply(Mordor::Address::ptr source,
                 const RpcMessageData& reply);

    void onTimeout();

    void wait();

    uint64_t timeoutUs() const;

    void serializeValue(paxos::Value::ptr value, ValueData* valueData) const;

    void serializeCommits(
        const std::vector<std::pair<paxos::InstanceId, Guid> >& commits,
        PaxosPhase2RequestData *request) const;

    RpcMessageData requestData_;
    const Guid valueId_;
    Mordor::Address::ptr lastRingHost_;
    const uint64_t timeoutUs_;
    Status status_;
    Result result_;

    Mordor::FiberEvent event_;
    mutable Mordor::FiberMutex mutex_;
};

} // namespace lightning
