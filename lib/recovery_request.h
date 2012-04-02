#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include "unicast_rpc_request.h"
#include "value.h"

namespace lightning {

class RecoveryRequest : public UnicastRpcRequest {
public:
    typedef boost::shared_ptr<RecoveryRequest> ptr;

    RecoveryRequest(GroupConfiguration::ptr groupConfiguration,
                    uint32_t destinationHostId,
                    uint64_t timeoutUs,
                    const Guid& epoch,
                    paxos::InstanceId instanceId);

    enum Result {
        OK,
        NOT_COMMITTED,
        FORGOTTEN
    };

    Result result() const;

    const paxos::Value& value() const;

    paxos::BallotId ballot() const;

    bool value(paxos::Value* value, paxos::BallotId* ballot);
private:
    virtual void applyReply(const RpcMessageData& reply);

    virtual std::ostream& output(std::ostream& os) const;

    paxos::Value value_;
    paxos::BallotId ballotId_;
    Result result_;
};

}  // namespace lightning
