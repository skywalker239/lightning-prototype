#pragma once

#include "guid.h"
#include "host_configuration.h"
#include "rpc_request.h"
#include "udp_sender.h"
#include <mordor/atomic.h>
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <boost/noncopyable.hpp>
#include <map>
#include <utility>
#include <vector>

namespace lightning {

//! Provides a generic way to synchronously execute a command on a group
//  of hosts within a specified timeout.
//
//  Each request is assigned a guid and sent to a specified multicast
//  address.
//  The requester is continuously listening to new incoming packets
//  (which contain the request ids and replies) and applies them to the
//  commands which are in-progress (i.e. have not timed out yet).
//
//  Because of the way MulticastRpcRequest is designed, we do not need to
//  explicitly keep track of the host group we are awaiting acks from:
//  the MulticastRpcRequest signals the command completion by releasing
//  RpcRequester::request() from RpcRequest::wait().
//
//  The logic of parsing the reply and taking any further actions needed
//  must be implemented in an override of the RpcRequest::onReply()
//  method.
//
//  If needed replies are not received within a given timeout, a user-supplied
//  onTimeout() is invoked.
//
//  Important note:
//  * This synchronous request mechanism is even less safe that
//    two-phase commit. Both the requester and any responder may fail at any
//    moment, packets might get lost and reordered etc. Because of this, this
//    mechanism should only be used for purposes that cannot lead to any
//    inconsistencies because of fail-stop failures and message loss.
//    For example:
//      1. This mechanism is relatively safe to use to set a new ring for
//         Ring Paxos. Any message loss simply leads to the coordinator
//         considering the new ring to have failed.
//         Coordinator failure simply stalls the broadcast.
//      2. This mechanism can be safely used for executing Phase 1 of
//         the Paxos algorithm. Its safety in face of failures is guaranteed
//         by the properties of Paxos.
//      3. It is perhaps less obvious that this mechanism can also be used
//         to perform Phase 2 of Ring Paxos. In this case, all the acceptors
//         listen for Phase 2 messages on a single multicast address, but
//         the proposer only waits for one ack from a single host -- the
//         last acceptor in the current ring.
class RpcRequester : boost::noncopyable
{
public:
    typedef boost::shared_ptr<RpcRequester> ptr;

    RpcRequester(Mordor::IOManager* ioManager,
                          GuidGenerator::ptr guidGenerator,
                          UdpSender::ptr udpSender,
                          Mordor::Socket::ptr socket,
                          GroupConfiguration::ptr groupConfiguration);

    virtual ~RpcRequester()
    {}

    //! Collects reply datagrams.
    void processReplies();

    //! Sends pending requests.
    void sendRequests();

    //! Blocks until request is completed or until the timeout expires;
    RpcRequest::Status request(RpcRequest::ptr request);

private:
    //! Registers a timer that will time the request out.
    //  Called as an onSend callback by UdpSender.
    void startTimeoutTimer(RpcRequest::ptr request);

    //! onFail callback for UdpSender. Times out the request immediately.
    void onSendFail(RpcRequest::ptr request);

    //! Stop tracking a timed-out request.
    void timeoutRequest(const Guid& requestId);

    static const size_t kMaxDatagramSize = 8950;

    Mordor::IOManager* ioManager_;
    GuidGenerator::ptr guidGenerator_;
    UdpSender::ptr udpSender_;
    Mordor::Socket::ptr socket_;
    GroupConfiguration::ptr groupConfiguration_;

    BlockingQueue<RpcRequest::ptr> sendQueue_;

    mutable Mordor::FiberMutex mutex_;
    std::map<Guid, RpcRequest::ptr> pendingRequests_;
};

}  // namespace lightning
