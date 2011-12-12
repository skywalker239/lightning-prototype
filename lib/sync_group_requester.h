#pragma once

#include "sync_group_request.h"
#include <mordor/atomic.h>
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <boost/noncopyable.hpp>
#include <map>
#include <vector>

namespace lightning {

//! Provides a generic way to synchronously execute a command on a group
//  of hosts within a specified timeout.
//
//  Each request is assigned a unique id and sent to a specified multicast
//  address.
//  The requester is continuously listening to new incoming packets
//  (which contain the request ids and replies) and applies them to the
//  commands which are in-progress (i.e. have not timed out yet).
//
//  Because of the way SyncGroupRequest is designed, we do not need to
//  explicitly keep track of the host group we are awaiting acks from:
//  the SyncGroupRequest signals the command completion by releasing
//  SyncGroupRequester::request() from SyncGroupRequest::wait().
//
//  Each host can either acknowledge the command or deny it. The logic of
//  parsing the reply and taking any further actions needed must be
//  implemented in an override of the SyncGroupRequest::onReply() method.
//
//  If even a single NACK is received, the command is assumed to have failed.
//  If not all ACK's are received within a given timeout, a user-supplied
//  onTimeout() is invoked and the command fails.
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
class SyncGroupRequester : boost::noncopyable
{
public:
    typedef boost::shared_ptr<SyncGroupRequester> ptr;

    SyncGroupRequester(Mordor::IOManager* ioManager,
                       Mordor::Socket::ptr socket,
                       Mordor::Address::ptr groupMulticastAddress,
                       uint64_t timeoutUs);

    virtual ~SyncGroupRequester()
    {}

    //! Collects reply datagrams.
    void run();

    SyncGroupRequest::Status request(SyncGroupRequest::ptr request);

private:
    //! Sets the multicast TTL to max.
    void setupSocket();

    //! Stop tracking a timed-out request.
    void timeoutRequest(uint64_t requestId);

    static const size_t kMaxCommandSize = 8000;

    Mordor::IOManager* ioManager_;
    Mordor::Socket::ptr socket_;
    Mordor::Address::ptr groupMulticastAddress_;
    const uint64_t timeoutUs_;

    mutable Mordor::FiberMutex mutex_;
    Mordor::Atomic<uint64_t> currentRequestId_;
    std::map<uint64_t, SyncGroupRequest::ptr> pendingRequests_;
};

}  // namespace lightning
