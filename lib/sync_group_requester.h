#pragma once

#include <mordor/fibersynchronization.h>
#include <mordor/socket.h>
#include <string>
#include <vector>

namespace lightning {

//! Provides a generic way to synchronously execute a command on a group
//  of hosts within a specified timeout.
//
//  The command is sent to a specified multicast address and the requester
//  awaits acks from a specified group of hosts up to some timeout.
//  For this class, a command is simply a string.
//
//  Each host can either acknowledge the command or deny it. The logic of
//  parsing the reply and taking any further actions needed must be
//  implemented in an override of the onReply() method.
//  The only thing about the reply that matters to the abstract requester
//  is whether it is an ACK or a NACK. That must be indicated by onReply().
//
//  If even a single NACK is received, the command is assumed to have failed.
//  If not all ACK's are received within a given timeout, a user-supplied
//  onTimeout() is invoked and the command fails.
//
//  Important notes:
//  * This class is not concurrently callable. An instance may, in principle,
//    be used to execute a sequence of commands, but it is the responsibility
//    of the user to ensure that there are no concurrently running
//    doRequest()'s and there can ensue no confusion on the side that
//    is answering these requests.
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
class SyncGroupRequester 
{
public:
    SyncGroupRequester(Mordor::Socket::ptr socket,
                       Mordor::Address::ptr groupMulticastAddress,
                       const std::vector<Mordor::Address::ptr>& groupHosts,
                       uint64_t timeoutUs);

    virtual ~SyncGroupRequester()
    {}

protected:
    //! The actual requester should serialize its command and call
    //   doRequest().
    bool doRequest(const std::string& command);

private:
    //! Sets the multicast TTL to max.
    void setupSocket();

    struct AddressCompare {
        bool operator()(const Mordor::Address::ptr& lhs,
                        const Mordor::Address::ptr& rhs)
        {
            return *lhs < *rhs;
        }
    };

    //! Registers a reply with the reply.
    //  Returns true if it's an ACK and false if it's a
    //  NACK.
    virtual bool onReply(Mordor::Address::ptr source,
                         const std::string& reply) = 0;

    //! Signals that the request has timed out.
    virtual void onTimeout() = 0;

    static const size_t kMaxCommandSize = 8000;

    Mordor::Socket::ptr socket_;
    Mordor::Address::ptr groupMulticastAddress_;
    const std::vector<Mordor::Address::ptr> groupHosts_;
    const uint64_t timeoutUs_;
};

}  // namespace lightning
