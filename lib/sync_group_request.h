#pragma once

#include <mordor/socket.h>
#include <boost/shared_ptr.hpp>
#include <string>

namespace lightning {

//! An abstract command to be executed synchronously on several hosts.
//  See SyncGroupRequester for the description of its semantics.
//  An implementation must be fiber-safe since onReply and
//  onTimeout may in principle be called concurrently (and will be
//  certainly called concurrently with wait()).
class SyncGroupRequest {
public:
    typedef boost::shared_ptr<SyncGroupRequest> ptr;

    enum Status {
        IN_PROGRESS,
        NACKED,
        TIMED_OUT,
        OK
    };

    virtual ~SyncGroupRequest() {}

    //! The serialized request to transmit over the network.
    virtual const std::string requestString() const = 0;

    //! Registers a reply from a certain address.
    //  If it's the last needed ack or a NACK, releases
    //  the wait()'ers.
    virtual void onReply(Mordor::Address::ptr sourceAddress,
                         const std::string& reply) = 0;

    //! Registers that a timeout occurred.
    //  The implementation should allow this to be called
    //  even after all the necessary acks have been received.
    virtual void onTimeout() = 0;

    //! Should block the calling fiber until all necessary
    //  acks have been collected or a nack/timeout happened.
    virtual void wait() = 0;

    //! Current status. 
    virtual Status status() const = 0;
};

}  // namespace lightning
