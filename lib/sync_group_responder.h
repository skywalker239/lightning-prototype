#pragma once

#include <mordor/socket.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <string>

namespace lightning {

//! Listens to incoming requests from some SyncGroupRequester and
//  responds to them.
class SyncGroupResponder : boost::noncopyable {
public:
    typedef boost::shared_ptr<SyncGroupResponder> ptr;

    //! Joins listenSocket to multicastGroup,
    //  sends replies via replySocket.
    SyncGroupResponder(Mordor::Socket::ptr listenSocket,
                       Mordor::Address::ptr multicastGroup,
                       Mordor::Socket::ptr replySocket);
    
    virtual ~SyncGroupResponder() {}

    //! Processes requests one by one, calling onRequest() synchronously.
    void run();

    //! We limit ourselves to datagrams not larger than this.
    static const size_t kMaxDatagramSize = 8900;
private:
    //! Return false to ignore the request.
    virtual bool onRequest(Mordor::Address::ptr source,
                           const std::string& request,
                           std::string* reply) = 0;

    Mordor::Socket::ptr listenSocket_;
    Mordor::Address::ptr multicastGroup_;
    Mordor::Socket::ptr replySocket_;
};

}  // namespace lightning
