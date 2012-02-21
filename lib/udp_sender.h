#pragma once

#include "blocking_queue.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/socket.h>
#include <mordor/statistics.h>
#include <boost/bind.hpp>
#include <string>
#include <utility>

namespace lightning {

//! A udp sender which manages a queue of outgoing RPC messages
//  and sends them in a single event loop to avoid collisions
//  (high send rate from several fibers can lead to an attempt to
//  simultaneously wait on the socket, which will assert inside
//  the IOManager.
//  May be made throttled in the future.
class UdpSender {
public:
    typedef boost::shared_ptr<UdpSender> ptr;
    //! Name is used for tracking statistics specific to this sender.
    //  It MUST be unique or Mordor will assert on registering
    //  the statistics.
    //  Socket MUST NOT be used for sending packets by anyone else.
    UdpSender(const std::string& name,
              Mordor::Socket::ptr socket);

    //! Sends the enqueued packets.
    void run();

    //! Enqueues a message.
    //  onSend is called upon returning from sendTo,
    //  onFail is called upon failure to serialize message.
    void send(const Mordor::Address::ptr& destination,
              const boost::shared_ptr<const RpcMessageData>& message,
              boost::function<void()> onSend = NULL,
              boost::function<void()> onFail = NULL);
private:
    //! Sets the multicast TTL on the socket to max (255).
    void setupSocket();

    static const size_t kMaxDatagramSize = 8900;

    struct PendingMessage {
        Mordor::Address::ptr destination;
        boost::shared_ptr<const RpcMessageData> message;
        boost::function<void()> onSend;
        boost::function<void()> onFail;

        PendingMessage(Mordor::Address::ptr _destination,
                       boost::shared_ptr<const RpcMessageData> _message,
                       boost::function<void()> _onSend,
                       boost::function<void()> _onFail)
            : destination(_destination),
              message(_message),
              onSend(_onSend),
              onFail(_onFail)
        {}
    };
    
    const std::string name_;
    Mordor::Socket::ptr socket_;
    BlockingQueue<PendingMessage> queue_;
    Mordor::CountStatistic<uint64_t>& outPackets_;
    Mordor::CountStatistic<uint64_t>& outBytes_;
};

}  // namespace lightning
