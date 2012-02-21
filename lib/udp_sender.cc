#include "udp_sender.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::CountStatistic;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using Mordor::Statistics;

static Logger::ptr g_log = Log::lookup("lightning:udp_sender");

UdpSender::UdpSender(const std::string& name,
                     Socket::ptr socket)
    : name_(name),
      socket_(socket),
      outPackets_(Statistics::registerStatistic(name_ + ".out_packets",
                                                CountStatistic<uint64_t>())),
      outBytes_(Statistics::registerStatistic(name_ + ".out_bytes",
                                              CountStatistic<uint64_t>()))
{}

void UdpSender::run() {
    setupSocket();
    while(true) {
        auto request = queue_.pop();
        
        char buffer[kMaxDatagramSize];
        size_t commandSize = request.message->ByteSize();
        MORDOR_ASSERT(commandSize <= kMaxDatagramSize);
        if(!request.message->SerializeToArray(buffer, kMaxDatagramSize)) {
            MORDOR_LOG_WARNING(g_log) << name_ << " failed to serialize";
            if(request.onFail) {
                request.onFail();
            }
            continue;
        }
        socket_->sendTo((const void*)buffer,
                        commandSize,
                        0,
                        *request.destination);
        outPackets_.increment();
        outBytes_.add(commandSize);
        if(request.onSend) {
            request.onSend();
        }
    }
}

void UdpSender::send(const Address::ptr& destination,   
                     const boost::shared_ptr<const RpcMessageData>& message,
                     boost::function<void()> onSend,
                     boost::function<void()> onFail)
{
    queue_.push(PendingMessage(destination, message, onSend, onFail));
}

void UdpSender::setupSocket() {
    const int kMaxMulticastTtl = 255;
    socket_->setOption(IPPROTO_IP, IP_MULTICAST_TTL, kMaxMulticastTtl);
}

}  // namespace lightning
