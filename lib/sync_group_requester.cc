#include "sync_group_requester.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <mordor/timer.h>
#include <set>

namespace lightning {

using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Socket;
using Mordor::TimedOutException;
using Mordor::TimerManager;
using std::set;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:sync_group_requester");

SyncGroupRequester::SyncGroupRequester(Socket::ptr socket,
                                       Address::ptr groupMulticastAddress,
                                       const vector<Address::ptr>& groupHosts,
                                       uint64_t timeoutUs)
    : socket_(socket),
      groupMulticastAddress_(groupMulticastAddress),
      groupHosts_(groupHosts),
      timeoutUs_(timeoutUs)
{
    MORDOR_LOG_TRACE(g_log) << this << " init group='" <<
                            *groupMulticastAddress_ << "' #hosts=" <<
                            groupHosts_.size() << " timeout=" << timeoutUs_;
    setupSocket();
}

void SyncGroupRequester::setupSocket() {
    const int kMaxMulticastTtl = 255;
    socket_->setOption(IPPROTO_IP, IP_MULTICAST_TTL, kMaxMulticastTtl);
}

bool SyncGroupRequester::doRequest(const string& command) {
    MORDOR_LOG_TRACE(g_log) << this << " request = '" << command << '\'';
    set<Address::ptr, AddressCompare> notYetAcked(groupHosts_.begin(),
                                                  groupHosts_.end());

    MORDOR_ASSERT(command.length() < kMaxCommandSize)
    socket_->sendTo((const void*) command.c_str(),
                     command.length(),
                     0,
                     *groupMulticastAddress_);
    const uint64_t commandTimeoutTime = TimerManager::now() + timeoutUs_;

    try {
        Address::ptr currentSourceAddress = socket_->emptyAddress();
        set<Address::ptr, AddressCompare> notYetAcked(groupHosts_.begin(),
                                                      groupHosts_.end());
        socket_->enableReceive();
        while(true) {
            const uint64_t now = TimerManager::now();
            if(now >= commandTimeoutTime) {
                MORDOR_LOG_TRACE(g_log) << this << " command timed out";
                onTimeout();
                return false;
            }
            const uint64_t receiveTimeout = commandTimeoutTime - now;
            socket_->receiveTimeout(receiveTimeout);
            MORDOR_LOG_TRACE(g_log) << this << " waiting for replies, " <<
                                       receiveTimeout << "us left";
            char buffer[kMaxCommandSize + 1];
            ssize_t bytes = socket_->receiveFrom((void*) buffer,
                                                 sizeof(buffer) - 1,
                                                 *currentSourceAddress);
            buffer[bytes] = '\0';
            MORDOR_LOG_TRACE(g_log) << this << " got " << bytes << " bytes " <<
                                       "from " << *currentSourceAddress;
            bool isAck = onReply(currentSourceAddress, string(buffer, bytes));

            if(isAck) {
                notYetAcked.erase(currentSourceAddress);
                MORDOR_LOG_TRACE(g_log) << this << " ack from " <<
                                           *currentSourceAddress <<
                                           ", " << notYetAcked.size() <<
                                           " left";
                if(notYetAcked.empty()) {
                    MORDOR_LOG_TRACE(g_log) << this << " all acks received";
                    return true;
                }
            } else {
                MORDOR_LOG_TRACE(g_log) << this << " nack from " <<
                                           *currentSourceAddress;
                return false;
            }
        }
    } catch(TimedOutException& e) {
        MORDOR_LOG_TRACE(g_log) << " command timed out";
        return false;
    }

    MORDOR_NOTREACHED();
}

}  // namespace lightning
