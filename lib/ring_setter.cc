#include "ring_setter.h"
#include "proto/set_ring.pb.h"
#include <mordor/log.h>
#include <boost/lexical_cast.hpp>
#include <string>

namespace lightning {

using boost::lexical_cast;
using Mordor::Address;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:ring_setter");

RingSetter::RingSetter(Socket::ptr socket,
                       Address::ptr ringMulticastAddress,
                       const vector<Address::ptr>& ringHosts,
                       uint64_t ringId,
                       uint64_t timeoutUs)
    : SyncGroupRequester(socket,
                         ringMulticastAddress,
                         ringHosts,
                         timeoutUs),
      ringHosts_(ringHosts),
      ringId_(ringId)
{}

bool RingSetter::setRing() {
    SetRingData setRingData;
    MORDOR_LOG_TRACE(g_log) << this << " trying to set ring id=" <<
                               ringId_;
    setRingData.set_ring_id(ringId_);
    for(size_t i = 0; i < ringHosts_.size(); ++i) {
        const string host = lexical_cast<string>(*ringHosts_[i]);
        MORDOR_LOG_TRACE(g_log) << this << " [" << ringId_ << ":" <<
                                   i << "] " << host;
        setRingData.add_ring_hosts(host);
    }

    string commandString;
    setRingData.SerializeToString(&commandString);
    return doRequest(commandString);
}

bool RingSetter::onReply(Address::ptr sourceAddress,
                         const string& reply)
{
    SetRingAckData setRingAckData;
    setRingAckData.ParseFromString(reply);
    MORDOR_LOG_TRACE(g_log) << this << " ring ack id=" <<
                               setRingAckData.ring_id() << " from " <<
                               *sourceAddress;
    return true;
}

void RingSetter::onTimeout() {
    MORDOR_LOG_TRACE(g_log) << this << " set ring id=" << ringId_ <<
                               " timed out";
}

}  // namespace lightning
