#include "set_ring_handler.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::Address;
using Mordor::Logger;
using Mordor::Log;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:set_ring_handler");

SetRingHandler::SetRingHandler(const Guid& hostGroupGuid,
                               RingChangeNotifier::ptr ringChangeNotifier,
                               GroupConfiguration::ptr groupConfiguration)
    : hostGroupGuid_(hostGroupGuid),
      ringChangeNotifier_(ringChangeNotifier),
      groupConfiguration_(groupConfiguration)
{
    // XXX fixed master
    const uint32_t masterId = groupConfiguration_->masterId();
    masterAddress_ =
        groupConfiguration_->host(masterId).multicastSourceAddress;
}

bool SetRingHandler::handleRequest(Address::ptr sourceAddress,
                               const RpcMessageData& request,
                               RpcMessageData* reply)
{
    if(*sourceAddress != *masterAddress_) {
        MORDOR_LOG_WARNING(g_log) << this << " source address " <<
                                     *sourceAddress << " doesn't match " <<
                                     "master address " << *masterAddress_;
        return false;
    }
    Guid requestHostGroupGuid = Guid::parse(request.set_ring().group_guid());
    if(requestHostGroupGuid != hostGroupGuid_) {
        MORDOR_LOG_WARNING(g_log) << this << " request group guid " <<
                                     requestHostGroupGuid <<
                                     " doesn't match " <<
                                     hostGroupGuid_;
        return false;
    }


    reply->set_type(RpcMessageData::SET_RING);
    const uint32_t ringId = request.set_ring().ring_id();
    vector<uint32_t> ringHosts(request.set_ring().ring_host_ids().begin(),
                               request.set_ring().ring_host_ids().end());

    MORDOR_LOG_TRACE(g_log) << this << " got set ring id=" << ringId <<
                               " with " << ringHosts.size() << " hosts";
    RingConfiguration::ptr ringConfiguration(
        new RingConfiguration(groupConfiguration_, ringHosts, ringId));
    ringChangeNotifier_->onRingChange(ringConfiguration);

    return true;
}

}  // namespace lightning
