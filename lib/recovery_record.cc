#include "recovery_record.h"
#include <algorithm>

namespace lightning {

using paxos::InstanceId;
using std::min;

RecoveryRecord::RecoveryRecord(const Guid& epoch,
                               InstanceId instanceId)
    : epoch_(epoch),
      instanceId_(instanceId)
{}

}  // namespace lightning
