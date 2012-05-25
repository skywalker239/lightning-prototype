#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include <boost/shared_ptr.hpp>
#include <iostream>

namespace lightning {

class RecoveryRecord {
public:
    typedef boost::shared_ptr<RecoveryRecord> ptr;

    RecoveryRecord(const Guid& epoch,
                   paxos::InstanceId instanceId);

    const Guid& epoch() const { return epoch_; }

    paxos::InstanceId instanceId() const { return instanceId_; }
private:
    const Guid epoch_;
    const paxos::InstanceId instanceId_;

    friend std::ostream& operator<<(std::ostream& os,
                                    const RecoveryRecord& r);
};

inline
std::ostream& operator<<(std::ostream& os,
                         const RecoveryRecord& r)
{
    os << "RecoveryRecord(" << r.epoch_ << ", " << r.instanceId_ << ")";
    return os;
}

}  // namespace lightning
