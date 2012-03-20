#pragma once

#include "proposer_instance.h"
#include <mordor/fibersynchronization.h>
#include <mordor/statistics.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <queue>
#include <vector>

namespace lightning {
namespace paxos {

//! Stores pending Paxos instances for which neither phase 1
//  nor phase 2 is currently executing.
//
//  These instances can be open, which means that they are
//  immediately available for phase 2, or reserved, meaning
//  that a complete phase 1 has to be re-run for any such
//  instance.
//
//  The workflow is as follows: the batch phase 1 executor
//  produces new open and reserved instances and adds them
//  to the instance pool.
//  There are two thresholds, maxOpenInstancesNumber and
//  maxReservedInstancesNumber, which control the new instances
//  production via an Event shared between the phase 1 batcher
//  and the instance pool.
//  When there are more than maxOpenInstancesNumber open instances
//  or more than maxReservedInstancesNumber reserved ones, the
//  instance pool will reset the event to signal the phase 1 batcher
//  to pause.
//
class InstancePool : boost::noncopyable {
public:
    typedef boost::shared_ptr<InstancePool> ptr;

    InstancePool(uint32_t maxOpenInstancesNumber,
                 uint32_t maxReservedInstancesNumber,
                 boost::shared_ptr<Mordor::FiberEvent>
                    pushMoreOpenInstancesEvent);

    //! Pushes an open instance (back) into the pool.
    //  Does not block.
    void pushOpenInstance(ProposerInstance::ptr instance);

    //! Pops the open instance with the lowest instance id.
    //  Blocks if there's none.
    ProposerInstance::ptr popOpenInstance();

    //! Pushes a reserved (with phase 1 to be done) instance
    //  into the pool.
    //  Does not block.
    void pushReservedInstance(ProposerInstance::ptr instance);

    //! Pops the reserved instance with the lowest instance id.
    //  Blocks if there's currently none.
    ProposerInstance::ptr popReservedInstance();
private:
    const uint32_t maxOpenInstancesNumber_;
    const uint32_t maxReservedInstancesNumber_;
    
    //! We want a min-heap on instances.
    struct InstancePtrCompare {
        bool operator()(const ProposerInstance::ptr& a,
                        const ProposerInstance::ptr& b) const
        {
            return !(*a < *b);
        }
    };

    typedef std::priority_queue<
                ProposerInstance::ptr,
                std::vector<ProposerInstance::ptr>,
                InstancePtrCompare>
            InstanceHeap;

    InstanceHeap openInstances_;
    InstanceHeap reservedInstances_;

    boost::shared_ptr<Mordor::FiberEvent> pushMoreOpenInstancesEvent_;
    Mordor::FiberEvent canPopOpenInstancesEvent_;
    Mordor::FiberSemaphore openInstancesNotEmpty_;
    Mordor::FiberSemaphore reservedInstancesNotEmpty_;
    Mordor::FiberMutex mutex_;
};

}  // namespace paxos
}  // namespace lightning
