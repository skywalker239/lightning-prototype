#include "instance_pool.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {
namespace paxos {

using boost::shared_ptr;
using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using std::priority_queue;

static Logger::ptr g_log = Log::lookup("lightning:instance_pool");

InstancePool::InstancePool(uint32_t maxOpenInstancesNumber,
                           uint32_t maxReservedInstancesNumber,
                           shared_ptr<FiberEvent> pushMoreOpenInstancesEvent)
    : maxOpenInstancesNumber_(maxOpenInstancesNumber),
      maxReservedInstancesNumber_(maxReservedInstancesNumber),
      pushMoreOpenInstancesEvent_(pushMoreOpenInstancesEvent)
{}

void InstancePool::pushOpenInstance(ProposerInstance::ptr instance) {
    MORDOR_ASSERT(instance->state() == ProposerInstance::P1_OPEN);
    MORDOR_LOG_TRACE(g_log) << this << " pushing open instance " <<
                               instance->instanceId();
    FiberMutex::ScopedLock lk(mutex_);
    openInstances_.push(instance);
    openInstancesNotEmpty_.notify();
    if(openInstances_.size() > maxOpenInstancesNumber_) {
        MORDOR_LOG_TRACE(g_log) << this << " maxOpenInstancesNumber threshold reached";
        pushMoreOpenInstancesEvent_->reset();
    }
}

ProposerInstance::ptr InstancePool::popOpenInstance() {
    openInstancesNotEmpty_.wait();
    FiberMutex::ScopedLock lk(mutex_);
    ProposerInstance::ptr instance = openInstances_.top();
    openInstances_.pop();
    MORDOR_LOG_TRACE(g_log) << this << " popped open instance " <<
                               instance->instanceId();
    if(openInstances_.size() <= maxOpenInstancesNumber_ &&
       reservedInstances_.size() <= maxReservedInstancesNumber_)
    {
        MORDOR_LOG_TRACE(g_log) << this << " signaling to push more open instances";
        pushMoreOpenInstancesEvent_->set();
    }
    return instance;
}

void InstancePool::pushReservedInstance(ProposerInstance::ptr instance) {
    MORDOR_ASSERT(instance->state() == ProposerInstance::P1_PENDING);
    MORDOR_LOG_TRACE(g_log) << this << " pushing reserved instance " <<
                               instance->instanceId();
    FiberMutex::ScopedLock lk(mutex_);
    reservedInstances_.push(instance);
    reservedInstancesNotEmpty_.notify();
    if(reservedInstances_.size() > maxReservedInstancesNumber_) {
        MORDOR_LOG_TRACE(g_log) << this << " maxReservedInstancesNumber threshold " <<
                                   "reached";
        pushMoreOpenInstancesEvent_->reset();
    }
}

ProposerInstance::ptr InstancePool::popReservedInstance() {
    reservedInstancesNotEmpty_.wait();
    FiberMutex::ScopedLock lk(mutex_);
    ProposerInstance::ptr instance = reservedInstances_.top();
    reservedInstances_.pop();
    MORDOR_LOG_TRACE(g_log) << this << " popped reserved instance " <<
                               instance->instanceId();
    if(openInstances_.size() <= maxOpenInstancesNumber_ &&
       reservedInstances_.size() <= maxReservedInstancesNumber_)
    {
        MORDOR_LOG_TRACE(g_log) << this << " signaling to push more open instances";
        pushMoreOpenInstancesEvent_->set();
    }
    return instance;
}

}  // namespace paxos
}  // namespace lightning
