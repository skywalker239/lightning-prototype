#include "value_buffer.h"
#include "blocking_queue.h"
#include <mordor/log.h>

namespace lightning {

using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using paxos::ProposerInstance;
using paxos::Value;

static Logger::ptr g_log = Log::lookup("lightning:value_buffer");

ValueBuffer::ValueBuffer(size_t uncommittedLimit,
                         ProposerState::ptr proposerState,
                         BlockingQueue<Value>::ptr submitQueue)
    : uncommittedLimit_(uncommittedLimit),
      proposerState_(proposerState),
      submitQueue_(submitQueue),
      canPush_(false)
{
    proposerState->addNotifier(this);
    canPush_.set();
}

ValueBuffer::~ValueBuffer() {
    proposerState_->removeNotifier(this);
}

void ValueBuffer::pushValue(const paxos::Value& value) {
    MORDOR_LOG_TRACE(g_log) << this << " push(" << value.valueId() <<
        ") waiting";
    canPush_.wait();
    MORDOR_LOG_TRACE(g_log) << this << " push(" << value.valueId() << ")";
    
    FiberMutex::ScopedLock lk(mutex_);
    uncommittedValueIds_.insert(value.valueId());
    if(uncommittedValueIds_.size() >= uncommittedLimit_) {
        MORDOR_LOG_TRACE(g_log) << this << " buffer is full";
        canPush_.reset();
    }
    submitQueue_->push(value);
}

void ValueBuffer::notify(const ProposerInstance::ptr& instance) {
    FiberMutex::ScopedLock lk(mutex_);
    MORDOR_LOG_TRACE(g_log) << this << " notify(" <<
        instance->value().valueId() << ")";
    uncommittedValueIds_.erase(instance->value().valueId());
    if(uncommittedValueIds_.size() + 1 == uncommittedLimit_) {
        MORDOR_LOG_TRACE(g_log) << this << " buffer no longer full";
        canPush_.set();
    }
}   

}  // namespace lightning
