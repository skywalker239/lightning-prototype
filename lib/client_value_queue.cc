#include "client_value_queue.h"
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::CountStatistic;
using Mordor::FiberMutex;
using Mordor::FiberEvent;
using Mordor::Logger;
using Mordor::Log;
using Mordor::Statistics;
using paxos::Value;

static Logger::ptr g_log = Log::lookup("lightning:client_value_queue");

static CountStatistic<uint64_t>& g_pushes =
    Statistics::registerStatistic("client_value_queue.pushes",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_pops =
    Statistics::registerStatistic("client_value_queue.pops",
                                  CountStatistic<uint64_t>());
static CountStatistic<uint64_t>& g_unpops =
    Statistics::registerStatistic("client_value_queue.unpops",
                                  CountStatistic<uint64_t>());

ClientValueQueue::ClientValueQueue()
    : event_(false)
{}

boost::shared_ptr<Value> ClientValueQueue::pop() {
    while(true) {
        event_.wait();
        FiberMutex::ScopedLock lk(mutex_);
        if(values_.empty()) {
            continue;
        }
        boost::shared_ptr<Value> value = *values_.begin();
        values_.pop_front();
        MORDOR_LOG_TRACE(g_log) << this << " pop " << value->valueId;
        g_pops.increment();
        if(values_.empty()) {
            event_.reset();
        }
        return value;
    }
}

void ClientValueQueue::push(boost::shared_ptr<Value> value) {
    FiberMutex::ScopedLock lk(mutex_);
    values_.push_back(value);
    g_pushes.increment();
    event_.set();
    MORDOR_LOG_TRACE(g_log) << this << " push " << value->valueId;
}

void ClientValueQueue::push_front(boost::shared_ptr<Value> value) {
    FiberMutex::ScopedLock lk(mutex_);
    values_.push_front(value);
    g_unpops.increment();
    event_.set();
    MORDOR_LOG_TRACE(g_log) << this << " push_front " << value->valueId;
}

}  // namespace lightning
