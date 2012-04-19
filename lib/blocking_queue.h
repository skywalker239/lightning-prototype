#pragma once

#include <mordor/fibersynchronization.h>
#include <mordor/log.h>
#include <mordor/statistics.h>
#include <deque>

namespace lightning {

template<typename T>
class BlockingQueue {
public:
    typedef boost::shared_ptr<BlockingQueue<T> > ptr;

    BlockingQueue(const std::string& name)
        : event_(false),
          logger_(Mordor::Log::lookup("lightning:blocking_queue:" + name)),
          pushes_(Mordor::Statistics::registerStatistic(
                  std::string("blocking_queue.") + name + ".pushes",
                  Mordor::CountStatistic<uint64_t>())),
          pops_(Mordor::Statistics::registerStatistic(
                std::string("blocking_queue.") + name + ".pops",
                Mordor::CountStatistic<uint64_t>())),
          unpops_(Mordor::Statistics::registerStatistic(
                  std::string("blocking_queue.") + name + ".unpops",
                  Mordor::CountStatistic<uint64_t>()))
    {
    }

    //! Blocks until a value is available.
    T pop() {
        while(true) {
            event_.wait();
            Mordor::FiberMutex::ScopedLock lk(mutex_);
            if(values_.empty()) {
                continue;
            }
            T value = *values_.begin();
            values_.pop_front();
            pops_.increment();
            MORDOR_LOG_TRACE(logger_) << this << " pop " << value;
            if(values_.empty()) {
                event_.reset();
            }
            return value;
        }
    }

    //! Never blocks.
    void push(T value) {
        Mordor::FiberMutex::ScopedLock lk(mutex_);
        pushes_.increment();
        values_.push_back(value);
        event_.set();
        MORDOR_LOG_TRACE(logger_) << this << " push " << value;
    }

    //! Never blocks
    void push_front(T value) {
        Mordor::FiberMutex::ScopedLock lk(mutex_);
        values_.push_front(value);
        unpops_.increment();
        event_.set();
        MORDOR_LOG_TRACE(logger_) << this << " push_front " << value;
    }
private:
    std::deque<T> values_;
    Mordor::FiberMutex mutex_;
    Mordor::FiberEvent event_;
    Mordor::Logger::ptr logger_;
    Mordor::CountStatistic<uint64_t>& pushes_;
    Mordor::CountStatistic<uint64_t>& pops_;
    Mordor::CountStatistic<uint64_t>& unpops_;
};

}  // namespace lightning
