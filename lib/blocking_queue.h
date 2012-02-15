#pragma once

#include <mordor/fibersynchronization.h>
#include <deque>

namespace lightning {

template<typename T>
class BlockingQueue {
public:
    typedef boost::shared_ptr<BlockingQueue<T> > ptr;

    BlockingQueue()
        : event_(false)
    {}

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
            if(values_.empty()) {
                event_.reset();
            }
            return value;
        }
    }

    //! Never blocks.
    void push(T value) {
        Mordor::FiberMutex::ScopedLock lk(mutex_);
        values_.push_back(value);
        event_.set();
    }

    //! Never blocks
    void push_front(T value) {
        Mordor::FiberMutex::ScopedLock lk(mutex_);
        values_.push_front(value);
        event_.set();
    }
private:
    std::deque<T> values_;
    Mordor::FiberMutex mutex_;
    Mordor::FiberEvent event_;
};

}  // namespace lightning
