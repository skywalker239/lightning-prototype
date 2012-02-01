#pragma once

#include "value.h"
#include <mordor/fibersynchronization.h>
#include <deque>

namespace lightning {

class ClientValueQueue {
public:
    typedef paxos::Value Value;

    typedef boost::shared_ptr<ClientValueQueue> ptr;

    ClientValueQueue();

    //! Blocks until a value is available.
    boost::shared_ptr<Value> pop();

    //! Never blocks.
    void push(boost::shared_ptr<Value> value);

    //! Never blocks
    void push_front(boost::shared_ptr<Value> value);
private:
    std::deque<boost::shared_ptr<Value> > values_;
    Mordor::FiberMutex mutex_;
    Mordor::FiberEvent event_;
};

}  // namespace lightning
