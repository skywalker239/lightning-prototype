#pragma once

#include "blocking_queue.h"
#include "guid.h"
#include "notifier.h"
#include "proposer_instance.h"
#include "proposer_state.h"
#include "value.h"
#include <mordor/fibersynchronization.h>
#include <set>

namespace lightning {

class ValueBuffer : public Notifier<paxos::ProposerInstance::ptr>,
                    public boost::enable_shared_from_this<ValueBuffer>
{
public:
    typedef boost::shared_ptr<ValueBuffer> ptr;

    ValueBuffer(size_t uncommittedLimit,
                ProposerState::ptr proposerState,
                BlockingQueue<paxos::Value>::ptr submitQueue);

    ~ValueBuffer();

    void pushValue(const paxos::Value& value);

    virtual void notify(const paxos::ProposerInstance::ptr& instance);
private:
    const size_t uncommittedLimit_;
    std::set<Guid> uncommittedValueIds_;
    ProposerState::ptr proposerState_;
    BlockingQueue<paxos::Value>::ptr submitQueue_;

    Mordor::FiberEvent canPush_;
    Mordor::FiberMutex mutex_;
};

}  // namespace lightning
