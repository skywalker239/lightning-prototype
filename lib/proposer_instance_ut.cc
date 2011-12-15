#include "proposer_instance.h"
#include <mordor/test/test.h>

using boost::shared_ptr;
using namespace Mordor;
using namespace lightning::paxos;

std::ostream& operator<<(std::ostream& os,
                         const ProposerInstance::State& state)
{
    return os << uint32_t(state);
}

MORDOR_UNITTEST(ProposerInstanceTest, EmptyInstance) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::EMPTY);
    MORDOR_TEST_ASSERT_EQUAL(instance.instanceId(), kInstanceId);
}

MORDOR_UNITTEST(ProposerInstanceTest, EmptyP1Open) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Open(kBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P1_OPEN);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
}

MORDOR_UNITTEST(ProposerInstanceTest, EmptyP1Pending) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P1_PENDING);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
}

MORDOR_UNITTEST(ProposerInstanceTest, P1PendingP1Open) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);
    instance.phase1Open(kBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P1_OPEN);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
}

MORDOR_UNITTEST(ProposerInstanceTest, P1PendingP1OpenBad) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Open(kBallotId + 1));
}

MORDOR_UNITTEST(ProposerInstanceTest, P1PendingP2Pending) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);
    shared_ptr<Value> value(new Value);
    instance.phase2Pending(value);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P2_PENDING);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
}

MORDOR_UNITTEST(ProposerInstanceTest, P1PendingRetry) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);

    const BallotId kNextBallotId = kBallotId + 2;
    instance.phase1Retry(kNextBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P1_PENDING);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kNextBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
}

MORDOR_UNITTEST(ProposerInstanceTest, P1PendingRetryBad) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);
    
    const BallotId kLesserBallotId = kBallotId - 1;
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Retry(kLesserBallotId));
}

MORDOR_UNITTEST(ProposerInstanceTest, P1OpenP2PendingClientValue) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Open(kBallotId);

    shared_ptr<Value> value(new Value);
    instance.phase2PendingWithClientValue(value);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P2_PENDING_CLIENT_VALUE);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
    MORDOR_TEST_ASSERT_EQUAL(instance.clientValue().get(), value.get());
}

MORDOR_UNITTEST(ProposerInstanceTest, P2PendingClientValueP1Pending) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Open(kBallotId);
    
    shared_ptr<Value> value(new Value);
    instance.phase2PendingWithClientValue(value);

    const BallotId kNextBallotId = kBallotId + 1;
    instance.phase1Pending(kNextBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P1_PENDING);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kNextBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
    MORDOR_TEST_ASSERT_EQUAL(value.use_count(), 1);
}

MORDOR_UNITTEST(ProposerInstanceTest, P2PendingP1Pending) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Pending(kBallotId);

    shared_ptr<Value> value(new Value);
    instance.phase2Pending(value);

    const BallotId kNextBallotId = kBallotId + 1;
    instance.phase1Pending(kNextBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::P1_PENDING);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kNextBallotId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.value());
    MORDOR_TEST_ASSERT_EQUAL(value.use_count(), 1);
}

MORDOR_UNITTEST(ProposerInstanceTest, P2PendingP1PendingBad) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Pending(kBallotId);

    shared_ptr<Value> value(new Value);
    instance.phase2Pending(value);

    const BallotId kLesserBallotId = kBallotId - 1;
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Pending(kLesserBallotId));
}

MORDOR_UNITTEST(ProposerInstanceTest, P2PendingClientValueP1PendingBad) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Open(kBallotId);

    shared_ptr<Value> value(new Value);
    instance.phase2PendingWithClientValue(value);

    const BallotId kLesserBallotId = kBallotId - 1;
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Pending(kLesserBallotId));
}

MORDOR_UNITTEST(ProposerInstanceTest, P2PendingWithClientValueClosed) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;

    instance.phase1Open(kBallotId);

    shared_ptr<Value> value(new Value);
    instance.phase2PendingWithClientValue(value);
    instance.close();
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::CLOSED);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.value().get(), value.get());
}

MORDOR_UNITTEST(ProposerInstanceTest, P2PendingClosed) {
    const InstanceId kInstanceId = 239;
    ProposerInstance instance(kInstanceId);

    const BallotId kBallotId = 13;
    instance.phase1Pending(kBallotId);
    shared_ptr<Value> value(new Value);
    instance.phase2Pending(value);

    instance.close();
    MORDOR_TEST_ASSERT_EQUAL(instance.state(), ProposerInstance::CLOSED);
    MORDOR_TEST_ASSERT_EQUAL(instance.ballotId(), kBallotId);
    MORDOR_TEST_ASSERT_EQUAL(instance.value().get(), value.get());
}

namespace {

void makeInstance(ProposerInstance::State state, ProposerInstance* instance, InstanceId iid) {
    instance->reset(iid);
    const BallotId kBallotId = 13;
    shared_ptr<Value> value(new Value);
    switch(state) {
        case ProposerInstance::EMPTY:
            break;
        case ProposerInstance::P1_OPEN:
            instance->phase1Open(kBallotId);
            break;
        case ProposerInstance::P1_PENDING:
            instance->phase1Pending(kBallotId);
            break;
        case ProposerInstance::P2_PENDING_CLIENT_VALUE:
            makeInstance(ProposerInstance::P1_OPEN, instance, iid);
            instance->phase2PendingWithClientValue(value);
            break;
        case ProposerInstance::P2_PENDING:
            makeInstance(ProposerInstance::P1_PENDING, instance, iid);
            instance->phase2Pending(value);
            break;
        case ProposerInstance::CLOSED:
            makeInstance(ProposerInstance::P2_PENDING, instance, iid);
            instance->close();
            break;
        default:
            MORDOR_TEST_ASSERT_EQUAL(1, 0);
    }
}

}  // anonymous namespace

MORDOR_UNITTEST(ProposerInstanceTest, InvalidTransitions) {
    const InstanceId kInstanceId = 239;
    const BallotId kBallotId = 139;
    shared_ptr<Value> value(new Value);
    ProposerInstance instance(kInstanceId);

    //!ProposerInstance::EMPTY P1Retry
    makeInstance(ProposerInstance::EMPTY, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Retry(kBallotId));

    //!ProposerInstance::EMPTY P2Pending
    makeInstance(ProposerInstance::EMPTY, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase2Pending(value));

    //!ProposerInstance::EMPTY CLOSED
    makeInstance(ProposerInstance::EMPTY, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.close());

    //! P1_OPEN P1_PENDING
    makeInstance(ProposerInstance::P1_OPEN, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Pending(kBallotId));

    //! P1_OPEN P2_PENDING
    makeInstance(ProposerInstance::P1_OPEN, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase2Pending(value));

    //! P1_OPEN P1Retry
    makeInstance(ProposerInstance::P1_OPEN, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Retry(kBallotId));

    //! P1_OPEN CLOSED
    makeInstance(ProposerInstance::P1_OPEN, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.close());

    //! P1_OPEN P1_OPEN
    makeInstance(ProposerInstance::P1_OPEN, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Open(kBallotId));

    //! P1_PENDING P1_PENDING
    makeInstance(ProposerInstance::P1_PENDING, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Pending(kBallotId));

    //! P1_PENDING CLOSED
    makeInstance(ProposerInstance::P1_PENDING, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.close());

    //! P1_PENDING P2_PENDING_CLIENT_VALUE
    makeInstance(ProposerInstance::P1_PENDING, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase2PendingWithClientValue(value));

    //! P2_PENDING P2_PENDING
    makeInstance(ProposerInstance::P2_PENDING, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase2Pending(value));

    //! P2_PENDING P1Retry
    makeInstance(ProposerInstance::P2_PENDING, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Retry(kBallotId));

    //! P2_PENDING P1_OPEN
    makeInstance(ProposerInstance::P2_PENDING, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Open(kBallotId));

    //! CLOSED P1_OPEN
    makeInstance(ProposerInstance::CLOSED, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Open(kBallotId));

    //! CLOSED P1_PENDING
    makeInstance(ProposerInstance::CLOSED, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Pending(kBallotId));

    //! CLOSED P1Retry
    makeInstance(ProposerInstance::CLOSED, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase1Retry(kBallotId));

    //! CLOSED P2_PENDING
    makeInstance(ProposerInstance::CLOSED, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.phase2Pending(value));

    //! CLOSED CLOSED
    makeInstance(ProposerInstance::CLOSED, &instance, kInstanceId);
    MORDOR_TEST_ASSERT_ASSERTED(instance.close());
}

MORDOR_UNITTEST(ProposerInstanceTest, ResetInstance) {
    const InstanceId kInstanceId = 239;
    shared_ptr<Value> value(new Value);
    ProposerInstance instance(kInstanceId);

    const InstanceId kNewInstanceId = kInstanceId + 1543;

    makeInstance(ProposerInstance::EMPTY, &instance, kInstanceId);
    instance.reset(kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.instanceId(), kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(),ProposerInstance::EMPTY);

    makeInstance(ProposerInstance::P1_OPEN, &instance, kInstanceId);
    instance.reset(kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.instanceId(), kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(),ProposerInstance::EMPTY);

    makeInstance(ProposerInstance::P1_PENDING, &instance, kInstanceId);
    instance.reset(kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.instanceId(), kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(),ProposerInstance::EMPTY);

    makeInstance(ProposerInstance::P2_PENDING, &instance, kInstanceId);
    instance.reset(kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.instanceId(), kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(),ProposerInstance::EMPTY);

    makeInstance(ProposerInstance::CLOSED, &instance, kInstanceId);
    instance.reset(kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.instanceId(), kNewInstanceId);
    MORDOR_TEST_ASSERT_EQUAL(instance.state(),ProposerInstance::EMPTY);
}
