#include "paxos_instance.h"
#include <mordor/test/test.h>

using namespace Mordor;
using namespace lightning::paxos;

MORDOR_UNITTEST(AcceptorInstanceTest, EmptyInstance) {
    AcceptorInstance instance;

    MORDOR_TEST_ASSERT_EQUAL(instance.value(NULL), false);
}

MORDOR_UNITTEST(AcceptorInstanceTest, SimpleCommit) {
    AcceptorInstance instance;

    BallotId firstBallot(kInvalidBallotId + 1);
    BallotId lastBallot;
    BallotId lastVotedBallot;
    ValueId  lastVote;
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(firstBallot,
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), true);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, firstBallot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, kInvalidBallotId);
    MORDOR_TEST_ASSERT_EQUAL(lastVote.epochId, kInvalidEpochId);

    ValueId vid(1, 239);
    BallotId highestPromisedBallotId = kInvalidBallotId;
    MORDOR_TEST_ASSERT_EQUAL(instance.beginBallot(firstBallot, vid, &highestPromisedBallotId), true);
    MORDOR_TEST_ASSERT_EQUAL(highestPromisedBallotId, kInvalidBallotId);

    // XXX nothing to test on commit(), because a value might be committed
    // that this acceptor never voted for.
    instance.commit(vid);
    ValueId committedVid;
    MORDOR_TEST_ASSERT_EQUAL(instance.value(&committedVid), true);
    MORDOR_TEST_ASSERT_EQUAL(vid, committedVid);
}

MORDOR_UNITTEST(AcceptorInstanceTest, Phase1Fail) {
    AcceptorInstance instance;

    BallotId ballot(239);
    BallotId lowerBallot(200);

    BallotId lastBallot;
    BallotId lastVotedBallot;
    ValueId  lastVote;
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(ballot,
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), true);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, ballot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, kInvalidBallotId);
    MORDOR_TEST_ASSERT_EQUAL(lastVote.epochId, kInvalidEpochId);

    //! Try to initiate P1 with lower ballot.
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(lowerBallot, 
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), false);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, ballot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, kInvalidBallotId);
    MORDOR_TEST_ASSERT_EQUAL(lastVote.epochId, kInvalidEpochId);
}

//! Test starting phase 1 on an instance which has a value reserved.
MORDOR_UNITTEST(AcceptorInstanceTest, NonTrivialPhase1) {
    AcceptorInstance instance;

    BallotId firstBallot(kInvalidBallotId + 1);
    BallotId lastBallot;
    BallotId lastVotedBallot;
    ValueId  lastVote;
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(firstBallot,
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), true);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, firstBallot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, kInvalidBallotId);
    MORDOR_TEST_ASSERT_EQUAL(lastVote.epochId, kInvalidEpochId);

    ValueId vid(1, 239);
    BallotId highestPromisedBallotId = kInvalidBallotId;
    MORDOR_TEST_ASSERT_EQUAL(instance.beginBallot(firstBallot, vid, &highestPromisedBallotId), true);
    MORDOR_TEST_ASSERT_EQUAL(highestPromisedBallotId, kInvalidBallotId);

    // Now start a next ballot.
    BallotId secondBallot(firstBallot + 1);
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(secondBallot,
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), true);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, secondBallot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, firstBallot);
    MORDOR_TEST_ASSERT_EQUAL(lastVote, vid);

    // Phase 2 must work out with second ballot.
    MORDOR_TEST_ASSERT_EQUAL(instance.beginBallot(secondBallot, vid, &highestPromisedBallotId), true);
    MORDOR_TEST_ASSERT_EQUAL(highestPromisedBallotId, kInvalidBallotId);
}

MORDOR_UNITTEST(AcceptorInstanceTest, Phase2Fail) {
    AcceptorInstance instance;

    BallotId firstBallot(kInvalidBallotId + 1);
    BallotId lastBallot;
    BallotId lastVotedBallot;
    ValueId  lastVote;
    // Pass the phase 1 for the first ballot.
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(firstBallot,
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), true);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, firstBallot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, kInvalidBallotId);
    MORDOR_TEST_ASSERT_EQUAL(lastVote.epochId, kInvalidEpochId);

    // Now another master kicks in and performs phase 1 for
    // a higher ballot number.
    BallotId secondBallot(firstBallot + 1);
    MORDOR_TEST_ASSERT_EQUAL(instance.nextBallot(secondBallot,
                                                 &lastBallot,
                                                 &lastVotedBallot,
                                                 &lastVote), true);
    MORDOR_TEST_ASSERT_EQUAL(lastBallot, secondBallot);
    MORDOR_TEST_ASSERT_EQUAL(lastVotedBallot, kInvalidBallotId);
    MORDOR_TEST_ASSERT_EQUAL(lastVote.epochId, kInvalidEpochId);

    // Now the first master wakes up, attempts to perform P2 and must fail.
    ValueId vid(1, 239);
    BallotId highestPromisedBallotId = kInvalidBallotId;
    MORDOR_TEST_ASSERT_EQUAL(instance.beginBallot(firstBallot, vid, &highestPromisedBallotId), false);
    MORDOR_TEST_ASSERT_EQUAL(highestPromisedBallotId, secondBallot);
}
