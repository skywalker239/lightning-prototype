#include "value_id_map.h"
#include <mordor/iomanager.h>
#include <mordor/test/test.h>

using namespace Mordor;
using namespace lightning::paxos;


MORDOR_UNITTEST(ValueIdMapTest, EmptyMap) {
    ValueIdMap testMap(239);
    IOManager dummyManager;

    MORDOR_TEST_ASSERT_EQUAL(testMap.size(), size_t(0));
}

MORDOR_UNITTEST(ValueIdMapTest, LookupFail) {
    ValueIdMap testMap(239);
    IOManager dummyManager;

    ValueId vid(1, 239);
    MORDOR_TEST_ASSERT_EQUAL(testMap.lookup(vid), false);
}

MORDOR_UNITTEST(ValueIdMapTest, LookupAndFetch) {
    ValueIdMap testMap(239);
    IOManager dummyManager;

    ValueId vid(1, 239);
    Value dummy;
    dummy.valueId = vid;
    testMap.addMapping(vid, dummy);
    MORDOR_TEST_ASSERT_EQUAL(testMap.size(), size_t(1));
    MORDOR_TEST_ASSERT_EQUAL(testMap.lookup(vid), true);
    Value fetched;
    MORDOR_TEST_ASSERT_EQUAL(testMap.fetch(vid, &fetched), true);
    MORDOR_TEST_ASSERT_EQUAL(dummy.valueId, fetched.valueId);
}

MORDOR_UNITTEST(ValueIdMapTest, Eviction) {
    ValueIdMap testMap(1);
    IOManager dummyManager;

    ValueId vid1(1, 2);
    ValueId vid2(1, 3);
    Value dummy;

    testMap.addMapping(vid1, dummy);
    MORDOR_TEST_ASSERT_EQUAL(testMap.size(), size_t(1));
    MORDOR_TEST_ASSERT_EQUAL(testMap.lookup(vid1), true);
    testMap.addMapping(vid2, dummy);
    MORDOR_TEST_ASSERT_EQUAL(testMap.size(), size_t(1));
    MORDOR_TEST_ASSERT_EQUAL(testMap.lookup(vid1), false);
    MORDOR_TEST_ASSERT_EQUAL(testMap.lookup(vid2), true);
}

