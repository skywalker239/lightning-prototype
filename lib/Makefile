CXX = g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Werror -Wsign-promo -Wno-deprecated
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt

LIB_TARGETS = libightning.a
LIB_OBJS = ping_stats.o pinger.o ponger.o pong_receiver.o multicast_util.o ping_tracker.o datacenter_aware_quorum_ring_oracle.o ring_manager.o value_id_map.o paxos_instance.o

TEST_TARGETS = test_ring_master test_ring_responder
TEST_OBJS = $(addsuffix .o, $(TEST_TARGETS))

UT_LIB_OBJS = paxos_instance_ut.o value_id_map_ut.o
UT_TARGETS = run_tests
UT_OBJS = $(addsuffix .o, $(UT_TARGETS))

all: $(LIB_TARGETS) $(TEST_TARGETS) $(UT_TARGETS)

$(LIB_TARGETS): %: $(LIB_OBJS)
	ar crs $(@) $(^)

$(TEST_TARGETS): %: %.o $(LIB_TARGETS)
	$(CXX) $(<) $(LIB_TARGETS) -static -o $(@)  $(LDFLAGS)

$(UT_TARGETS): %: %.o $(LIB_TARGETS) $(UT_LIB_OBJS)
	$(CXX) $(<) $(UT_LIB_OBJS) $(LIB_TARGETS) -static -o $(@) $(LDFLAGS) `pkg-config --libs --static libmordortest`

%.o: %.cc; $(CXX) -c $(CXXFLAGS) $(<) -o $(@)

clean:; @rm -f $(TEST_TARGETS) $(TEST_OBJS) $(LIB_TARGETS) $(LIB_OBJS) $(UT_LIB_OBJS) $(UT_TARGETS) $(UT_OBJS)
