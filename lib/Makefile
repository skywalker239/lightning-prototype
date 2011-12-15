CXX = g++ --std=gnu++0x
OPT ?= 0

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Wsign-promo -Wno-deprecated
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt -lprotobuf

PROTO_SRCS = \
    proto/set_ring.proto \
    proto/ping.proto \
    proto/sync_group_request.proto \
    proto/batch_phase1.proto

PROTO_GENSRCS = $(PROTO_SRCS:.proto=.pb.cc)
PROTO_GENHDRS = $(PROTO_SRCS:.proto=.pb.h)

LIB_TARGETS = libightning.a
LIB_OBJS = $(PROTO_GENSRCS:.cc=.o)
LIB_OBJS += \
    ping_stats.o \
    pinger.o \
    ponger.o \
    multicast_util.o \
    ping_tracker.o \
    datacenter_aware_quorum_ring_oracle.o \
    ring_manager.o \
    value_id_map.o \
    proposer_instance.o \
    acceptor_instance.o \
    sync_group_requester.o \
    ping_request.o \
    set_ring_request.o \
    instance_pool.o \
    ring_holder.o \
    ring_change_notifier.o \
    sync_group_responder.o \
    batch_phase1_request.o \
    phase1_batcher.o \
    

TEST_TARGETS = test_ring_master test_ring_responder
TEST_OBJS = $(addsuffix .o, $(TEST_TARGETS))

UT_LIB_OBJS = acceptor_instance_ut.o proposer_instance_ut.o value_id_map_ut.o
UT_TARGETS = run_tests
UT_OBJS = $(addsuffix .o, $(UT_TARGETS))

all: $(LIB_TARGETS) $(TEST_TARGETS) $(UT_TARGETS)

$(LIB_TARGETS): %: $(LIB_OBJS)
	ar crs $(@) $(^)

$(TEST_TARGETS): %: %.o $(LIB_TARGETS)
	$(CXX) $(<) $(LIB_TARGETS) -static -o $(@)  $(LDFLAGS)

$(UT_TARGETS): %: %.o $(LIB_TARGETS) $(UT_LIB_OBJS)
	$(CXX) $(<) $(UT_LIB_OBJS) $(LIB_TARGETS) -static -o $(@) $(LDFLAGS) `pkg-config --libs --static libmordortest`

%.pb.cc: %.proto; protoc --cpp_out=proto -I proto $(<)
%.o: %.cc; $(CXX) -c $(CXXFLAGS) $(<) -o $(@)

clean:; @rm -f $(TEST_TARGETS) $(TEST_OBJS) $(LIB_TARGETS) $(LIB_OBJS) $(UT_LIB_OBJS) $(UT_TARGETS) $(UT_OBJS) $(PROTO_GENSRCS) $(PROTO_GENHDRS)
