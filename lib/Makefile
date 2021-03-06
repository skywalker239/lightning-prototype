CXX = g++ --std=gnu++0x # clang++ # g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Wsign-promo -Wno-deprecated
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt -lprotobuf

PROTO_SRCS = \
    proto/rpc_messages.proto

PROTO_GENSRCS = $(PROTO_SRCS:.proto=.pb.cc)
PROTO_GENHDRS = $(PROTO_SRCS:.proto=.pb.h)

LIB_TARGETS = libightning.a
LIB_OBJS = $(PROTO_GENSRCS:.cc=.o)
LIB_OBJS += \
    acceptor_state.o \
    ballot_generator.o \
    blocking_abcast.o \
    host_configuration.o \
    ring_configuration.o \
    MurmurHash3.o \
    guid.o \
    rpc_requester.o \
    rpc_responder.o \
    multicast_rpc_stats.o \
    set_ring_handler.o \
    ping_stats.o \
    pinger.o \
    ponger.o \
    multicast_util.o \
    ping_tracker.o \
    datacenter_aware_quorum_ring_oracle.o \
    ring_manager.o \
    proposer_instance.o \
    acceptor_instance.o \
    ping_request.o \
    set_ring_request.o \
    instance_pool.o \
    ring_holder.o \
    ring_change_notifier.o \
    batch_phase1_handler.o \
    phase1_handler.o \
    phase2_handler.o \
    ring_voter.o \
    batch_phase1_request.o \
    phase1_request.o \
    phase2_request.o \
    proposer_state.o \
    phase1_batcher.o \
    multicast_rpc_request.o \
    udp_sender.o \
    vote.o \
    sleep_helper.o \
    tcp_recovery_service.o \
    rpc_request.o \
    unicast_rpc_request.o \
    recovery_manager.o \
    value.o \
    recovery_record.o \
    recovery_connection.o \
    value_buffer.o \
    tcp_value_receiver.o \
    stream_reassembler.o \
    value_cache.o \
    commit_tracker.o \

TEST_TARGETS = test_ring_master test_ring_acceptor test_ring_learner submit_random_values submit_snapshot
TEST_OBJS = $(addsuffix .o, $(TEST_TARGETS))

UT_LIB_OBJS = \
# acceptor_instance_ut.o \
#    proposer_instance_ut.o \
#    value_id_map_ut.o

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
