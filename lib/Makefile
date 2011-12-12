CXX = g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Wsign-promo -Wno-deprecated
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt -lprotobuf

PROTO_SRCS = \
    proto/set_ring.proto \
    proto/ping.proto

PROTO_GENSRCS = $(PROTO_SRCS:.proto=.pb.cc)
PROTO_GENHDRS = $(PROTO_SRCS:.proto=.pb.h)

LIB_TARGETS = libightning.a
LIB_OBJS = ping_stats.o pinger.o ponger.o pong_receiver.o multicast_util.o ping_tracker.o datacenter_aware_quorum_ring_oracle.o ring_manager.o value_id_map.o proposer_instance.o acceptor_instance.o sync_group_requester.o ring_setter.o ping_requester.o
LIB_OBJS += $(PROTO_GENSRCS:.cc=.o)

TEST_TARGETS = test_ring_master test_ring_responder test_multicast_pinger test_ponger
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
