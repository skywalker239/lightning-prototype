CXX = g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Werror -Wsign-promo
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt

LIB_TARGETS = libightning.a
LIB_OBJS = ping_stats.o pinger.o ponger.o pong_receiver.o multicast_util.o ping_tracker.o datacenter_aware_quorum_ring_oracle.o ring_manager.o
TEST_TARGETS = test_ring_master test_ring_responder
TEST_OBJS = $(addsuffix .o, $(TEST_TARGETS))

all: $(LIB_TARGETS) $(TEST_TARGETS)

$(LIB_TARGETS): %: $(LIB_OBJS)
	ar crs $(@) $(^)

$(TEST_TARGETS): %: %.o $(LIB_TARGETS)
	$(CXX) $(<) $(LIB_TARGETS) -static -o $(@)  $(LDFLAGS)


%.o: %.cc; $(CXX) -c $(CXXFLAGS) $(<) -o $(@)

clean:; @rm -f $(TEST_TARGETS) $(TEST_OBJS) $(LIB_TARGETS) $(LIB_OBJS)
