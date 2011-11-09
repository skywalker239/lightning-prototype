CXX = g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Werror -Wsign-promo
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt

TARGETS = test_simple test_pinger test_forwarder test_load_json test_multicast_receiver test_multicast_pinger test_multicast_ponger

TMPS += $(addsuffix .o, $(TARGETS))
TMPS += $(addsuffix .s, $(TARGETS))

all: $(TARGETS)

$(TARGETS): %: %.o
	$(CXX) $(<) -static -o $(@) $(LDFLAGS)


%.o: %.cc; $(CXX) -c $(CXXFLAGS) $(<) -o $(@)

clean:; @rm -f $(TARGETS) $(TMPS)
