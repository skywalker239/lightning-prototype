CXX = g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Werror -Wsign-promo
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt

TARGETS = test_simple

TMPS += $(addsuffix .o, $(TARGETS))
TMPS += $(addsuffix .s, $(TARGETS))

all: $(TARGETS)

$(TARGETS): %: %.o
	$(CXX) $(<) -static -o $(@) $(LDFLAGS)


%.o: %.cc; $(CXX) -c $(CXXFLAGS) $(<) -o $(@)

clean:; @rm -f $(TARGETS) $(TMPS)
