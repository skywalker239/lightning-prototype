CXX = g++ --std=gnu++0x
OPT ?= 3

CXXFLAGS= \
    -g -O$(OPT) `pkg-config --cflags libmordor` \
    -Wall -W -Werror -Wsign-promo
LDFLAGS = `pkg-config --libs --static libmordor` -lpthread -lrt

TARGETS = libightning.a
OBJS = ping_stats.o pinger.o ponger.o pong_receiver.o


all: $(TARGETS)

$(TARGETS): %: $(OBJS)
	ar crs $(@) $(^)


%.o: %.cc; $(CXX) -c $(CXXFLAGS) $(<) -o $(@)

clean:; @rm -f $(TARGETS) $(OBJS)
