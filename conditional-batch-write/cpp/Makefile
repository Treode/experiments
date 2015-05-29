CFLAGS = -std=c++11

HDRS = $(wildcard src/*.hpp)
SRCS = $(wildcard src/*.cpp)
OBJS = $(SRCS:%.cpp=%.o)

TEST_SRCS = $(wildcard tests/*.cpp)
TEST_OBJS = $(TEST_SRCS:%.cpp=%.o)

TARGETS = target/perf target/test

default: runtest

# Optimize.
$(OBJS): %.o: %.cpp $(HDRS)
	g++ $(CFLAGS) -O3 $< -c -o $@

# Include main headers; don't optimize.
$(TEST_OBJS): %.o: %.cpp $(HDRS)
	g++ $(CFLAGS) -Isrc $< -c -o $@

default: runtest

target/perf: $(OBJS)
	-mkdir target
	g++ $(CFLAGS) $^ -o $@

target/test: $(filter-out src/main.o, $(OBJS)) $(TEST_OBJS)
	-mkdir target
	g++ $(CFLAGS) $^ -o $@

.PHONY: runperf
runperf: target/perf
	target/perf

.PHONY: runtest
runtest: target/test
	target/test

.PHONY: clean
clean:
	-rm -f $(OBJS) $(TEST_OBJS) $(TARGETS)
