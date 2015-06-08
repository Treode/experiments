#
# Copyright 2015 Treode, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CFLAGS = -std=c++11
LDFLAGS = -ltbb

HDRS = $(wildcard src/*.hpp)
SRCS = $(wildcard src/*.cpp)
OBJS = $(SRCS:%.cpp=%.o)

TEST_SRCS = $(wildcard tests/*.cpp)
TEST_OBJS = $(TEST_SRCS:%.cpp=%.o)

TARGETS = target/perf target/test

default: $(TARGETS) runtest

# Optimize.
$(OBJS): %.o: %.cpp $(HDRS)
	g++ $(CFLAGS) -O3 $< -c -o $@

# Include main headers; don't optimize.
$(TEST_OBJS): %.o: %.cpp $(HDRS)
	g++ $(CFLAGS) -Isrc $< -c -o $@

default: runtest

target/perf: $(OBJS)
	-mkdir target
	g++ $(CFLAGS) $(LDFLAGS) $^ -o $@

target/test: $(filter-out src/main.o, $(OBJS)) $(TEST_OBJS)
	-mkdir target
	g++ $(CFLAGS) $(LDFLAGS) $^ -o $@

.PHONY: runperf
runperf: target/perf
	target/perf

.PHONY: runtest
runtest: target/test
	target/test

.PHONY: clean
clean:
	-rm -f $(OBJS) $(TEST_OBJS) $(TARGETS)
