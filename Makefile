CXX ?= g++
CXXFLAGS ?= -std=c++20 -O2 -g -Wall -Wextra -Wconversion -Wshadow
CPPFLAGS = -Isrc

.PHONY: all test check demo clean
all: build/quota_server build/core_tests build/explore
build:
	mkdir -p build
build/quota_server: src/server.cpp src/counter.hpp | build
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@
build/core_tests: tests/core_tests.cpp src/counter.hpp | build
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@
build/explore: tests/explore.cpp src/counter.hpp | build
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@
test: all
	./build/core_tests
	./build/explore
	python3 scripts/correctness.py
check: test
demo: all
	python3 scripts/demo.py
clean:
	rm -rf build
