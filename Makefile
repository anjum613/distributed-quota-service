CXX ?= g++
CXXFLAGS ?= -std=c++20 -O2 -g -Wall -Wextra -Wconversion -Wshadow
CPPFLAGS = -Isrc

.PHONY: all test clean
all: build/core_tests
build:
	mkdir -p build
build/core_tests: tests/core_tests.cpp src/counter.hpp | build
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@
test: all
	./build/core_tests
clean:
	rm -rf build
