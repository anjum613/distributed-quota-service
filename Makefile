CXX ?= g++
CXXFLAGS ?= -std=c++20 -O2 -g -Wall -Wextra -Wconversion -Wshadow
CPPFLAGS = -Isrc
SANITIZER_LDFLAGS ?=
SANITIZER_OUT ?= results/sanitized

.PHONY: all test check demo benchmark stress clean sanitize
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
benchmark: all
	python3 scripts/benchmark.py
stress: all
	python3 scripts/stress.py --seconds 30
sanitize: | build
	$(CXX) $(CPPFLAGS) -std=c++20 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer tests/core_tests.cpp $(SANITIZER_LDFLAGS) -o build/core_tests_san
	./build/core_tests_san
	$(CXX) $(CPPFLAGS) -std=c++20 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer src/server.cpp $(SANITIZER_LDFLAGS) -o build/quota_server_san
	QUOTA_SERVER=build/quota_server_san python3 scripts/correctness.py --out $(SANITIZER_OUT)
clean:
	rm -rf build
