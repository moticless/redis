# Redis Makefile
# Copyright (c) 2011-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of (a) the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
#
# Compatibility wrapper: the real build is CMake (see ../CMakeLists.txt and
# ../cmake/RedisOptions.cmake). This Makefile keeps the familiar targets and
# variables working by translating them to CMake cache variables and building
# in $(BUILD_DIR). Binaries are copied into src/ as before.
#
#   make [MALLOC=libc|jemalloc|tcmalloc|tcmalloc_minimal] [USE_JEMALLOC=no]
#        [BUILD_TLS=yes|module] [USE_SYSTEMD=yes|no] [SANITIZER=address|...]
#        [OPTIMIZATION=-O2] [CFLAGS=...] [LDFLAGS=...] [REDIS_CFLAGS=...]
#        [REDIS_LDFLAGS=...] [PROG_SUFFIX=...] [DEBUG_DEFRAG=force|fully]
#        [BUILD_COMPRESSION=yes] [SKIP_VEC_SETS=yes] [LUA_DEBUG=yes] [V=1]
#   make test | test-modules | test-sentinel | check | install [PREFIX=...]
#   make noopt | valgrind | helgrind | 32bit | gcov | lcov | commands.def
#   make clean | distclean

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
BUILD_DIR ?= ../build/make
CMAKE ?= cmake
PREFIX ?= /usr/local
OPTIMIZATION ?= -O3
# Like the legacy build: an OPTIMIZATION from the command line or the
# environment also reaches deps/hiredis (its Makefile has OPTIMIZATION?=-O3).
ifneq ($(filter command environment,$(firstword $(origin OPTIMIZATION))),)
	HIREDIS_OPTIMIZATION = $(OPTIMIZATION)
else
	HIREDIS_OPTIMIZATION = -O3
endif

# Allocator selection, same defaults and backwards-compatible switches as before.
ifeq ($(uname_S),Linux)
	MALLOC ?= jemalloc
else
	MALLOC ?= libc
endif
ifeq ($(USE_TCMALLOC),yes)
	MALLOC = tcmalloc
endif
ifeq ($(USE_TCMALLOC_MINIMAL),yes)
	MALLOC = tcmalloc_minimal
endif
ifeq ($(USE_JEMALLOC),yes)
	MALLOC = jemalloc
endif
ifeq ($(USE_JEMALLOC),no)
	MALLOC = libc
endif

BUILD_TLS ?= no
USE_SYSTEMD ?= auto
ifeq ($(BUILD_COMPRESSION),yes)
	COMPRESSION = ON
else
	COMPRESSION = OFF
endif
ifeq ($(SKIP_VEC_SETS),yes)
	VEC_SETS_SKIP = ON
else
	VEC_SETS_SKIP = OFF
endif
ifeq ($(LUA_DEBUG),yes)
	LUA_DBG = ON
else
	LUA_DBG = OFF
endif

ifneq ($(shell sh -c 'command -v ninja'),)
	GENERATOR = Ninja
else
	GENERATOR = Unix Makefiles
endif

# Pass every setting explicitly, so a later plain `make` returns to the
# defaults instead of keeping values cached by a previous invocation.
CMAKE_ARGS = -S .. -B $(BUILD_DIR) -G "$(GENERATOR)" \
	-DREDIS_MALLOC="$(MALLOC)" \
	-DREDIS_BUILD_TLS="$(BUILD_TLS)" \
	-DREDIS_SYSTEMD="$(USE_SYSTEMD)" \
	-DREDIS_SANITIZER="$(SANITIZER)" \
	-DREDIS_OPTIMIZATION="$(OPTIMIZATION)" \
	-DREDIS_HIREDIS_OPTIMIZATION="$(HIREDIS_OPTIMIZATION)" \
	-DREDIS_CFLAGS="$(REDIS_CFLAGS)" \
	-DREDIS_LDFLAGS="$(REDIS_LDFLAGS)" \
	-DREDIS_PROG_SUFFIX="$(PROG_SUFFIX)" \
	-DREDIS_DEBUG_DEFRAG="$(DEBUG_DEFRAG)" \
	-DREDIS_COMPRESSION=$(COMPRESSION) \
	-DREDIS_SKIP_VEC_SETS=$(VEC_SETS_SKIP) \
	-DREDIS_LUA_DEBUG=$(LUA_DBG) \
	-DCMAKE_C_FLAGS="$(CFLAGS)" \
	-DCMAKE_EXE_LINKER_FLAGS="$(LDFLAGS)" \
	-DCMAKE_SHARED_LINKER_FLAGS="$(LDFLAGS)" \
	-DCMAKE_MODULE_LINKER_FLAGS="$(LDFLAGS)" \
	-DCMAKE_INSTALL_PREFIX="$(PREFIX)"
ifdef CC
ifneq ($(origin CC),default)
	CMAKE_ARGS += -DCMAKE_C_COMPILER="$(CC)"
endif
endif

# Configure only when the settings changed (or on first use); otherwise the
# build tool re-runs CMake by itself when a CMakeLists.txt changes. This keeps
# a no-op `make` at the cost of a ninja no-op instead of a full configure.
ARGS_STAMP = $(BUILD_DIR)/.make-cmake-args
QUOTED_ARGS = '$(subst ','\'',$(CMAKE_ARGS))'
CONFIGURE = mkdir -p $(BUILD_DIR); \
	if [ ! -f $(BUILD_DIR)/CMakeCache.txt ] || [ "$$(cat $(ARGS_STAMP) 2>/dev/null)" != $(QUOTED_ARGS) ]; then \
		$(CMAKE) $(CMAKE_ARGS) > /dev/null && printf '%s' $(QUOTED_ARGS) > $(ARGS_STAMP); \
	fi

# `make -jN` becomes `cmake --build -j N`. Evaluated when a recipe runs: before
# GNU make 4.4, -jN is not in MAKEFLAGS while the makefile is parsed. Older make
# records a bare -j (or only --jobserver-fds); use the CPU count then.
JOBS = $(or $(patsubst -j%,%,$(filter -j%,$(MAKEFLAGS))),$(if $(filter -j --jobserver%,$(MAKEFLAGS)),$(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)))
BUILD_ARGS = $(if $(JOBS),-j $(JOBS)) $(if $(V),-v)
# Clear MAKEFLAGS for the build tool: under a Makefiles generator the inner make
# would inherit a jobserver it cannot reach and fall back to -j1.
CMAKE_BUILD = MAKEFLAGS= MFLAGS= $(CMAKE) --build $(BUILD_DIR)

all:
	@$(CONFIGURE)
	@$(CMAKE_BUILD) $(BUILD_ARGS)
	@echo ""
	@echo "Hint: It's a good idea to run 'make test' ;)"
	@echo ""

.PHONY: all

module_tests:
	$(MAKE) -C ../tests/modules

test: all
	@(cd ..; ./runtest)

test-modules: all
	@(cd ..; ./runtest-moduleapi)

test-sentinel: all
	@(cd ..; ./runtest-sentinel)

check: test

bench: all
	./redis-benchmark$(PROG_SUFFIX)

install: all
	@$(CONFIGURE)
	@$(CMAKE_BUILD) --target install

uninstall:
	rm -f $(PREFIX)/bin/{redis-server,redis-benchmark,redis-cli,redis-check-rdb,redis-check-aof,redis-sentinel}$(PROG_SUFFIX)

commands.def:
	@$(CONFIGURE)
	@$(CMAKE_BUILD) --target redis_commands_def

noopt:
	$(MAKE) OPTIMIZATION="-O0"

valgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc"

helgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc" CFLAGS="-D__ATOMIC_VAR_FORCE_SYNC_MACROS" REDIS_CFLAGS="-I/usr/local/include" REDIS_LDFLAGS="-L/usr/local/lib"

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32" SKIP_VEC_SETS="yes"

# Use -fprofile-update=atomic because IO threads can update counters concurrently.
gcov:
	$(MAKE) REDIS_CFLAGS="-fprofile-arcs -ftest-coverage -fprofile-update=atomic -DCOVERAGE_TEST" REDIS_LDFLAGS="-fprofile-arcs -ftest-coverage"

lcov:
	@lcov --version
	$(MAKE) gcov
	@(set -e; cd ..; ./runtest)
	@geninfo --rc geninfo_unexecuted_blocks=1 -o redis.info $(BUILD_DIR)
	@genhtml --legend -o lcov-html redis.info

clean:
	-@[ -d $(BUILD_DIR) ] && $(CMAKE_BUILD) --target clean
	rm -f redis-server$(PROG_SUFFIX) redis-sentinel$(PROG_SUFFIX) redis-cli$(PROG_SUFFIX) \
	      redis-benchmark$(PROG_SUFFIX) redis-check-rdb$(PROG_SUFFIX) redis-check-aof$(PROG_SUFFIX) \
	      redis-tls$(PROG_SUFFIX).so redis.info
	rm -rf lcov-html
	-(cd ../tests/modules && $(MAKE) clean)

distclean: clean
	rm -rf $(BUILD_DIR)
	-(cd modules && $(MAKE) clean)

.PHONY: module_tests test test-modules test-sentinel check bench install uninstall \
	commands.def noopt valgrind helgrind 32bit gcov lcov clean distclean
