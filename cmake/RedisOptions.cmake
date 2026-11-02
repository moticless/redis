# Build options and compiler/linker flags, mirroring src/Makefile.
#
#   make variable           CMake cache variable
#   ---------------------   --------------------------------------------
#   MALLOC / USE_JEMALLOC   REDIS_MALLOC        jemalloc|libc|tcmalloc|tcmalloc_minimal
#   BUILD_TLS               REDIS_BUILD_TLS     no|yes|module
#   USE_SYSTEMD             REDIS_SYSTEMD       auto|yes|no
#   SANITIZER               REDIS_SANITIZER     ""|address|undefined|thread|memory
#   OPTIMIZATION            REDIS_OPTIMIZATION  default -O3 (LTO is added for -O3)
#   REDIS_CFLAGS            REDIS_CFLAGS        flags for redis sources only
#   REDIS_LDFLAGS           REDIS_LDFLAGS       link flags for redis binaries only
#   CFLAGS / LDFLAGS        CMAKE_C_FLAGS / CMAKE_EXE_LINKER_FLAGS (all targets)
#   PROG_SUFFIX             REDIS_PROG_SUFFIX
#   DEBUG_DEFRAG            REDIS_DEBUG_DEFRAG  ""|force|fully
#   BUILD_COMPRESSION       REDIS_COMPRESSION   ON|OFF
#   SKIP_VEC_SETS           REDIS_SKIP_VEC_SETS ON|OFF
#   LUA_DEBUG               REDIS_LUA_DEBUG     ON|OFF
#   PREFIX                  CMAKE_INSTALL_PREFIX

include(CheckCSourceCompiles)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(_default_malloc jemalloc)
else()
  set(_default_malloc libc)
endif()
set(REDIS_MALLOC ${_default_malloc} CACHE STRING "Allocator: jemalloc, libc, tcmalloc, tcmalloc_minimal")
set(REDIS_BUILD_TLS no CACHE STRING "TLS support: no, yes, module")
set(REDIS_SYSTEMD auto CACHE STRING "systemd support: auto, yes, no")
set(REDIS_SANITIZER "" CACHE STRING "Sanitizer: address, undefined, thread, memory")
set(REDIS_OPTIMIZATION "-O3" CACHE STRING "Optimization flags (LTO is added for -O3)")
set(REDIS_CFLAGS "" CACHE STRING "Extra compile flags for redis sources only")
set(REDIS_LDFLAGS "" CACHE STRING "Extra link flags for redis binaries only")
set(REDIS_PROG_SUFFIX "" CACHE STRING "Suffix appended to binary names")
# deps/hiredis/Makefile has "OPTIMIZATION?=-O3", and an OPTIMIZATION given to make on
# the command line or in the environment reaches it (make noopt/valgrind build hiredis
# with -O0). The make wrapper passes it through this variable.
set(REDIS_HIREDIS_OPTIMIZATION "-O3" CACHE STRING "Optimization flag for the bundled hiredis")
set(REDIS_DEBUG_DEFRAG "" CACHE STRING "Force defrag without jemalloc: force, fully")
option(REDIS_COMPRESSION "Replication compression with libzstd" OFF)
option(REDIS_SKIP_VEC_SETS "Do not build vector sets into redis-server" OFF)
option(REDIS_LUA_DEBUG "Build lua with -O0 and API checks" OFF)
option(REDIS_COPY_BINARIES_TO_SRC "Copy built binaries into src/ for the Tcl test suite" ON)

set(REDIS_TLS OFF)
if(REDIS_BUILD_TLS STREQUAL "yes" OR REDIS_BUILD_TLS STREQUAL "module")
  set(REDIS_TLS ON)
  find_package(OpenSSL REQUIRED)
elseif(NOT REDIS_BUILD_TLS STREQUAL "no")
  message(FATAL_ERROR "REDIS_BUILD_TLS must be no, yes or module (got '${REDIS_BUILD_TLS}')")
endif()

set(_is_clang OFF)
if(CMAKE_C_COMPILER_ID MATCHES "Clang")
  set(_is_clang ON)
endif()

# jemalloc and tests/modules keep GNU Makefiles: run them with GNU make (gmake on
# the BSDs, where "make" is BSD make), as $(MAKE) does under the legacy build.
find_program(REDIS_GNU_MAKE NAMES gmake make)
if(NOT REDIS_GNU_MAKE)
  message(FATAL_ERROR "GNU make (gmake or make) is required for the jemalloc and tests/modules sub-builds")
endif()

# Sanitizers force libc malloc, as in src/Makefile.
set(_san_flags "")
if(REDIS_SANITIZER STREQUAL "address")
  set(REDIS_MALLOC libc CACHE STRING "" FORCE)
  set(_san_flags -fsanitize=address -fno-sanitize-recover=all -fno-omit-frame-pointer)
elseif(REDIS_SANITIZER STREQUAL "undefined")
  set(REDIS_MALLOC libc CACHE STRING "" FORCE)
  set(_san_flags -fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
elseif(REDIS_SANITIZER STREQUAL "thread")
  set(_san_flags -fsanitize=thread -fno-sanitize-recover=all -fno-omit-frame-pointer)
elseif(REDIS_SANITIZER STREQUAL "memory")
  if(NOT _is_clang)
    message(FATAL_ERROR "MemorySanitizer needs clang (-DCMAKE_C_COMPILER=clang)")
  endif()
  set(REDIS_MALLOC libc CACHE STRING "" FORCE)
  set(_san_flags -fsanitize=memory -fsanitize-memory-track-origins=2 -fno-sanitize-recover=all -fno-omit-frame-pointer)
elseif(NOT REDIS_SANITIZER STREQUAL "")
  message(FATAL_ERROR "unknown REDIS_SANITIZER '${REDIS_SANITIZER}'")
endif()
# src/Makefile links with LDFLAGS+=-fsanitize=<name> only.
set(_san_ldflags "")
if(NOT REDIS_SANITIZER STREQUAL "")
  set(_san_ldflags -fsanitize=${REDIS_SANITIZER})
endif()
# Like deps/Makefile, only MemorySanitizer is propagated to the bundled deps.
set(REDIS_DEPS_C_FLAGS "")
set(REDIS_DEPS_LD_FLAGS "")
if(REDIS_SANITIZER STREQUAL "memory")
  list(JOIN _san_flags " " REDIS_DEPS_C_FLAGS)
  set(REDIS_DEPS_LD_FLAGS "-fsanitize=memory")
endif()

# Optimization: -O3 gets LTO, anything but -O0 keeps frame pointers.
separate_arguments(_opt UNIX_COMMAND "${REDIS_OPTIMIZATION}")
set(REDIS_LTO_FLAGS "")
if(REDIS_OPTIMIZATION STREQUAL "-O3")
  if(_is_clang)
    set(REDIS_LTO_FLAGS "-flto")
  else()
    set(REDIS_LTO_FLAGS "-flto=auto -ffat-lto-objects")
  endif()
  separate_arguments(_lto UNIX_COMMAND "${REDIS_LTO_FLAGS}")
  list(APPEND _opt ${_lto})
endif()
if(NOT REDIS_OPTIMIZATION STREQUAL "-O0")
  list(APPEND _opt -fno-omit-frame-pointer)
endif()

# C standard: gnu11 when <stdatomic.h> works, else c99 without vector sets.
set(CMAKE_REQUIRED_FLAGS "-std=gnu11")
check_c_source_compiles("#include <stdatomic.h>\nint main(void){return 0;}" REDIS_HAVE_C11_ATOMIC)
unset(CMAKE_REQUIRED_FLAGS)
if(REDIS_HAVE_C11_ATOMIC)
  set(_std -std=gnu11)
else()
  set(_std -std=c99)
  set(REDIS_SKIP_VEC_SETS ON)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm)")
  string(APPEND CMAKE_C_FLAGS " -funwind-tables")
endif()

# Everything the redis sources (not the deps) compile and link with.
add_library(redis_build_flags INTERFACE)
target_compile_options(redis_build_flags INTERFACE
  ${_std} -pedantic
  -Wall -W -Wno-missing-field-initializers -Werror=deprecated-declarations -Wstrict-prototypes
  ${_opt} -g -ggdb ${_san_flags})
# Keep __FILE__ in logs and crash reports short and independent of the checkout
# path (debug info keeps absolute paths, so gdb still finds sources). The last
# matching map wins: src/core/db.c -> "core/db.c", deps/... -> "deps/...".
# GCC < 8 has no -fmacro-prefix-map; there __FILE__ stays an absolute path.
include(CheckCCompilerFlag)
check_c_compiler_flag("-fmacro-prefix-map=/a=/b" REDIS_HAVE_MACRO_PREFIX_MAP)
if(REDIS_HAVE_MACRO_PREFIX_MAP)
  target_compile_options(redis_build_flags INTERFACE
    -fmacro-prefix-map=${CMAKE_SOURCE_DIR}/=
    -fmacro-prefix-map=${CMAKE_SOURCE_DIR}/src/=)
endif()
if(_is_clang OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_compile_options(redis_build_flags INTERFACE -Wno-c11-extensions)
endif()
target_compile_definitions(redis_build_flags INTERFACE REDIS_STATIC=)
target_link_options(redis_build_flags INTERFACE ${_opt} -g -ggdb ${_san_ldflags})
separate_arguments(_redis_cflags UNIX_COMMAND "${REDIS_CFLAGS}")
separate_arguments(_redis_ldflags UNIX_COMMAND "${REDIS_LDFLAGS}")
target_compile_options(redis_build_flags INTERFACE ${_redis_cflags})
target_link_options(redis_build_flags INTERFACE ${_redis_ldflags})

if(REDIS_DEBUG_DEFRAG STREQUAL "force")
  target_compile_definitions(redis_build_flags INTERFACE DEBUG_DEFRAG_FORCE)
elseif(REDIS_DEBUG_DEFRAG STREQUAL "fully")
  target_compile_definitions(redis_build_flags INTERFACE DEBUG_DEFRAG_FORCE DEBUG_DEFRAG_FULLY)
endif()

# Platform libraries (src/Makefile uname_S branches).
set(REDIS_PLATFORM_LIBS m)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  list(APPEND REDIS_PLATFORM_LIBS dl)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD|DragonFly|OpenBSD|NetBSD")
  list(APPEND REDIS_PLATFORM_LIBS pthread execinfo)
elseif(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  target_compile_definitions(redis_build_flags INTERFACE __EXTENSIONS__ _XPG6)
  list(APPEND REDIS_PLATFORM_LIBS dl nsl socket resolv pthread rt)
else()
  # "All the other OSes (notably Linux)": -rdynamic, -ldl -pthread -lrt
  target_link_options(redis_build_flags INTERFACE -rdynamic -pthread)
  list(APPEND REDIS_PLATFORM_LIBS dl rt)
endif()
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^armv")
  list(APPEND REDIS_PLATFORM_LIBS atomic)
endif()

# systemd
find_package(PkgConfig QUIET)
set(REDIS_SYSTEMD_LIBS "")
if(NOT REDIS_SYSTEMD STREQUAL "no")
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(LIBSYSTEMD QUIET libsystemd)
  endif()
  if(LIBSYSTEMD_FOUND)
    set(REDIS_SYSTEMD_LIBS ${LIBSYSTEMD_LINK_LIBRARIES})
  elseif(REDIS_SYSTEMD STREQUAL "yes")
    set(REDIS_SYSTEMD_LIBS systemd)
  endif()
  if(REDIS_SYSTEMD_LIBS)
    target_compile_definitions(redis_build_flags INTERFACE HAVE_LIBSYSTEMD)
  endif()
endif()

# Replication compression
set(REDIS_COMPRESSION_LIBS "")
if(REDIS_COMPRESSION)
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(LIBZSTD QUIET libzstd)
  endif()
  if(LIBZSTD_FOUND)
    set(REDIS_COMPRESSION_LIBS ${LIBZSTD_LINK_LIBRARIES})
    target_include_directories(redis_build_flags INTERFACE ${LIBZSTD_INCLUDE_DIRS})
  else()
    set(REDIS_COMPRESSION_LIBS zstd)
  endif()
  target_compile_definitions(redis_build_flags INTERFACE USE_COMPRESSION=1)
endif()

# Allocator
set(REDIS_MALLOC_LIBS "")
if(REDIS_MALLOC STREQUAL "jemalloc")
  target_compile_definitions(redis_build_flags INTERFACE USE_JEMALLOC)
  set(REDIS_MALLOC_LIBS jemalloc)
elseif(REDIS_MALLOC STREQUAL "tcmalloc")
  target_compile_definitions(redis_build_flags INTERFACE USE_TCMALLOC)
  set(REDIS_MALLOC_LIBS tcmalloc)
elseif(REDIS_MALLOC STREQUAL "tcmalloc_minimal")
  target_compile_definitions(redis_build_flags INTERFACE USE_TCMALLOC)
  set(REDIS_MALLOC_LIBS tcmalloc_minimal)
elseif(NOT REDIS_MALLOC STREQUAL "libc")
  message(FATAL_ERROR "unknown REDIS_MALLOC '${REDIS_MALLOC}'")
endif()

# TLS: USE_OPENSSL=1 (built in) or 2 (module), as in src/Makefile.
if(REDIS_BUILD_TLS STREQUAL "yes")
  target_compile_definitions(redis_build_flags INTERFACE USE_OPENSSL=1 BUILD_TLS_MODULE=0)
elseif(REDIS_BUILD_TLS STREQUAL "module")
  target_compile_definitions(redis_build_flags INTERFACE USE_OPENSSL=2)
endif()
if(REDIS_TLS)
  target_include_directories(redis_build_flags INTERFACE ${OPENSSL_INCLUDE_DIR})
endif()

if(NOT REDIS_SKIP_VEC_SETS)
  # src/Makefile adds it after TLS_MODULE_CFLAGS is taken, so redis-tls.so is built without it.
  target_compile_definitions(redis_build_flags INTERFACE
    "$<$<NOT:$<STREQUAL:$<TARGET_PROPERTY:NAME>,redis-tls>>:INCLUDE_VEC_SETS=1>")
endif()

message(STATUS "redis: malloc=${REDIS_MALLOC} tls=${REDIS_BUILD_TLS} sanitizer='${REDIS_SANITIZER}' opt='${_opt}' std=${_std} skip_vec_sets=${REDIS_SKIP_VEC_SETS}")
