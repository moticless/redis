# Bundled dependencies. Each target uses the same sources and flags as the
# per-dependency Makefiles under deps/, plus the user's CFLAGS (sanitizers,
# -m32, ...) via REDIS_DEPS_C_FLAGS. Upstream code under deps/ is not modified.

separate_arguments(_deps_flags UNIX_COMMAND "${REDIS_DEPS_C_FLAGS}")
separate_arguments(_deps_ldflags UNIX_COMMAND "${REDIS_DEPS_LD_FLAGS}")

function(redis_dep name)
  cmake_parse_arguments(D "" "" "SOURCES;FLAGS;DEFS;INCLUDES;PUBLIC_INCLUDES" ${ARGN})
  add_library(${name} STATIC ${D_SOURCES})
  target_compile_options(${name} PRIVATE ${D_FLAGS} ${_deps_flags})
  target_compile_definitions(${name} PRIVATE ${D_DEFS})
  target_include_directories(${name} PRIVATE ${D_INCLUDES})
  target_include_directories(${name} SYSTEM INTERFACE ${D_PUBLIC_INCLUDES})
  target_link_options(${name} INTERFACE ${_deps_ldflags})
endfunction()

# hiredis (deps/hiredis/Makefile: OBJ, REAL_CFLAGS incl. its -std=c99 -pedantic -Werror)
separate_arguments(_hiredis_opt UNIX_COMMAND "${REDIS_HIREDIS_OPTIMIZATION}")
# deps/hiredis/Makefile: -pedantic everywhere except FreeBSD with a non-GCC compiler.
set(_hiredis_pedantic -pedantic)
if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" AND NOT CMAKE_C_COMPILER_ID STREQUAL "GNU")
  set(_hiredis_pedantic "")
endif()
set(_hiredis_flags -std=c99 ${_hiredis_pedantic} ${_hiredis_opt} -fPIC -Wall -Wextra -Werror -Wstrict-prototypes
    -Wwrite-strings -Wno-missing-field-initializers -g -ggdb)
if(REDIS_TLS)
  # deps/Makefile builds hiredis with USE_SSL=1 for BUILD_TLS=yes|module.
  list(APPEND _hiredis_flags -DHIREDIS_TEST_SSL)
endif()
redis_dep(hiredis
  SOURCES hiredis/alloc.c hiredis/net.c hiredis/hiredis.c hiredis/sds.c
          hiredis/async.c hiredis/read.c hiredis/sockcompat.c
  FLAGS ${_hiredis_flags}
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/hiredis)
if(REDIS_TLS)
  redis_dep(hiredis_ssl
    SOURCES hiredis/ssl.c
    FLAGS ${_hiredis_flags}
    INCLUDES ${OPENSSL_INCLUDE_DIR}
    PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/hiredis)
  target_link_libraries(hiredis_ssl INTERFACE OpenSSL::SSL OpenSSL::Crypto)
endif()

# linenoise (redis-cli only)
redis_dep(linenoise
  SOURCES linenoise/linenoise.c
  FLAGS -Wall -Os -g
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/linenoise)

# lua (deps/Makefile LUA_CFLAGS; deps/lua/src/Makefile CORE_O + LIB_O)
set(_lua_src fpconv.c lapi.c lcode.c ldebug.c ldo.c ldump.c lfunc.c lgc.c llex.c
    lmem.c lobject.c lopcodes.c lparser.c lstate.c lstring.c ltable.c ltm.c
    lundump.c lvm.c lzio.c strbuf.c lauxlib.c lbaselib.c ldblib.c liolib.c
    lmathlib.c loslib.c ltablib.c lstrlib.c loadlib.c linit.c lua_cjson.c
    lua_struct.c lua_cmsgpack.c lua_bit.c)
list(TRANSFORM _lua_src PREPEND lua/src/)
if(REDIS_LUA_DEBUG)
  set(_lua_opt -O0 -g)
  set(_lua_defs LUA_USE_APICHECK)
else()
  set(_lua_opt -O2)
endif()
redis_dep(lua
  SOURCES ${_lua_src}
  FLAGS -Wall ${_lua_opt}
  DEFS LUA_ANSI ENABLE_CJSON_GLOBAL REDIS_STATIC= LUA_USE_MKSTEMP ${_lua_defs}
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/lua/src)

# hdr_histogram (allocates through redis' zmalloc)
redis_dep(hdr_histogram
  SOURCES hdr_histogram/hdr_histogram.c
  FLAGS -std=c99 -Wall -O3 -g
  DEFS HDR_MALLOC_INCLUDE="hdr_redis_malloc.h"
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/hdr_histogram)
# deps/hdr_histogram/Makefile: GCC 4.8's intrinsic headers need global AVX flags,
# so it uses the scalar fallback there.
if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND CMAKE_C_COMPILER_VERSION VERSION_GREATER_EQUAL 4.8
   AND CMAKE_C_COMPILER_VERSION VERSION_LESS 4.9)
  target_compile_definitions(hdr_histogram PRIVATE HDR_DISABLE_AVX2)
endif()

redis_dep(fpconv
  SOURCES fpconv/fpconv_dtoa.c
  FLAGS -Wall -O3 -g
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/fpconv)

set(_tre_src regcomp.c regerror.c regexec.c tre-ast.c tre-compile.c tre-filter.c
    tre-match-backtrack.c tre-match-parallel.c tre-mem.c tre-parse.c tre-stack.c
    xmalloc.c)
list(TRANSFORM _tre_src PREPEND tre/lib/)
redis_dep(tre
  SOURCES ${_tre_src}
  FLAGS -std=c99 -Wall -Os -g
  DEFS TRE_REGEX_T_FIELD=value
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/tre/local_includes ${CMAKE_CURRENT_SOURCE_DIR}/tre/lib)
# Quote-only include path: tre ships a regex.h that must not shadow <regex.h>.
target_compile_options(tre INTERFACE -iquote ${CMAKE_CURRENT_SOURCE_DIR}/tre/local_includes)

# deps/Makefile passes CFLAGS="-fPIC $(CFLAGS)" on the command line, which
# replaces xxhash's own "CFLAGS ?= -O3": today xxhash is built without -O.
# Kept identical here; optimizing it is a separate change.
redis_dep(xxhash
  SOURCES xxhash/xxhash.c
  FLAGS -fPIC
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/xxhash)

set(_roaring_src isadetection.c array_util.c bitset_util.c art/art.c bitset.c
    containers/array.c containers/bitset.c containers/containers.c
    containers/convert.c containers/mixed_intersection.c containers/mixed_union.c
    containers/mixed_equal.c containers/mixed_subset.c containers/mixed_negation.c
    containers/mixed_xor.c containers/mixed_andnot.c containers/run.c memory.c
    roaring.c roaring64.c roaring_priority_queue.c roaring_array.c)
list(TRANSFORM _roaring_src PREPEND croaring/src/)
redis_dep(croaring
  SOURCES ${_roaring_src}
  FLAGS -std=gnu11 -Wall -O3 -g -fPIC -fvisibility=hidden
  DEFS ROARING_DISABLE_X64=1 DISABLENEON=1
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/croaring/include ${CMAKE_CURRENT_SOURCE_DIR}/croaring/src
  PUBLIC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/croaring/include)

# jemalloc keeps its autoconf build, configured out of tree in the build dir
# with the same options as deps/Makefile.
if(REDIS_MALLOC STREQUAL "jemalloc")
  include(ExternalProject)
  set(_je_build ${CMAKE_CURRENT_BINARY_DIR}/jemalloc)
  # deps/Makefile: JEMALLOC_CFLAGS=$(ENABLE_LTO) $(CFLAGS)
  set(_je_cflags "${REDIS_LTO_FLAGS} ${CMAKE_C_FLAGS}")
  set(_je_ldflags "${REDIS_LTO_FLAGS} ${CMAKE_EXE_LINKER_FLAGS}")
  # jemalloc is the longest single dependency, so build it in parallel. Pass -j
  # explicitly: Ninja has no jobserver, and under Makefiles generators the
  # jobserver does not reach commands run through ExternalProject's log wrapper.
  cmake_host_system_information(RESULT _je_jobs QUERY NUMBER_OF_LOGICAL_CORES)
  set(_je_make ${CMAKE_COMMAND} -E env --unset=MAKEFLAGS --unset=MFLAGS ${REDIS_GNU_MAKE} -j${_je_jobs})
  ExternalProject_Add(jemalloc_build
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
    BINARY_DIR ${_je_build}
    CONFIGURE_COMMAND ${CMAKE_COMMAND} -E env
        CC=${CMAKE_C_COMPILER} "CFLAGS=${_je_cflags}" "LDFLAGS=${_je_ldflags}"
        ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc/configure
        --disable-cxx --with-version=5.3.0-0-g0 --with-lg-quantum=3
        --disable-cache-oblivious --with-jemalloc-prefix=je_
    BUILD_COMMAND ${_je_make} lib/libjemalloc.a
    INSTALL_COMMAND ""
    # Compiling redis sources only needs the headers that configure generates;
    # only the link needs libjemalloc.a (via the imported target below).
    STEP_TARGETS configure
    BUILD_BYPRODUCTS ${_je_build}/lib/libjemalloc.a
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_OUTPUT_ON_FAILURE ON)
  # Makefile generators give the step target its own copy of the configure
  # rule; without this ordering both copies run configure concurrently.
  add_dependencies(jemalloc_build jemalloc_build-configure)
  file(MAKE_DIRECTORY ${_je_build}/include)
  add_library(jemalloc STATIC IMPORTED GLOBAL)
  set_target_properties(jemalloc PROPERTIES
    IMPORTED_LOCATION ${_je_build}/lib/libjemalloc.a
    INTERFACE_INCLUDE_DIRECTORIES ${_je_build}/include)
  add_dependencies(jemalloc jemalloc_build)
endif()
