# Test integration. The Tcl suites start ./src/redis-server, so they rely on
# REDIS_COPY_BINARIES_TO_SRC (default ON) during the transition.
#
#   cmake --build build/release && ctest --test-dir build/release
#   ctest --test-dir build/release -R tcl-unit --verbose

option(REDIS_BUILD_TEST_MODULES "Build tests/modules as part of the default build (as make does)" ON)

# tests/modules keeps its Makefile; it only needs src/redismodule.h.
set(_mod_flags "")
if(CMAKE_C_FLAGS MATCHES "-m32")
  set(_mod_target 32bit)
else()
  set(_mod_target all)
endif()
if(REDIS_BUILD_TEST_MODULES)
  set(_all ALL)
else()
  set(_all "")
endif()
# Same job handling as jemalloc: drop the inherited MAKEFLAGS (its jobserver may
# not be reachable from here) and pass -j explicitly.
cmake_host_system_information(RESULT _mod_jobs QUERY NUMBER_OF_LOGICAL_CORES)
set(_mod_make ${CMAKE_COMMAND} -E env --unset=MAKEFLAGS --unset=MFLAGS ${REDIS_GNU_MAKE} -j${_mod_jobs})
add_custom_target(module_tests ${_all}
  COMMAND ${_mod_make} -C ${CMAKE_SOURCE_DIR}/tests/modules ${_mod_target} CC=${CMAKE_C_COMPILER}
  COMMENT "Building tests/modules"
  USES_TERMINAL)

enable_testing()
add_test(NAME tcl-unit COMMAND ./runtest --clients 16 WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_test(NAME tcl-moduleapi COMMAND ./runtest-moduleapi --clients 16 WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_test(NAME tcl-sentinel COMMAND ./runtest-sentinel WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
set_tests_properties(tcl-unit tcl-moduleapi tcl-sentinel PROPERTIES RUN_SERIAL TRUE)
