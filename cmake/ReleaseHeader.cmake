# Generates release.h (git SHA, dirty flag, build id). Run as a script:
#   cmake -DSOURCE_DIR=<repo> -DOUTPUT=<file> -P ReleaseHeader.cmake
# The file is rewritten only when the SHA or the dirty state changes, so a
# no-op build does not recompile release.c (same rule as the former src/mkreleasehdr.sh).

execute_process(COMMAND git show-ref --head --hash=8
  WORKING_DIRECTORY ${SOURCE_DIR} OUTPUT_VARIABLE _refs ERROR_QUIET
  RESULT_VARIABLE _rc OUTPUT_STRIP_TRAILING_WHITESPACE)
set(GIT_SHA1 00000000)
if(_rc EQUAL 0 AND _refs)
  string(REGEX MATCH "^[0-9a-f]+" GIT_SHA1 "${_refs}")
endif()

execute_process(COMMAND git diff --no-ext-diff -- src deps
  WORKING_DIRECTORY ${SOURCE_DIR} OUTPUT_VARIABLE _diff ERROR_QUIET)
set(GIT_DIRTY 0)
if(_diff)
  string(REGEX MATCHALL "\n" _lines "${_diff}")
  list(LENGTH _lines GIT_DIRTY)
endif()

if(EXISTS ${OUTPUT})
  file(READ ${OUTPUT} _old)
  if(_old MATCHES "REDIS_GIT_SHA1 \"${GIT_SHA1}\"" AND _old MATCHES "REDIS_GIT_DIRTY \"${GIT_DIRTY}\"")
    return()
  endif()
endif()

# Same as mkreleasehdr.sh: "<host>-<time>", or just the timestamp when
# SOURCE_DATE_EPOCH is set, so reproducible builds do not depend on the host.
if(DEFINED ENV{SOURCE_DATE_EPOCH} AND NOT "$ENV{SOURCE_DATE_EPOCH}" STREQUAL "")
  set(_build_id "$ENV{SOURCE_DATE_EPOCH}")
else()
  cmake_host_system_information(RESULT _host QUERY HOSTNAME)
  string(TIMESTAMP _ts "%s" UTC)
  set(_build_id "${_host}-${_ts}")
endif()
file(WRITE ${OUTPUT}
"#define REDIS_GIT_SHA1 \"${GIT_SHA1}\"
#define REDIS_GIT_DIRTY \"${GIT_DIRTY}\"
#define REDIS_BUILD_ID \"${_build_id}\"
#include \"version.h\"
#define REDIS_BUILD_ID_RAW REDIS_VERSION REDIS_BUILD_ID REDIS_GIT_DIRTY REDIS_GIT_SHA1
")
