# redis sources. Each folder under src/ has a CMakeLists.txt that calls
# redis_folder() to declare its sources:
#
#   redis_folder(<name>
#     SERVER <files>   compiled into redis-server only
#     COMMON <files>   compiled once, linked into redis-server, redis-cli and
#                      redis-benchmark (zmalloc, dict, ae, ...)
#     TOOLS  <files>   compiled once, linked into redis-cli and redis-benchmark)
#
# During the transition every folder is on the include path, so existing
# includes such as #include "server.h" resolve without edits.

set(REDIS_FOLDERS
  platform util ds net core
  types/string types/list types/hash types/set types/zset types/stream
  types/geo types/hll types/bitmap types/array types/gcra
  persistence replication cluster scripting moduleapi sentinel
  tools tools/cli tools/benchmark misc)

set(REDIS_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${REDIS_GENERATED_DIR})

# Compile environment shared by every redis source: flags, include paths and
# the bundled deps' headers (as FINAL_CFLAGS in the legacy src/Makefile).
#
# Source folders go on the quote-only path (-iquote): in the flat layout every
# redis header was found next to the including file, while <...> includes
# (e.g. redis-cli's <sds.h>, meant to be hiredis' copy) resolved through the
# deps' -I paths. -iquote keeps exactly that split.
add_library(redis_env INTERFACE)
target_link_libraries(redis_env INTERFACE redis_build_flags)
foreach(dir IN LISTS REDIS_FOLDERS)
  target_compile_options(redis_env INTERFACE "SHELL:-iquote ${CMAKE_CURRENT_SOURCE_DIR}/${dir}")
endforeach()
target_compile_options(redis_env INTERFACE
  "SHELL:-iquote ${CMAKE_CURRENT_SOURCE_DIR}" "SHELL:-iquote ${REDIS_GENERATED_DIR}")
foreach(dep hiredis linenoise lua hdr_histogram fpconv xxhash tre croaring)
  target_include_directories(redis_env INTERFACE $<TARGET_PROPERTY:${dep},INTERFACE_INCLUDE_DIRECTORIES>)
  target_compile_options(redis_env INTERFACE $<TARGET_PROPERTY:${dep},INTERFACE_COMPILE_OPTIONS>)
endforeach()
if(REDIS_MALLOC STREQUAL "jemalloc")
  target_include_directories(redis_env INTERFACE $<TARGET_PROPERTY:jemalloc,INTERFACE_INCLUDE_DIRECTORIES>)
endif()

add_library(redis_common OBJECT)
add_library(redis_tools_common OBJECT)
foreach(t redis_common redis_tools_common)
  target_link_libraries(${t} PRIVATE redis_env)
endforeach()
set_property(GLOBAL PROPERTY REDIS_SERVER_FOLDER_TARGETS "")

function(redis_folder name)
  cmake_parse_arguments(F "" "" "SERVER;COMMON;TOOLS" ${ARGN})
  if(F_SERVER)
    string(REPLACE "/" "_" tgt "redis_${name}")
    add_library(${tgt} OBJECT ${F_SERVER})
    target_link_libraries(${tgt} PRIVATE redis_env)
    if(REDIS_MALLOC STREQUAL "jemalloc")
      add_dependencies(${tgt} jemalloc_build-configure)
    endif()
    set_property(GLOBAL APPEND PROPERTY REDIS_SERVER_FOLDER_TARGETS ${tgt})
  endif()
  if(F_COMMON)
    target_sources(redis_common PRIVATE ${F_COMMON})
  endif()
  if(F_TOOLS)
    target_sources(redis_tools_common PRIVATE ${F_TOOLS})
  endif()
endfunction()

foreach(dir IN LISTS REDIS_FOLDERS)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/CMakeLists.txt)
    add_subdirectory(${dir})
  endif()
endforeach()

if(REDIS_MALLOC STREQUAL "jemalloc")
  add_dependencies(redis_common jemalloc_build-configure)
  add_dependencies(redis_tools_common jemalloc_build-configure)
endif()

# release.h: git SHA, dirty flag and build id, regenerated only when the SHA or
# dirty state changes (replaces the former src/mkreleasehdr.sh).
add_custom_target(redis_release_header
  COMMAND ${CMAKE_COMMAND} -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
          -DOUTPUT=${REDIS_GENERATED_DIR}/release.h
          -P ${CMAKE_SOURCE_DIR}/cmake/ReleaseHeader.cmake
  BYPRODUCTS ${REDIS_GENERATED_DIR}/release.h
  COMMENT "Checking release.h")
add_dependencies(redis_common redis_release_header)
add_dependencies(redis_core redis_release_header)

# commands.def is checked in; regenerate it when a command JSON file or the
# generator changes, as src/Makefile does.
# With -DLOG_REQ_RES in REDIS_CFLAGS, commands.c includes
# commands_with_reply_schema.def instead; it is not checked in, so it is always
# generated (and needs Python), like src/Makefile's COMMANDS_DEF_FILENAME.
if(REDIS_CFLAGS MATCHES "LOG_REQ_RES")
  set(_commands_def commands_with_reply_schema)
  set(_gen_commands_flags --with-reply-schema)
else()
  set(_commands_def commands)
  set(_gen_commands_flags "")
endif()
find_package(Python3 COMPONENTS Interpreter QUIET)
if(NOT Python3_FOUND AND _commands_def STREQUAL "commands_with_reply_schema")
  message(FATAL_ERROR "LOG_REQ_RES builds generate commands_with_reply_schema.def and need python3")
endif()
if(Python3_FOUND)
  file(GLOB _command_json CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/commands/*.json)
  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/core/${_commands_def}.def
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/utils/generate-command-code.py ${_gen_commands_flags}
    DEPENDS ${_command_json} ${CMAKE_SOURCE_DIR}/utils/generate-command-code.py
    COMMENT "Generating ${_commands_def}.def")
  add_custom_target(redis_commands_def DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/core/${_commands_def}.def)
  add_dependencies(redis_core redis_commands_def)
  add_dependencies(redis_tools_common redis_commands_def)
endif()

# Vector sets are compiled into redis-server (REDIS_VEC_SETS_OBJ).
if(NOT REDIS_SKIP_VEC_SETS)
  set(_vs ${CMAKE_SOURCE_DIR}/modules/vector-sets)
  add_library(redis_vector_sets OBJECT ${_vs}/hnsw.c ${_vs}/vset.c ${_vs}/vset_config.c)
  target_link_libraries(redis_vector_sets PRIVATE redis_env)
  if(REDIS_MALLOC STREQUAL "jemalloc")
    add_dependencies(redis_vector_sets jemalloc_build-configure)
  endif()
  set_property(GLOBAL APPEND PROPERTY REDIS_SERVER_FOLDER_TARGETS redis_vector_sets)
endif()

# Libraries every binary links (src/Makefile FINAL_LIBS).
set(_tls_client_libs "")
if(REDIS_TLS)
  set(_tls_client_libs hiredis_ssl)
endif()
set(_server_tls_libs "")
if(REDIS_BUILD_TLS STREQUAL "yes")
  set(_server_tls_libs hiredis_ssl)
endif()
set(_final_libs ${REDIS_MALLOC_LIBS} ${REDIS_SYSTEMD_LIBS} ${REDIS_COMPRESSION_LIBS} ${REDIS_PLATFORM_LIBS})

get_property(_server_targets GLOBAL PROPERTY REDIS_SERVER_FOLDER_TARGETS)
set(_server_objects "")
foreach(t IN LISTS _server_targets)
  list(APPEND _server_objects $<TARGET_OBJECTS:${t}>)
endforeach()

set(REDIS_SERVER_NAME redis-server${REDIS_PROG_SUFFIX})
add_executable(redis-server ${_server_objects} $<TARGET_OBJECTS:redis_common>)
set_target_properties(redis-server PROPERTIES OUTPUT_NAME ${REDIS_SERVER_NAME} LINKER_LANGUAGE C)
target_link_libraries(redis-server PRIVATE redis_build_flags
  hiredis lua hdr_histogram fpconv xxhash tre croaring ${_server_tls_libs} ${_final_libs})

add_executable(redis-cli $<TARGET_OBJECTS:redis_common> $<TARGET_OBJECTS:redis_tools_common>
  tools/cli/redis-cli.c tools/cli/cli_commands.c)
set_target_properties(redis-cli PROPERTIES OUTPUT_NAME redis-cli${REDIS_PROG_SUFFIX})
target_link_libraries(redis-cli PRIVATE redis_env hiredis linenoise hdr_histogram ${_final_libs} ${_tls_client_libs})
if(Python3_FOUND)
  add_dependencies(redis-cli redis_commands_def)
endif()

add_executable(redis-benchmark $<TARGET_OBJECTS:redis_common> $<TARGET_OBJECTS:redis_tools_common>
  tools/benchmark/redis-benchmark.c)
set_target_properties(redis-benchmark PROPERTIES OUTPUT_NAME redis-benchmark${REDIS_PROG_SUFFIX})
target_link_libraries(redis-benchmark PRIVATE redis_env hiredis hdr_histogram ${_final_libs} ${_tls_client_libs})

# redis-tls.so (BUILD_TLS=module)
if(REDIS_BUILD_TLS STREQUAL "module")
  add_library(redis-tls MODULE core/tls.c)
  set_target_properties(redis-tls PROPERTIES PREFIX "" OUTPUT_NAME redis-tls${REDIS_PROG_SUFFIX})
  target_link_libraries(redis-tls PRIVATE redis_env hiredis_ssl)
  target_compile_definitions(redis-tls PRIVATE BUILD_TLS_MODULE=2)
  add_dependencies(redis-tls redis_release_header)
  if(REDIS_MALLOC STREQUAL "jemalloc")
    add_dependencies(redis-tls jemalloc_build-configure)
  endif()
  set_target_properties(redis-tls PROPERTIES DEFINE_SYMBOL "")
endif()

# redis-sentinel, redis-check-rdb and redis-check-aof are redis-server under
# another name (src/Makefile installs copies; make install makes symlinks).
set(_aliases redis-sentinel redis-check-rdb redis-check-aof)
foreach(alias IN LISTS _aliases)
  add_custom_command(TARGET redis-server POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:redis-server>
            $<TARGET_FILE_DIR:redis-server>/${alias}${REDIS_PROG_SUFFIX})
endforeach()

# Copy binaries into src/ so ./runtest and the scripts that start
# ./src/redis-server keep working unchanged during the transition.
if(REDIS_COPY_BINARIES_TO_SRC)
  foreach(tgt redis-server redis-cli redis-benchmark)
    add_custom_command(TARGET ${tgt} POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:${tgt}> ${CMAKE_CURRENT_SOURCE_DIR}/)
  endforeach()
  foreach(alias IN LISTS _aliases)
    add_custom_command(TARGET redis-server POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:redis-server>
              ${CMAKE_CURRENT_SOURCE_DIR}/${alias}${REDIS_PROG_SUFFIX})
  endforeach()
  if(REDIS_BUILD_TLS STREQUAL "module")
    add_custom_command(TARGET redis-tls POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:redis-tls> ${CMAKE_CURRENT_SOURCE_DIR}/)
  endif()
endif()

# Install layout of `make install`: three binaries, three symlinks.
include(GNUInstallDirs)
install(TARGETS redis-server redis-cli redis-benchmark RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
foreach(alias IN LISTS _aliases)
  install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${REDIS_SERVER_NAME}
      \$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/${alias}${REDIS_PROG_SUFFIX})")
endforeach()
# `make install` has never installed redismodule.h; packaging that fails on
# unexpected files (e.g. dh_missing --fail-missing) relies on that.
option(REDIS_INSTALL_MODULE_HEADER "Install redismodule.h into <prefix>/include" OFF)
if(REDIS_INSTALL_MODULE_HEADER)
  install(FILES redismodule.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()
