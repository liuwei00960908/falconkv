cmake_minimum_required(VERSION 3.16)
project(FalconKV LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Enable PIC for all targets when Python bindings are requested
if(FALCONKV_BUILD_PYTHON)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

# Options
option(FALCONKV_BUILD_TESTS "Build tests" ON)
option(FALCONKV_BUILD_PYTHON "Build Python bindings" OFF)
option(FALCONKV_DEBUG "Debug build" OFF)
option(FALCONKV_WITH_HIXL "Build with Huawei HiXL remote-read support" OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    if(FALCONKV_DEBUG)
        set(CMAKE_BUILD_TYPE Debug)
    else()
        set(CMAKE_BUILD_TYPE Release)
    endif()
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-shadow")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

# Dependencies
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

# Find glog (required for logging)
find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
find_library(GLOG_LIB NAMES glog)
if(GLOG_LIB AND GLOG_INCLUDE_PATH)
    message(STATUS "Found glog: ${GLOG_LIB}")
    add_library(falconkv_glog INTERFACE)
    target_link_libraries(falconkv_glog INTERFACE ${GLOG_LIB})
    target_include_directories(falconkv_glog INTERFACE ${GLOG_INCLUDE_PATH})
    set(FALCONKV_HAS_GLOG TRUE)
else()
    message(WARNING "glog not found, using fallback logging")
    set(FALCONKV_HAS_GLOG FALSE)
endif()

# Find brpc (required)
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES brpc)
if(BRPC_INCLUDE_PATH AND BRPC_LIB)
    message(STATUS "Found brpc: ${BRPC_LIB}")
    set(FALCONKV_HAS_BRPC TRUE)
    # brpc static library requires these additional dependencies
    find_package(OpenSSL QUIET)
    find_library(GFLAGS_LIB NAMES gflags)
    if(NOT GFLAGS_LIB)
        find_package(gflags QUIET)
        if(gflags_FOUND)
            set(GFLAGS_LIB gflags::gflags)
        endif()
    endif()
    find_library(GLOG_LIB NAMES glog)

    # brpc also needs leveldb
    find_library(LEVELDB_LIB NAMES leveldb)

    # Create an interface library that bundles brpc + all its transitive deps
    add_library(falconkv_brpc_deps INTERFACE)
    target_link_libraries(falconkv_brpc_deps INTERFACE
        ${BRPC_LIB}
        ${Protobuf_LIBRARIES}
        ${GLOG_LIB}
        ${GFLAGS_LIB}
    )
    if(LEVELDB_LIB)
        target_link_libraries(falconkv_brpc_deps INTERFACE ${LEVELDB_LIB})
    endif()
    if(OpenSSL_FOUND)
        target_link_libraries(falconkv_brpc_deps INTERFACE OpenSSL::SSL OpenSSL::Crypto)
    endif()
    target_include_directories(falconkv_brpc_deps INTERFACE ${BRPC_INCLUDE_PATH})
else()
    message(WARNING "brpc not found, building without RPC support")
    set(FALCONKV_HAS_BRPC FALSE)
endif()

# Find jsoncpp (required)
find_path(JSONCPP_INCLUDE_PATH NAMES json/json.h PATH_SUFFIXES jsoncpp)
find_library(JSONCPP_LIB NAMES jsoncpp)
if(JSONCPP_INCLUDE_PATH AND JSONCPP_LIB)
    message(STATUS "Found jsoncpp: ${JSONCPP_LIB}")
else()
    message(FATAL_ERROR "jsoncpp is required but not found")
endif()

# Find liburing (optional, for io_uring support)
find_path(LIBURING_INCLUDE_DIR NAMES liburing.h)
find_library(LIBURING_LIBRARY NAMES uring)
if(LIBURING_INCLUDE_DIR AND LIBURING_LIBRARY)
    set(FALCONKV_HAS_IOURING TRUE)
    message(STATUS "Found liburing: ${LIBURING_LIBRARY}")
else()
    set(FALCONKV_HAS_IOURING FALSE)
    message(STATUS "liburing not found, io_uring disabled")
endif()

# Find Huawei HiXL (optional, disabled by default so non-Ascend hosts build normally)
if(FALCONKV_WITH_HIXL)
    find_path(HIXL_INCLUDE_DIR NAMES hixl/hixl.h)
    find_library(HIXL_LIBRARY NAMES cann_hixl)
    find_library(ACL_RT_LIBRARY NAMES acl_rt)
    find_library(ASCEND_HAL_LIBRARY NAMES ascend_hal)
    find_library(METADEF_LIBRARY NAMES metadef)
    if(HIXL_INCLUDE_DIR AND HIXL_LIBRARY AND ACL_RT_LIBRARY AND ASCEND_HAL_LIBRARY AND METADEF_LIBRARY)
        set(FALCONKV_HAS_HIXL TRUE)
        message(STATUS "Found HiXL: ${HIXL_LIBRARY}")
        add_library(falconkv_hixl_deps INTERFACE)
        target_include_directories(falconkv_hixl_deps INTERFACE ${HIXL_INCLUDE_DIR})
        target_link_libraries(falconkv_hixl_deps INTERFACE
            ${HIXL_LIBRARY}
            ${ACL_RT_LIBRARY}
            ${ASCEND_HAL_LIBRARY}
            ${METADEF_LIBRARY}
        )
    else()
        message(FATAL_ERROR "FALCONKV_WITH_HIXL=ON requires hixl/hixl.h, libcann_hixl, acl_rt, ascend_hal, and metadef")
    endif()
else()
    set(FALCONKV_HAS_HIXL FALSE)
endif()

# Find GTest
if(FALCONKV_BUILD_TESTS)
    find_package(GTest QUIET)
    if(NOT GTest_FOUND)
        message(STATUS "GTest not found, will fetch via FetchContent")
    endif()
endif()

# Protobuf generation
set(PROTO_SRC_DIR ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

file(GLOB PROTO_FILES "${PROTO_SRC_DIR}/*.proto")
foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    set(PROTO_SRC ${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc)
    set(PROTO_HDR ${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h)
    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --cpp_out=${PROTO_GEN_DIR} -I${PROTO_SRC_DIR} ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating protobuf sources for ${PROTO_NAME}"
    )
    list(APPEND PROTO_SRCS ${PROTO_SRC})
    list(APPEND PROTO_HDRS ${PROTO_HDR})
endforeach()

# Protobuf generated library
add_library(falconkv_proto ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(falconkv_proto PUBLIC ${PROTO_GEN_DIR} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(falconkv_proto PUBLIC ${Protobuf_LIBRARIES})

# Common library
add_subdirectory(src/common)

# Module libraries
add_subdirectory(src/store)
add_subdirectory(src/transfer)
add_subdirectory(src/meta)
add_subdirectory(src/scheduler)
add_subdirectory(src/client)

# Python bindings
if(FALCONKV_BUILD_PYTHON)
    find_package(Python3 REQUIRED COMPONENTS Development)
    add_subdirectory(python/_pyfalconkv_internal)
endif()

# Tests
if(FALCONKV_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
