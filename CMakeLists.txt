cmake_minimum_required(VERSION 3.5.1)

project(raft)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS messages.proto client.proto)

add_executable(main main.cpp ${PROTO_HDRS} ${PROTO_SRCS})

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)

find_package(spdlog REQUIRED)
add_definitions(-DSPDLOG_FMT_EXTERNAL)
find_package(fmt REQUIRED)

add_subdirectory(bus)
include_directories(bus ${JSONCPP_INCLUDE_DIRS})

target_link_libraries(main ${JSONCPP_LIBRARIES} ${Protobuf_LIBRARIES}
    ${spdlog_LIBRARIES} fmt::fmt bus)

add_executable(client client.cpp ${PROTO_HDRS} ${PROTO_SRCS})
target_link_libraries(client ${JSONCPP_LIBRARIES} ${Protobuf_LIBRARIES} bus)
