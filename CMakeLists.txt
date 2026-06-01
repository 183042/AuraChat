cmake_minimum_required(VERSION 3.12)
project(IMServerWithAI CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package(CURL REQUIRED)
find_library(MYSQL_LIB mysqlclient REQUIRED)

include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}           # ← 添加这行！
        ${Protobuf_INCLUDE_DIRS}
        ${CURL_INCLUDE_DIRS}
        /usr/include/mysql
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS message.proto)

set(SOURCES
        src/event_loop.cpp
        src/connection.cpp
        src/worker.cpp
        src/acceptor.cpp
        src/db_writer.cpp
        src/ai_manager.cpp
        src/business.cpp
        src/main.cpp
)

add_executable(im_server ${SOURCES} ${PROTO_SRCS} ${PROTO_HDRS})

target_link_libraries(im_server
        PRIVATE
        ${Protobuf_LIBRARIES}
        ${CURL_LIBRARIES}
        ${MYSQL_LIB}
        pthread
        fmt
        spdlog
)