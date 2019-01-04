cmake_minimum_required(VERSION 3.13)
project(c_src)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(/opt/local/lib/erlang/erts-9.3/include)
include_directories(/opt/local/include)
include_directories(/usr/local/include/bsoncxx/v_noabi)
include_directories(/usr/local/include/mongocxx/v_noabi)


add_executable(c_src
        emongo.cpp
        emongo.h
        mongo_connection.cpp
        mongo_connection.h
        mongo_exception.h
        mongo_response.h
        mongo_api.h mongo_api.cpp)
