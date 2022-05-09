cmake_minimum_required(VERSION 3.20)
project(OS_ex3_MapReduce)

set(CMAKE_CXX_STANDARD 11)

add_executable(OS_ex3_MapReduce MapReduceFramework.cpp SampleClient.cpp Barrier.cpp contexts.cpp)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
