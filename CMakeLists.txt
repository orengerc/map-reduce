#cmake_minimum_required(VERSION 3.20)
#project(OS_ex3_MapReduce)
#
#set(CMAKE_CXX_STANDARD 11)
#
#add_executable(OS_ex3_MapReduce MapReduceFramework.cpp contexts.cpp SampleClient.cpp Barrier.cpp)
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

#GTEST!!

#cmake_minimum_required(VERSION 3.20)
#project(OS_ex3_MapReduce)
#
#set(CMAKE_CXX_STANDARD 11)
#
#include(FetchContent)
#FetchContent_Declare(
#        googletest
#        # Specify the commit you depend on and update it regularly.
#        URL https://github.com/google/googletest/archive/e2239ee6043f73722e7aa812a459f54a28552929.zip
#)
## For Windows: Prevent overriding the parent project's compiler/linker settings
#set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
#FetchContent_MakeAvailable(googletest)
#
## Now simply link against gtest or gtest_main as needed. Eg
#add_executable(OS_ex3_MapReduce MapReduceFramework.cpp contexts.cpp SampleClient.cpp Barrier.cpp)
#target_link_libraries(OS_ex3_MapReduce gtest_main)


cmake_minimum_required(VERSION 3.1)


# NOTE: You can't have both ThreadSanitizer and AddressSanitizer enabled at the same time.

# Uncomment the following to enable ThreadSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")

# Uncomment the following to enable AddressSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")


# Project configuration
project(OS_ex3_MapReduce VERSION 1.0 LANGUAGES C CXX)


# Ensure system has pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(MapReduceFramework
        MapReduceClient.h
        MapReduceFramework.cpp MapReduceFramework.h contexts.h Barrier.cpp Barrier.h
        # ------------- Add your own .h/.cpp files here -------------------
        )


set_property(TARGET MapReduceFramework PROPERTY CXX_STANDARD 11)
target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra)
target_include_directories(MapReduceFramework PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# link pthreads to your framework
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

# Add tests
add_subdirectory(mattanTests)