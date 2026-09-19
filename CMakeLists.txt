cmake_minimum_required(VERSION 3.14)
project(concurrent_kv_store LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
option(KV_SANITIZE "Build with -fsanitize=<thread|address>" "")

find_package(Threads REQUIRED)

add_library(kvcore STATIC src/store.cpp src/protocol.cpp src/aof.cpp src/server.cpp)
target_include_directories(kvcore PUBLIC src)
target_link_libraries(kvcore PUBLIC Threads::Threads)

function(kv_flags tgt)
  target_compile_options(${tgt} PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic -Wshadow -O2>)
  if(KV_SANITIZE)
    target_compile_options(${tgt} PRIVATE -fsanitize=${KV_SANITIZE} -g -O1 -fno-omit-frame-pointer)
    target_link_options(${tgt} PRIVATE -fsanitize=${KV_SANITIZE})
  endif()
endfunction()
kv_flags(kvcore)

add_executable(kvserver src/main.cpp)
target_link_libraries(kvserver PRIVATE kvcore)
kv_flags(kvserver)

add_executable(kvbench bench/kvbench.cpp)
target_link_libraries(kvbench PRIVATE Threads::Threads)
kv_flags(kvbench)

enable_testing()
foreach(t store protocol aof server)
  add_executable(test_${t} tests/test_${t}.cpp)
  target_link_libraries(test_${t} PRIVATE kvcore)
  kv_flags(test_${t})
  add_test(NAME ${t} COMMAND test_${t})
  set_tests_properties(${t} PROPERTIES TIMEOUT 120)
endforeach()
