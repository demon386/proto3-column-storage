cmake_minimum_required(VERSION 3.14)
project(proto-columns CXX)

set(CMAKE_CXX_STANDARD 14)

find_package(Protobuf REQUIRED)


enable_testing()
find_package(GTest REQUIRED)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS tests/test.proto protos/column.proto)

add_executable(tests tests/test.cpp
  tests/buffer_test.cpp
  field_writer.cpp
  buffers.cpp
  ${PROTO_SRCS})
target_include_directories(tests PRIVATE
  ./
  ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(tests GTest::GTest GTest::Main
  ${PROTO_HDRS}
  ${Protobuf_LIBRARIES}
  glog)
