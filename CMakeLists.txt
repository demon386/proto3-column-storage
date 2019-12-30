cmake_minimum_required(VERSION 3.12.4)
project(proto-columns CXX)

set(CMAKE_CXX_STANDARD 14)

find_package(Protobuf REQUIRED)


enable_testing()
find_package(GTest REQUIRED)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS protos/column.proto)
protobuf_generate_cpp(TEST_PROTO_SRCS TEST_PROTO_HDRS tests/test.proto)

add_library(proto_column
  ${PROTO_SRCS}
  buffers.cpp
  field_writer.cpp
  record_decoder.cpp
  serialization.cpp)
target_link_libraries(proto_column
  ${PROTO_HDRS}
  ${Protobuf_LIBRARIES}
  glog)

target_include_directories(proto_column PUBLIC
  ./
  ${CMAKE_CURRENT_BINARY_DIR})

add_executable(demo
  ${TEST_PROTO_SRCS}
  demo.cpp)
target_link_libraries(demo
  proto_column
  ${TEST_PROTO_HDRS})

add_executable(tests
  ${TEST_PROTO_SRCS}
  tests/buffers_test.cpp
  tests/field_writer_test.cpp
  tests/serialization_test.cpp)
target_link_libraries(tests
  ${TEST_PROTO_HDRS}
  GTest::GTest
  GTest::Main
  proto_column)
