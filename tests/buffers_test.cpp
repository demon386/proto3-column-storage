#include "buffers.h"

#include <gtest/gtest.h>

#include "proto_traits.h"

namespace proto_column_storage {

TEST(Buffers, FieldOutputBuffer) {
  FieldOutputBuffer field_output_buffer;
  field_output_buffer.WriteLevel(10, 20);
}

// Writes then reads.
TEST(Buffers, FieldBufferLevlsAndValues) {
  using Traits = ProtoTraits<google::protobuf::FieldDescriptor::CPPTYPE_STRING>;

  FieldOutputBuffer field_output_buffer;
  field_output_buffer.WriteLevel(10, 20);
  Traits::Serialize(field_output_buffer.mutable_value_output_stream(), "hello");
  field_output_buffer.WriteLevel(30, 40);
  Traits::Serialize(field_output_buffer.mutable_value_output_stream(), "world");
  std::string serialized = field_output_buffer.Serialize();

  FieldInputBuffer field_input_buffer(std::move(serialized));

  int repetition_level = 0;
  int definition_level = 0;
  ASSERT_TRUE(
      field_input_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(10, repetition_level);
  EXPECT_EQ(20, definition_level);
  EXPECT_EQ("hello", Traits::Deserialize(
                         field_input_buffer.mutable_value_input_stream()));

  ASSERT_TRUE(
      field_input_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(30, repetition_level);
  EXPECT_EQ(40, definition_level);
  EXPECT_EQ("world", Traits::Deserialize(
                         field_input_buffer.mutable_value_input_stream()));
}

}  // namespace proto_column_storage
