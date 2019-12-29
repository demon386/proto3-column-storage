#include <gtest/gtest.h>

#include "buffers.h"

using namespace proto_column_storage;

TEST(Buffers, FieldOutputBuffer) {
  FieldOutputBuffer field_output_buffer;
  field_output_buffer.WriteLevel(10, 20);
}

// Writes then reads.
TEST(Buffers, FieldBuffer) {
  FieldOutputBuffer field_output_buffer;
  field_output_buffer.WriteLevel(10, 20);
  field_output_buffer.WriteLevel(30, 40);
  std::string serialized = field_output_buffer.Serialize();

  FieldInputBuffer field_input_buffer(std::move(serialized));

  int repetition_level = 0;
  int definition_level = 0;
  ASSERT_TRUE(
      field_input_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(10, repetition_level);
  EXPECT_EQ(20, definition_level);

  ASSERT_TRUE(
      field_input_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(30, repetition_level);
  EXPECT_EQ(40, definition_level);
}
