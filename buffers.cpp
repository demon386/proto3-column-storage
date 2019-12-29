#include "buffers.h"

#include <glog/logging.h>

#include <limits>

namespace proto_column_storage {

FieldOutputBuffer::FieldOutputBuffer()
    : string_output_stream_(&buffer_data_.values),
      coded_output_stream_(&string_output_stream_) {}

void FieldOutputBuffer::WriteLevel(int repetition_level, int definition_level) {
  static constexpr int kLowest = std::numeric_limits<uint8_t>::lowest();
  static constexpr int kMax = std::numeric_limits<uint8_t>::max();

  DCHECK(repetition_level >= kLowest && repetition_level <= kMax)
      << "Only support repetition_level in range (for now): "
      << "[" << kLowest << ", " << kMax << "]";
  DCHECK(definition_level >= kLowest && repetition_level <= kMax)
      << "Only support definition_level in range (for now): "
      << "[" << kLowest << ", " << kMax << "]";
  buffer_data_.repetition_levels += static_cast<uint8_t>(repetition_level);
  buffer_data_.definition_levels += static_cast<uint8_t>(definition_level);
}

std::string FieldOutputBuffer::Serialize() {
  pb::Column column;
  *column.mutable_repetition_levels() =
      std::move(buffer_data_.repetition_levels);
  *column.mutable_definition_levels() =
      std::move(buffer_data_.definition_levels);

  std::string serialized;
  column.SerializeToString(&serialized);
  // TODO: Clean state?
  return serialized;
}

FieldInputBuffer::FieldInputBuffer(std::string serialized) {
  CHECK(column_.ParseFromString(std::move(serialized)));
}

bool FieldInputBuffer::ReadLevel(int* repetition_level, int* definition_level) {
  DCHECK_EQ(column_.repetition_levels().size(),
            column_.definition_levels().size());
  if (level_cursor_ >= column_.repetition_levels().size()) {
    return false;
  }
  *repetition_level =
      static_cast<int>(column_.repetition_levels().at(level_cursor_));
  *definition_level =
      static_cast<int>(column_.definition_levels().at(level_cursor_));
  level_cursor_++;
  return true;
}

}  // namespace proto_column_storage
