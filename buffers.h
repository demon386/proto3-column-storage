#ifndef DATA_STORE_H_
#define DATA_STORE_H_

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <map>

#include "column.pb.h"

namespace proto_column_storage {

// Wrap internal data structure into a class to avoid exposing it.
class FieldBuffer {
 protected:
  struct BufferData {
    // Byte string (uint8). Each takes one byte.
    // TODO: This is not the most optimal implementation. We should only use
    // some bits because the max repetition level can be pre-determined. This
    // also applied to |definition_levels|.
    std::string repetition_levels;
    // Byte string (uint8). Each takes one byte.
    std::string definition_levels;
    // Byte string.
    std::string values;
  };
};

class FieldOutputBuffer : protected FieldBuffer {
 public:
  FieldOutputBuffer();

  void WriteLevel(int repetition_level, int definition_level);

  google::protobuf::io::CodedOutputStream* mutable_value_output_stream();

  std::string Serialize();

 private:
  BufferData buffer_data_;
  google::protobuf::io::StringOutputStream string_output_stream_;
  google::protobuf::io::CodedOutputStream coded_output_stream_;
};

class FieldInputBuffer {
 public:
  FieldInputBuffer(std::string serialized);

  bool ReadLevel(int* repetition_level, int* definition_level);

  google::protobuf::io::CodedInputStream* mutable_value_input_stream();

 private:
  pb::Column column_;
  int level_cursor_ = 0;
};

class OutputBuffers {
 private:
  std::map<std::string, FieldBuffer> buffers_;
};

}  // namespace proto_column_storage

#endif  // DATA_STORE_H_
