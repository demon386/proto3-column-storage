#include "serialization.h"

#include "field_writer.h"

namespace proto_column {

std::map<std::string, std::string> Serialize(
    const google::protobuf::Message& message) {
  proto_column::MsgWriter writer(*message.GetDescriptor());
  proto_column::DissectRecord(
      std::make_unique<proto_column::RecordDecoder>(&message), &writer);
  writer.Flush();

  // Get serialized columns.
  std::map<std::string, proto_column::FieldOutputBuffer>& buffers =
      *writer.mutable_output_buffers();

  std::map<std::string, std::string> output;
  for (auto& column_and_buffer : buffers) {
    std::string serialized = column_and_buffer.second.Serialize();
    output[column_and_buffer.first] = std::move(serialized);
  }
  return output;
}

}  // namespace proto_column
