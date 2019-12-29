#include "buffers.h"
#include "field_writer.h"
#include "test.pb.h"

int main(int argc, char* argv[]) {
  pb::Document doc;

  proto_column::MsgWriter writer(*doc.GetDescriptor());
  proto_column::DissectRecord(
      std::make_unique<proto_column::RecordDecoder>(&doc), &writer);
  writer.Flush();

  // Get serialized columns.
  std::map<std::string, proto_column::FieldOutputBuffer>& buffers =
      *writer.mutable_output_buffers();
  for (auto& column_to_buffer : buffers) {
    const std::string serialized = column_to_buffer.second.Serialize();
    // ...
  }

  return 0;
}
