#ifndef RECORD_DECODER_
#define RECORD_DECODER_

#include <utility>

#include <glog/logging.h>
#include <google/protobuf/message.h>

namespace proto_column {

class RecordDecoder {
 public:
  RecordDecoder(const google::protobuf::Message* message);

  struct FieldCursor {
    const google::protobuf::FieldDescriptor* field;
    // -1 for scalar field. Otherwise it means array position of the repeated
    // field.
    int array_pos;
  };

  // Gets next value and move forward. Returns |false| if there's no new value,
  // otherwise returns |true|.
  bool GetNextValue(FieldCursor* field_cursor);

  const google::protobuf::Message* message() { return message_; }

 private:
  int field_pos_ = 0;
  int array_pos_ = 0;
  // Not owned by this class.
  const google::protobuf::Message* message_;
  // Not owned by this class.
  const google::protobuf::Reflection* reflection_;
  // Elements are not owned by this class.
  std::vector<const google::protobuf::FieldDescriptor*> fields_;
};

}  // namespace proto_column

#endif  // RECORD_DECODER_
