#ifndef RECORD_DECODER_
#define RECORD_DECODER_

#include <utility>

#include <glog/logging.h>
#include <google/protobuf/message.h>

namespace proto_column {

class RecordDecoder {
 public:
  RecordDecoder(const google::protobuf::Message* message) {
    message_ = message;
    reflection_ = message_->GetReflection();
    reflection_->ListFields(*message_, &fields_);
  }

  struct FieldCursor {
    const google::protobuf::FieldDescriptor* field;
    // -1 for scalar field. Otherwise it means array position of the repeated
    // field.
    int array_pos;
  };

  // Gets next value and move forward. Returns |false| if there's no new value,
  // otherwise returns |true|.
  bool GetNextValue(FieldCursor* field_cursor) {
    const int fields_size = fields_.size();
    if (field_pos_ >= fields_size) {
      return false;
    }
    const auto* field = fields_[field_pos_];
    const bool is_repeated = field->is_repeated();
    const int array_size =
        is_repeated ? reflection_->FieldSize(*message_, field) : 1;
    // The initial value |array_pos_| is always valid here for repeated fields.
    // Because if the field is present, then there's at least one value.
    DCHECK_LT(array_pos_, array_size);
    field_cursor->field = field;
    if (field->is_repeated()) {
      field_cursor->array_pos = array_pos_;
    } else {
      field_cursor->array_pos = -1;
    }

    // Forward the cursors.
    if (array_pos_ == array_size - 1) {
      field_pos_++;
      array_pos_ = 0;
    } else {
      array_pos_++;
    }

    return true;
  }

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
