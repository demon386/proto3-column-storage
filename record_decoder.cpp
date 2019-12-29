#include "record_decoder.h"

namespace proto_column {

RecordDecoder::RecordDecoder(const google::protobuf::Message* message) {
  message_ = message;
  reflection_ = message_->GetReflection();
  reflection_->ListFields(*message_, &fields_);
}

bool RecordDecoder::GetNextValue(FieldCursor* field_cursor) {
  const int fields_size = fields_.size();
  if (field_pos_ >= fields_size) {
    return false;
  }
  const auto* field = fields_[field_pos_];
  const bool is_repeated = field->is_repeated();
  const int array_size =
      is_repeated ? reflection_->FieldSize(*message_, field) : 1;
  // The check of |array_pos_| is always valid here even for initial value (0).
  // Because if the field is present, then there's at least one value (including
  // repeated field).
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

}  // namespace proto_column
