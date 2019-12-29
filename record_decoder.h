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

  bool HasNext() const {
    const int fields_size = fields_.size();
    if (field_pos_ < fields_size - 1) {
      return true;
    } else if (field_pos_ >= fields_size) {
      return false;
    } else {
      const auto* field = fields_[field_pos_];
      if (field->is_repeated()) {
        return array_pos_ < reflection_->FieldSize(*message_, field);
      } else {
        return true;
      }
    }
  }

  struct Position {
    int field_pos;
    int array_pos;
    const google::protobuf::FieldDescriptor* field;
  };

  Position GetNextField() {
    CHECK(HasNext());
    const auto* field = fields_[field_pos_];
    if (field->is_repeated()) {
      auto ret = Position{field->index(), array_pos_++, field};
      if (array_pos_ >= reflection_->FieldSize(*message_, field)) {
        field_pos_++;
        array_pos_ = 0;
      }
      return ret;
    } else {
      return Position{fields_[field_pos_++]->index(), -1, field};
    }
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
