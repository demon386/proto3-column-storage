#include "field_writer.h"

#include <glog/logging.h>

namespace proto_column_storage {

std::unique_ptr<FieldWriter> MakeFieldWriter(
    const google::protobuf::FieldDescriptor& field_descriptor,
    int repetition_level, int definition_level, MsgWriter* parent) {
  DVLOG(2) << "MakeFieldWriter: " << field_descriptor.name()
           << ", is_repeated: " << field_descriptor.is_repeated();
  if (field_descriptor.is_repeated()) {
    repetition_level += 1;
  }
  definition_level += 1;
  const int number = field_descriptor.number();

  const auto cpp_type = field_descriptor.cpp_type();
  switch (cpp_type) {
    case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
      return std::make_unique<MsgWriter>(
          *field_descriptor.message_type(), number, repetition_level,
          definition_level, field_descriptor.full_name(), parent);
    }
    case google::protobuf::FieldDescriptor::CPPTYPE_INT64: {
      using Writer =
          AtomicWriter<google::protobuf::FieldDescriptor::CPPTYPE_INT64>;
      return std::make_unique<Writer>(number, repetition_level,
                                      definition_level,
                                      field_descriptor.full_name(), parent);
    }
    case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
      using Writer =
          AtomicWriter<google::protobuf::FieldDescriptor::CPPTYPE_STRING>;
      return std::make_unique<Writer>(number, repetition_level,
                                      definition_level,
                                      field_descriptor.full_name(), parent);
    }
  }
  DCHECK(false) << "Not implemented yet";
  return nullptr;
}

MsgWriter::MsgWriter(const google::protobuf::Descriptor& descriptor,
                     int field_number, int repetition_level,
                     int definition_level, std::string full_field_name,
                     MsgWriter* parent)
    : FieldWriter(field_number, repetition_level, definition_level,
                  std::move(full_field_name)) {
  parent_ = parent;
  if (parent_ == nullptr) {
    output_buffers_ =
        std::make_shared<std::map<std::string, FieldOutputBuffer>>();
  } else {
    output_buffers_ = parent_->output_buffers_;
  }
  const int field_count = descriptor.field_count();
  for (int i = 0; i < field_count; i++) {
    const google::protobuf::FieldDescriptor& field_descriptor =
        *descriptor.field(i);
    field_writers_[field_descriptor.number()] = MakeFieldWriter(
        field_descriptor, repetition_level, definition_level, this);
  }
}

FieldWriter* MsgWriter::GetChild(int field_number) {
  return field_writers_.at(field_number).get();
}

void MsgWriter::AddVersion(int repetition_level) {
  MsgValue val;
  val.repetition_level = repetition_level;
  if (parent_) {
    val.parent_version = parent_->Version();
    CHECK_GE(val.parent_version, 0);
  }
  versions_.push_back(std::move(val));
}

void MsgWriter::Flush() {
  Flush(/*parent_version=*/0, /*repetition_level=*/0,
        /*def_level=*/0);
}

void MsgWriter::Flush(int parent_version, int repetition_level, int def_level) {
  bool written = false;
  while (version_cursor_ < versions_.size() &&
         (parent_ == nullptr ||
          versions_[version_cursor_].parent_version == parent_version)) {
    repetition_level = versions_[version_cursor_].repetition_level;
    def_level = definition_level();
    parent_version = version_cursor_;
    version_cursor_++;
    written = true;
    for (auto& writer : field_writers_) {
      writer.second->Flush(parent_version, repetition_level, def_level);
    }
  }
  if (!written && parent_ != nullptr) {
    parent_version = -1;
    for (auto& writer : field_writers_) {
      writer.second->Flush(parent_version, repetition_level, def_level);
    }
  }
}

void DissectRecord(std::unique_ptr<RecordDecoder> decoder, MsgWriter* writer,
                   int repetition_level) {
  writer->AddVersion(repetition_level);
  std::set<int> seen_fields;
  while (decoder->HasNext()) {
    const RecordDecoder::Position pos = decoder->GetNextField();
    DVLOG(2) << "Process field: " << pos.field->name();
    FieldWriter* child_writer = writer->GetChild(pos.field->number());
    CHECK_EQ(child_writer->field_name(), pos.field->full_name());
    DVLOG(2) << "Use writer: " << child_writer->field_name()
             << ". At repetition level: " << child_writer->repetition_level()
             << ". At definition level: " << child_writer->definition_level();

    int child_repetition_level = repetition_level;
    if (seen_fields.find(child_writer->field_number()) != seen_fields.cend()) {
      child_repetition_level = child_writer->repetition_level();
      DVLOG(2) << "Seen: " << pos.field->name() << std::endl;
      DVLOG(2) << "Change repetition level to: " << child_repetition_level;
    } else {
      seen_fields.insert(child_writer->field_number());
    }

    if (child_writer->is_atomic()) {
      dynamic_cast<AtomicWriterBase*>(child_writer)
          ->Write(*decoder->message(), *pos.field, child_repetition_level,
                  pos.array_pos);
    } else {
      const google::protobuf::Reflection& reflection =
          *decoder->message()->GetReflection();
      const google::protobuf::Message& msg =
          reflection.GetMessage(*decoder->message(), pos.field);
      DVLOG(2) << "Going deeper";
      DissectRecord(std::make_unique<RecordDecoder>(&msg),
                    dynamic_cast<MsgWriter*>(child_writer),
                    child_repetition_level);
    }
  }
}

}  // namespace proto_column_storage
