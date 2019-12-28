#include "field_writer.h"

#include <glog/logging.h>

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
  if (cpp_type == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
    return std::make_unique<MsgWriter>(*field_descriptor.message_type(), number,
                                       repetition_level, definition_level,
                                       field_descriptor.name(), parent);
  } else if (cpp_type == google::protobuf::FieldDescriptor::CPPTYPE_INT64) {
    using Writer =
        AtomicWriter<google::protobuf::FieldDescriptor::CPPTYPE_INT64>;
    return std::make_unique<Writer>(number, repetition_level, definition_level,
                                    field_descriptor.name(), parent);
  } else if (cpp_type == google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
    using Writer =
        AtomicWriter<google::protobuf::FieldDescriptor::CPPTYPE_STRING>;
    return std::make_unique<Writer>(number, repetition_level, definition_level,
                                    field_descriptor.name(), parent);
  } else {
    // Not implemented yet.
    DCHECK(false);
    return nullptr;
  }
}

MsgWriter::MsgWriter(const google::protobuf::Descriptor& descriptor,
                     int field_number, int repetition_level,
                     int defintion_level, std::string field_name,
                     MsgWriter* parent)
    : FieldWriter(field_number, repetition_level, defintion_level,
                  std::move(field_name)) {
  parent_ = parent;
  const int field_count = descriptor.field_count();
  for (int i = 0; i < field_count; i++) {
    const google::protobuf::FieldDescriptor& field_descriptor =
        *descriptor.field(i);
    field_writers_[field_descriptor.number()] = MakeFieldWriter(
        field_descriptor, repetition_level, defintion_level, this);
  }
}

FieldWriter* MsgWriter::GetChild(int field_number) {
  return field_writers_.at(field_number).get();
}

void DissectRecord(std::unique_ptr<RecordDecoder> decoder, MsgWriter* writer,
                   int repetition_level) {
  writer->AddVersion(repetition_level);
  std::set<int> seen_fields;
  while (decoder->HasNext()) {
    const RecordDecoder::Position pos = decoder->GetNextField();
    DVLOG(2) << "Process field: " << pos.field->name();
    FieldWriter* child_writer = writer->GetChild(pos.field->number());
    CHECK_EQ(child_writer->field_name(), pos.field->name());
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