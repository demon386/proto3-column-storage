#ifndef PROTO_TRAITS_H_
#define PROTO_TRAITS_H_

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>

#include <string>

namespace proto_column_storage {

template <typename google::protobuf::FieldDescriptor::CppType T>
struct ProtoTraits;

template <>
struct ProtoTraits<google::protobuf::FieldDescriptor::CPPTYPE_INT64> {
  using ValueType = int64_t;
  static ValueType Read(const google::protobuf::Message& msg,
                        const google::protobuf::FieldDescriptor& field,
                        int idx) {
    const google::protobuf::Reflection* reflection = msg.GetReflection();
    if (idx == -1) {
      return reflection->GetInt64(msg, &field);
    } else {
      return reflection->GetRepeatedInt64(msg, &field, idx);
    }
  }

  static void Serialize(google::protobuf::io::CodedOutputStream* output_stream,
                        ValueType val) {
    output_stream->WriteVarint64(val);
  }

  static ValueType Deserialize(
      google::protobuf::io::CodedInputStream* input_stream) {
    uint64_t val;
    input_stream->ReadVarint64(&val);
    return static_cast<ValueType>(val);
  }
};

template <>
struct ProtoTraits<google::protobuf::FieldDescriptor::CPPTYPE_STRING> {
  using ValueType = std::string;
  static ValueType Read(const google::protobuf::Message& msg,
                        const google::protobuf::FieldDescriptor& field,
                        int idx) {
    const google::protobuf::Reflection* reflection = msg.GetReflection();
    if (idx == -1) {
      return reflection->GetString(msg, &field);
    } else {
      return reflection->GetRepeatedString(msg, &field, idx);
    }
  }

  static void Serialize(google::protobuf::io::CodedOutputStream* output_stream,
                        std::string&& val) {
    output_stream->WriteVarint32(val.size());
    output_stream->WriteString(val);
  }

  static ValueType Deserialize(
      google::protobuf::io::CodedInputStream* input_stream) {
    uint32_t size;
    input_stream->ReadVarint32(&size);
    ValueType val;
    input_stream->ReadString(&val, size);
    return val;
  }
};

}  // namespace proto_column_storage

#endif  // PROTO_TRAITS_H_
