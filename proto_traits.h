#ifndef PROTO_TRAITS_H_
#define PROTO_TRAITS_H_

#include <string>

#include <google/protobuf/message.h>

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
};

}  // namespace proto_column_storage

#endif  // PROTO_TRAITS_H_
