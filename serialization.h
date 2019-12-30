#ifndef SERIALIZATION_H_
#define SERIALIZATION_H_

#include <map>

#include <google/protobuf/message.h>

namespace proto_column {

// Serializes given message. The returned map contains all fields with atomic
// (including repeated atomic) values. Key is field full name, value is
// serialized data.
std::map<std::string, std::string> Serialize(
    const google::protobuf::Message& message);

}  // namespace proto_column

#endif  // SERIALIZATION_H_
