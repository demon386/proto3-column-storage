#include "serialization.h"
#include "test.pb.h"

int main(int argc, char* argv[]) {
  pb::Document doc;
  // Full field name to serialized data.
  std::map<std::string, std::string> serialized = proto_column::Serialize(doc);
  for (const auto& column_and_data : serialized) {
    const std::string& serialized = column_and_data.second;
    // ...
  }

  return 0;
}
