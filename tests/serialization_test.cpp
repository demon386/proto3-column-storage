#include "serialization.h"

#include <gtest/gtest.h>

#include "test.pb.h"

namespace proto_column {

TEST(Serialization, Serialize) {
  pb::Document doc;
  pb::Document::Links links;
  doc.set_doc_id(1);
  links.mutable_backward()->Add(1);
  links.mutable_backward()->Add(2);
  links.mutable_forward()->Add(3);
  links.mutable_forward()->Add(4);

  doc.mutable_links()->Swap(&links);

  const std::map<std::string, std::string> serialized = Serialize(doc);
  const auto atomic_fields = {"pb.Document.doc_id",
                              "pb.Document.Links.backward",
                              "pb.Document.Links.forward",
                              "pb.Document.Name.Language.code",
                              "pb.Document.Name.Language.country",
                              "pb.Document.Name.url"};
  EXPECT_EQ(serialized.size(), atomic_fields.size());
  for (const std::string& field : atomic_fields) {
    EXPECT_TRUE(serialized.find(field) != serialized.cend());
  }
}

}  // namespace proto_column
