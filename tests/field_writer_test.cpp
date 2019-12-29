#include "field_writer.h"

#include <gtest/gtest.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "record_decoder.h"
#include "test.pb.h"

namespace proto_column {

TEST(FieldWriter, EmptyProto) {
  pb::Document doc;

  MsgWriter writer(*doc.GetDescriptor());
  DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer);
  writer.Flush();
}

TEST(FieldWriter, FlushTwice) {
  pb::Document doc;
  pb::Document::Links links;
  links.mutable_backward()->Add(1);
  links.mutable_forward()->Add(2);

  doc.mutable_links()->Swap(&links);

  MsgWriter writer(*doc.GetDescriptor());
  DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer);
  DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer);
  // Flush twice, the second time writes nothing.
  writer.Flush();
  writer.Flush();
}

TEST(FieldWriter, Serialize) {
  pb::Document doc;
  pb::Document::Links links;
  doc.set_doc_id(1);
  links.mutable_backward()->Add(1);
  links.mutable_backward()->Add(2);
  links.mutable_forward()->Add(3);
  links.mutable_forward()->Add(4);

  doc.mutable_links()->Swap(&links);

  MsgWriter writer(*doc.GetDescriptor());
  DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer);
  writer.Flush();

  // Verify that all atomic fields are present.
  auto* output_buffers = writer.mutable_output_buffers();
  const auto atomic_fields = {"pb.Document.doc_id",
                              "pb.Document.Links.backward",
                              "pb.Document.Links.forward",
                              "pb.Document.Name.Language.code",
                              "pb.Document.Name.Language.country",
                              "pb.Document.Name.url"};
  EXPECT_EQ(output_buffers->size(), atomic_fields.size());
  for (const std::string& field : atomic_fields) {
    EXPECT_TRUE(output_buffers->find(field) != output_buffers->cend());
  }

  // Verify atomic fields without values.
  int repetition_level;
  int definition_level;
  const auto absent_atomic_fields = {"pb.Document.Name.Language.code",
                                     "pb.Document.Name.Language.country",
                                     "pb.Document.Name.url"};
  for (const std::string& field : absent_atomic_fields) {
    auto& buffer = output_buffers->at(field);
    FieldInputBuffer input_buffer(buffer.Serialize());
    ASSERT_TRUE(input_buffer.ReadLevel(&repetition_level, &definition_level));
    EXPECT_EQ(0, repetition_level);
    EXPECT_EQ(0, definition_level);
    ASSERT_FALSE(input_buffer.ReadLevel(&repetition_level, &definition_level));
  }

  // Verify atomic fields with values.
  using Traits = ProtoTraits<google::protobuf::FieldDescriptor::CPPTYPE_INT64>;

  FieldInputBuffer doc_id_buffer(
      output_buffers->at("pb.Document.doc_id").Serialize());
  ASSERT_TRUE(doc_id_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(0, repetition_level);
  EXPECT_EQ(1, definition_level);
  EXPECT_EQ(1, Traits::Deserialize(doc_id_buffer.mutable_value_input_stream()));
  ASSERT_FALSE(doc_id_buffer.ReadLevel(&repetition_level, &definition_level));

  FieldInputBuffer backward_buffer(
      output_buffers->at("pb.Document.Links.backward").Serialize());
  ASSERT_TRUE(backward_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(0, repetition_level);
  EXPECT_EQ(2, definition_level);
  EXPECT_EQ(1,
            Traits::Deserialize(backward_buffer.mutable_value_input_stream()));
  ASSERT_TRUE(backward_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(1, repetition_level);
  EXPECT_EQ(2, definition_level);
  EXPECT_EQ(2,
            Traits::Deserialize(backward_buffer.mutable_value_input_stream()));
  ASSERT_FALSE(backward_buffer.ReadLevel(&repetition_level, &definition_level));

  FieldInputBuffer forward_buffer(
      output_buffers->at("pb.Document.Links.forward").Serialize());
  ASSERT_TRUE(forward_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(0, repetition_level);
  EXPECT_EQ(2, definition_level);
  EXPECT_EQ(3,
            Traits::Deserialize(forward_buffer.mutable_value_input_stream()));
  ASSERT_TRUE(forward_buffer.ReadLevel(&repetition_level, &definition_level));
  EXPECT_EQ(1, repetition_level);
  EXPECT_EQ(2, definition_level);
  EXPECT_EQ(4,
            Traits::Deserialize(forward_buffer.mutable_value_input_stream()));
  ASSERT_FALSE(backward_buffer.ReadLevel(&repetition_level, &definition_level));
}

}  // namespace proto_column
