#include <gtest/gtest.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "field_writer.h"
#include "record_decoder.h"
#include "test.pb.h"

TEST(Dummy, DissectRecord) {
  pb::Document doc;
  pb::Document::Links links;
  links.mutable_backward()->Add(1);
  links.mutable_backward()->Add(2);
  links.mutable_forward()->Add(3);
  links.mutable_forward()->Add(4);
  links.mutable_forward()->Add(5);

  doc.mutable_links()->Swap(&links);

  MsgWriter writer(*doc.GetDescriptor());
  DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer, 0);
  DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer, 0);
  writer.Flush(0, 0, 0);
  writer.Flush(0, 0, 0);
}

// TEST(Test, RecordDecoder) {
//   {
//     pb::Document doc;
//     RecordDecoder decoder(&doc);
//     EXPECT_FALSE(decoder.HasNext());
//   }

//   {
//     pb::Document doc;
//     doc.set_doc_id(3);
//     RecordDecoder decoder(&doc);
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(0, -1), decoder.GetNextField());
//     EXPECT_FALSE(decoder.HasNext());
//   }

//   {
//     pb::Document::Name name;

//     pb::Document doc;
//     doc.mutable_name()->Swap(&name);
//     RecordDecoder decoder(&doc);
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(2, -1), decoder.GetNextField());
//     EXPECT_FALSE(decoder.HasNext());
//   }

//   {
//     pb::Document::Links links;
//     links.mutable_backward()->Add(2);
//     links.mutable_backward()->Add(2);
//     links.mutable_backward()->Add(2);
//     links.mutable_forward()->Add(1);
//     links.mutable_forward()->Add(2);

//     RecordDecoder decoder(&links);
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(0, 0), decoder.GetNextField());
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(0, 1), decoder.GetNextField());
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(0, 2), decoder.GetNextField());
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(1, 0), decoder.GetNextField());
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(1, 1), decoder.GetNextField());

//     EXPECT_FALSE(decoder.HasNext());
//   }

//   {
//     pb::Document::Links links;
//     links.mutable_backward()->Add(2);
//     links.mutable_backward()->Add(2);
//     links.mutable_backward()->Add(2);
//     links.mutable_forward()->Add(1);
//     links.mutable_forward()->Add(2);

//     pb::Document doc;
//     doc.set_doc_id(10);
//     doc.mutable_links()->Swap(&links);

//     RecordDecoder decoder(&doc);
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(0, -1), decoder.GetNextField());
//     EXPECT_TRUE(decoder.HasNext());
//     EXPECT_EQ(std::make_pair(1, -1), decoder.GetNextField());
//     EXPECT_FALSE(decoder.HasNext());
//   }
// }
