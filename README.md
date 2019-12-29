# Protobuf 3 column storage (Dremel) in C++14

Note: This is still WIP.

This is an ongoing effort to implement Protobuf column storage in C++. The format and algorithm are illustrated in [Google's Dremel paper](https://research.google/pubs/pub36632/). [Parquet](https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html) adapts the same idea.

Required libraries:
- Protobuf 3
- Google test
- Glog

Serialize a Proto into column format:
```
pb::Document doc;

proto_column::MsgWriter writer(*doc.GetDescriptor());
proto_column::DissectRecord(std::make_unique<RecordDecoder>(&doc), &writer);
writer.Flush();

// Get output
const std::map<std::string, proto_column::FieldOutputBuffer> buffers = writer->mutable_output_buffers();
for (auto& column_to_buffer : buffers) {
    const std::string serialized = column_to_buffer.second.Serialize();    
    ...
}
```
