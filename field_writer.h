#ifndef FIELD_WRITER_
#define FIELD_WRITER_

#include <iostream>
#include <memory>
#include <vector>

#include <google/protobuf/message.h>

#include "proto_traits.h"
#include "record_decoder.h"

class FieldWriter {
 public:
  FieldWriter(int field_number, int repetition_level, int definition_level,
              std::string field_name)
      : field_number_(field_number),
        repetition_level_(repetition_level),
        definition_level_(definition_level),
        field_name_(std::move(field_name)) {}
  virtual ~FieldWriter() {}

  virtual void Flush(int parent_version, int repetition_level,
                     int def_level) = 0;

  virtual bool is_atomic() const = 0;

  int field_number() const { return field_number_; }

  int repetition_level() const { return repetition_level_; }

  int definition_level() const { return definition_level_; }

  const std::string& field_name() const { return field_name_; }

 private:
  int field_number_ = -1;
  int repetition_level_ = -1;
  int definition_level_ = -1;
  std::string field_name_;
};

class MsgWriter : public FieldWriter {
 public:
  MsgWriter(const google::protobuf::Descriptor& descriptor,
            int field_number = 0, int repetition_level = 0,
            int definition_level = 0, std::string field_name = "",
            MsgWriter* parent = nullptr);

  FieldWriter* GetChild(int field_number);

  void AddVersion(int repetition_level);

  int Version() { return versions_.size() - 1; }

  void Flush(int parent_version, int repetition_level, int def_level) override;

  bool is_atomic() const override { return false; }

 private:
  struct MsgValue {
    int parent_version;
    int repetition_level;
  };

  std::map<int, std::unique_ptr<FieldWriter>> field_writers_;
  std::vector<MsgValue> versions_;
  int version_cursor_ = 0;
  MsgWriter* parent_ = nullptr;
};

class AtomicWriterBase : public FieldWriter {
 public:
  AtomicWriterBase(int field_number, int repetition_level, int definition_level,
                   std::string field_name)
      : FieldWriter(field_number, repetition_level, definition_level,
                    std::move(field_name)) {}
  virtual void Write(const google::protobuf::Message& msg,
                     const google::protobuf::FieldDescriptor& field,
                     int curr_repetition_level, int idx) = 0;
};

template <typename google::protobuf::FieldDescriptor::CppType T>
class AtomicWriter : public AtomicWriterBase {
 public:
  AtomicWriter(int field_number, int repetition_level, int definition_level,
               std::string field_name, MsgWriter* parent)
      : AtomicWriterBase(field_number, repetition_level, definition_level,
                         std::move(field_name)) {
    DCHECK_NOTNULL(parent);
    parent_ = parent;
  }

  void Write(const google::protobuf::Message& msg,
             const google::protobuf::FieldDescriptor& field,
             int curr_repetition_level, int idx) override {
    Version version;
    version.parent_version = parent_->Version();
    version.repetition_level = curr_repetition_level;
    version.value = ProtoTraits<T>::Read(msg, field, idx);
    versions_.push_back(std::move(version));
  }

  void Flush(int parent_version, int repetition_level, int def_level) override {
    // TODO: Implement writing into a data store.
    ValueType val = ValueType();
    bool printed = false;
    while (version_cursor_ < versions_.size() &&
           versions_[version_cursor_].parent_version == parent_version) {
      repetition_level = versions_[version_cursor_].repetition_level;
      def_level = definition_level();
      val = versions_[version_cursor_].value;
      version_cursor_++;
      printed = true;
      DVLOG(2) << "Flush (int) val: " << field_name() << ", "
               << repetition_level << ", " << def_level << ", " << val;
    }
    if (!printed) {
      DVLOG(2) << "Flush (int) val: " << field_name() << ", "
               << repetition_level << ", " << def_level << ", " << val;
    }
  }

  bool is_atomic() const override { return true; }

 private:
  using ValueType = typename ProtoTraits<T>::ValueType;
  struct Version {
    int parent_version;
    int repetition_level;
    ValueType value;
  };

  MsgWriter* parent_ = nullptr;
  std::vector<Version> versions_;
  int version_cursor_ = 0;
};

void DissectRecord(std::unique_ptr<RecordDecoder> decoder, MsgWriter* writer,
                   int repetition_level);

#endif  // FIELD_WRITER_
