
#pragma once

#include <sstream>

#include "builder.hpp"

namespace arrow {

namespace hpp {

namespace builder {

template <typename ChildBuilderT>
class FixedSizeListArrayBuilder: public ArrayBuilder {
public:
  FixedSizeListArrayBuilder(int64_t item_size = -1): item_size_(item_size) {}

  ChildBuilderT& child() { return child_builder_; }

  void set_item_size(int64_t item_size) {
    if (item_size_ == -1) {
        item_size_ = item_size;
    } else {
        throw util::Exception(
            "Attempt to resize a fixed-size list from %lld to %lld",
            item_size_, item_size);
    }
  }

  int64_t item_size() { return item_size_; }

  void finish_elements(int64_t n, bool not_null = true) {
    size_ += n;
    validity_buffer_builder_.write_elements(n, not_null);
  }

  void shrink() {
    ArrayBuilder::shrink();
    child_builder_.shrink();
  }

  void reserve(int64_t additional_capacity) {
    ArrayBuilder::reserve(additional_capacity);
    child_builder_.reserve(additional_capacity * item_size_);
  }

  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    CArrayFinalizer finalizer;
    finalizer.allocate(1, 1);
    finalizer.set_schema_format(get_format());
    finalizer.set_schema_name(name().c_str());
    finalizer.set_schema_metadata(metadata_names_, metadata_values_);

    finalizer.array_data.length = size();
    finalizer.array_data.null_count = validity_buffer_builder_.null_count();
    finalizer.array_data.buffers[0] = validity_buffer_builder_.release();

    child_builder_.release(
        finalizer.array_data.children[0],
        finalizer.schema->children[0]);

    finalizer.release(array_data, schema);
  }

  const char* get_format() {
      memset(format_, 0, sizeof(format_));
      snprintf(format_, sizeof(format_), "+w:%lld", item_size_);
      return format_;
  }

protected:
  int64_t item_size_;
  ChildBuilderT child_builder_;

private:
  char format_[128];
};

template <typename ChildBuilderT>
class ListArrayBuilder: public ArrayBuilder {
public:
  ListArrayBuilder() {
    offset_buffer_builder_.write_element(0);
  }

  ChildBuilderT& child() { return child_builder_; }

  void finish_element(bool not_null = true) {
    size_ += 1;
    validity_buffer_builder_.write_element(not_null);
    offset_buffer_builder_.write_element(child_builder_.size());
  }

  void shrink() {
    ArrayBuilder::shrink();
    child_builder_.shrink();
    offset_buffer_builder_.shrink();
  }

  void reserve(int64_t additional_capacity) {
    ArrayBuilder::reserve(additional_capacity);
    offset_buffer_builder_.reserve(additional_capacity);
  }

  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    CArrayFinalizer finalizer;
    finalizer.allocate(2, 1);
    finalizer.set_schema_format(get_format());
    finalizer.set_schema_name(name().c_str());
    finalizer.set_schema_metadata(metadata_names_, metadata_values_);

    finalizer.array_data.length = size();
    finalizer.array_data.null_count = validity_buffer_builder_.null_count();
    finalizer.array_data.buffers[0] = validity_buffer_builder_.release();
    finalizer.array_data.buffers[1] = offset_buffer_builder_.release();

    child_builder_.release(
        finalizer.array_data.children[0],
        finalizer.schema->children[0]);

    finalizer.release(array_data, schema);
  }

  const char* get_format() {
      return "+l";
  }

protected:
  builder::BufferBuilder<int32_t> offset_buffer_builder_;
  ChildBuilderT child_builder_;
};

}

}

}
