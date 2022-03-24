
#pragma once

#include "builder.hpp"

namespace arrow {

namespace hpp {

namespace builder {

class BinaryArrayBuilder: public ArrayBuilder {
public:
  BinaryArrayBuilder(int64_t capacity = 1024, int64_t data_size_guess = 1024):
      ArrayBuilder(capacity),
      is_large_(false),
      item_size_(0),
      offset_buffer_builder_(capacity),
      large_offset_buffer_builder_(capacity),
      data_buffer_builder_(data_size_guess) {
    if (is_large_) {
      large_offset_buffer_builder_.write_element(0);
    } else {
      offset_buffer_builder_.write_element(0);
    }
  }

  void reserve(int64_t additional_capacity) {
    if (is_large_) {
      large_offset_buffer_builder_.reserve(additional_capacity);
    } else if (needs_make_large(additional_capacity)) {
      make_large();
      reserve(additional_capacity);
    } else {
      offset_buffer_builder_.reserve(additional_capacity);
    }
  }

  void shrink() {
    ArrayBuilder::shrink();

    if (is_large_) {
      large_offset_buffer_builder_.shrink();
    } else {
      offset_buffer_builder_.shrink();
    }

    data_buffer_builder_.shrink();
  }

  void reserve_data(int64_t additional_data_size_guess) {
    if (needs_make_large(additional_data_size_guess)) {
      make_large();
    }

    data_buffer_builder_.reserve(additional_data_size_guess);
  }

  int64_t remaining_data_capacity() {
    return data_buffer_builder_.remaining_capacity();
  }

  uint8_t* mutable_data() {
    return data_buffer_builder_.mutable_data();
  }

  uint8_t* data_at_cursor() {
    return data_buffer_builder_.data_at_cursor();
  }

  void advance_data(int64_t n) {
    data_buffer_builder_.advance(n);
  }

  int64_t data_size() {
    return data_buffer_builder_.size();
  }

  void write_buffer(const uint8_t* buffer, int64_t capacity) {
    if (needs_make_large(capacity)) {
      make_large();
    }

    data_buffer_builder_.write_buffer(buffer, capacity);
    item_size_ += capacity;
  }

  void finish_element(bool not_null = true) {
    if (is_large_) {
      large_offset_buffer_builder_.write_element(data_buffer_builder_.size());
    } else {
      offset_buffer_builder_.write_element(data_buffer_builder_.size());
    }

    item_size_ = 0;
    validity_buffer_builder_.write_element(not_null);
    size_++;
  }

  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    CArrayFinalizer finalizer;
    finalizer.allocate(3);

    finalizer.set_schema_format(get_format());

    finalizer.array_data.buffers[0] = validity_buffer_builder_.release();
    finalizer.array_data.buffers[2] = data_buffer_builder_.release();
    if (is_large_) {
      finalizer.array_data.buffers[1] = large_offset_buffer_builder_.release();
    } else {
      finalizer.array_data.buffers[1] = offset_buffer_builder_.release();
    }

    finalizer.set_schema_name(name().c_str());
    finalizer.array_data.null_count = validity_buffer_builder_.null_count();
    finalizer.array_data.length = size_;

    finalizer.release(array_data, schema);
  }

protected:
  bool is_large_;
  int64_t item_size_;
  builder::BufferBuilder<int32_t> offset_buffer_builder_;
  builder::BufferBuilder<int64_t> large_offset_buffer_builder_;
  builder::BufferBuilder<uint8_t> data_buffer_builder_;

  virtual const char* get_format() {
    if (is_large_) {
      return "Z";
    } else {
      return "z";
    }
  }

  bool needs_make_large(int64_t capacity) {
    return !is_large_ &&
      ((data_buffer_builder_.size() + capacity) > std::numeric_limits<int32_t>::max());
  }

  void make_large() {
    for (int64_t i = 0; i < offset_buffer_builder_.size(); i++) {
      large_offset_buffer_builder_.write_element(offset_buffer_builder_.data()[i]);
    }

    free(offset_buffer_builder_.release());
    is_large_ = true;
  }
};

class StringArrayBuilder: public BinaryArrayBuilder {
public:
  StringArrayBuilder(int64_t capacity = 1024, int64_t data_size_guess = 1024)
    : BinaryArrayBuilder(capacity, data_size_guess) {}

protected:

  const char* get_format() {
    if (is_large_) {
      return "U";
    } else {
      return "u";
    }
  }

};

}

}

}
