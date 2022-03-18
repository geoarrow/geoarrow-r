
#pragma once

#include <cstdlib>
#include <limits>
#include <cstring>
#include <algorithm>

#include "handler.hpp"
#include "common.hpp"

namespace geoarrow {

namespace builder {

template<typename BufferT, typename T = BufferT, int bitwidth = 8 * sizeof(BufferT)>
class BufferBuilder {
public:
  BufferBuilder(int64_t capacity = 1024): data_(nullptr), capacity_(capacity),
    size_(0), growth_factor_(1.5) {
    reallocate(capacity);
  }

  virtual ~BufferBuilder() {
    if (data_ != nullptr) {
      free(data_);
    }
  }

  virtual void write_element(const T& item) {
    BufferT item_buffer = item;
    write_buffer(&item_buffer, 1);
  }

  virtual BufferT* release() {
    if (size_ != capacity_) {
      memset(data_ + size_, 0, capacity_ - size_);
    }

    BufferT* out = data_;
    data_ = nullptr;
    return out;
  }

  void reserve(int64_t additional_capacity) {
    if ((size_ + additional_capacity) > capacity_) {
      int64_t target_capacity = std::max<int64_t>(
        capacity_ * growth_factor_ + 1,
        size_ + additional_capacity
      );

      reallocate(target_capacity);
    }
  }

  void reallocate(int64_t capacity) {
    int64_t n_bytes = (capacity * 8 / bitwidth) + 1;
    BufferT* new_data = reinterpret_cast<BufferT*>(realloc(data_, n_bytes));
    if (new_data == nullptr) {
      throw util::IOException(
        "Failed to allocate BufferBuilder::data_ of capacity %lld", capacity);
    }

    data_ = new_data;
    capacity_ = n_bytes * bitwidth;
  }

  void write_buffer(const BufferT* buffer, int64_t size) {
    reserve(size);
    memcpy(data_ + size_, buffer, size);
    size_ += size;
  }

  const BufferT* data() {
    return data_;
  }

  BufferT* data_at_cursor(int64_t* max_size) {
    *max_size = remaining_capacity();
    return data_ + size_;
  }

  int64_t size() {
    return size_;
  }

  int64_t remaining_capacity() {
    return capacity_ - size_;
  }

protected:
  BufferT* data_;
  int64_t capacity_;
  int64_t size_;
  double growth_factor_;
};

class BitmapBuilder: public BufferBuilder<uint8_t, bool, 1> {
public:
  BitmapBuilder(int64_t capacity, int64_t null_count_guess = 0):
    BufferBuilder<uint8_t, bool, 1>(0), null_count_(0), buffer_(0), buffer_size_(0) {
    if (null_count_guess != 0) {
      trigger_alloc(capacity);
    }
  }

  void write_element(const bool& value) {
    if (null_count_) {
      buffer_ = buffer_ | (((uint8_t) value) << buffer_size_);

      buffer_size_++;
      if (buffer_size_ == 8) {
        write_buffer(&buffer_, 1);
        buffer_size_ = 0;
      }
    }
  }

  uint8_t* release() {
    if (buffer_size_ != 0) {
      for (int i = 0; i < (8 - buffer_size_); i++) {
        write_element(false);
      }
    }

    return BufferBuilder<uint8_t, bool, 1>::release();
  }

private:
  int64_t null_count_;
  uint8_t buffer_;
  int buffer_size_;

  void trigger_alloc(int64_t logical_size) {
    int64_t physical_size = logical_size / 8 + 1;
    data_ = reinterpret_cast<uint8_t*>(malloc(physical_size));
    if (data_ == nullptr) {
      throw util::IOException(
        "Failed to allocate BitmapBuilder::data_ of capacity %lld",
          physical_size);
    }

    for (int64_t i = 0; i < physical_size; i++) {
      data_[i] = 0xff;
    }
  }
};


class ArrayBuilder {
public:
  ArrayBuilder(int64_t capacity = 1024):
    size_(0), validity_buffer_builder_(capacity) {}

  virtual ~ArrayBuilder() {}

  virtual void reserve(int64_t additional_capacity) {}

  virtual void release(struct ArrowArray* array) {
    throw util::IOException("Not implemented");
  }

protected:
  int64_t size_;
  builder::BitmapBuilder validity_buffer_builder_;
};


class StringArrayBuilder: public ArrayBuilder {
public:
  StringArrayBuilder(int64_t capacity = 1024, int64_t data_size_guess_ = 1024):
      ArrayBuilder(capacity),
      is_large_(false),
      item_size_(0),
      offset_buffer_builder_(capacity),
      large_offset_buffer_builder_(capacity),
      data_buffer_builder_(data_size_guess_) {
    reserve_data(data_size_guess_);
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

  void reserve_data(int64_t additional_data_size_guess) {
    if (needs_make_large(additional_data_size_guess)) {
      make_large();
    }
  }

  int64_t remaining_data_capacity() {
    return data_buffer_builder_.remaining_capacity();
  }

  uint8_t* data_at_cursor(int64_t* max_size) {
    return data_buffer_builder_.data_at_cursor(max_size);
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
      large_offset_buffer_builder_.write_element(item_size_);
    } else {
      offset_buffer_builder_.write_element(item_size_);
    }

    item_size_ = 0;
    validity_buffer_builder_.write_element(not_null);
  }

  void release(struct ArrowArray* array) {
    throw util::IOException("Not implemented");
  }

protected:
  bool is_large_;
  int64_t item_size_;
  builder::BufferBuilder<int32_t> offset_buffer_builder_;
  builder::BufferBuilder<int64_t> large_offset_buffer_builder_;
  builder::BufferBuilder<uint8_t> data_buffer_builder_;

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

}

class GeoArrayBuilder: public builder::ArrayBuilder, public Handler {
public:
  GeoArrayBuilder(int64_t capacity = 1024): ArrayBuilder(capacity) {}
};

}
