
#pragma once

#include <cstdlib>
#include <limits>

#include "common.hpp"
#include "array-view-base.hpp"

namespace geoarrow {

namespace builder {

template<typename BufferT>
class BufferBuilder {
public:
  BufferBuilder(int64_t size): data_(nullptr), size_(size), offset_(0) {
    if (size > 0) {
      data_ = reinterpret_cast<BufferT*>(malloc(size));
      if (data_ == nullptr) {
        throw std::runtime_error("Failed to allocate BufferBuilder::data_ of size %lld", size);
      }
    }
  }

  ~BufferBuilder() {
    if (data_ != nullptr) {
      free(data_);
    }
  }

  void write(BufferT item) {
    write(&item, 1);
  }

  void write(const BufferT* buffer, int64_t size) {
    while ((offset_ + size) >= size_) {
      int64_t new_size = (size_ + 1) * 1.5;
      T* new_str = reinterpret_cast<BufferT*>(realloc(data_, new_size));
      if (new_str == nullptr) {
        throw std::runtime_error("Failed to reallocate BufferBuilder::data_");
      }

      data_ = new_str;
      size_ = new_size;
    }

    memcpy(data_ + offset_, buffer, size);
    offset_ += size;
  }

  const BufferT* data() {
    return data_;
  }

  int64_t offset() {
    return offset_;
  }

  virtual BufferT* release() {
    if (offset_ != size_) {
      memset(data_ + offset_, 0, size_ - offset_);
    }

    T* out = data_;
    data_ = nullptr;
    return out;
  }

protected:
  BufferT* data_;
  int64_t size_;
  int64_t offset_;
};

class BitmapBuilder: public BufferBuilder<uint8_t> {
public:
  BitmapBuilder(int64_t size, int64_t null_count_guess = 0):
    BufferBuilder<uint8_t>(0), null_count_(0), buffer_(0), buffer_offset_(0) {
    if (null_count_guess != 0) {
      trigger_alloc(size);
    }
  }

  void write_bool(bool value) {
    if (null_count_) {
      buffer_ = buffer_ | (((uint8_t) value) << buffer_offset_);

      buffer_offset_++;
      if (buffer_offset_ == 8) {
        write(&buffer_, 1);
        buffer_offset_ = 0;
      }
    }
  }

  uint8_t* release() {
    if (buffer_offset_ != 0) {
      for (int i = 0; i < (8 - buffer_offset_); i++) {
        write_bool(false);
      }
    }

    return BufferBuilder<uint8_t>::release();
  }

private:
  int64_t null_count_;
  uint8_t buffer_;
  int buffer_offset_;

  void trigger_alloc(int64_t logical_size) {
    int64_t physical_size = logical_size / 8 + 1;
    data_ = reinterpret_cast<uint8_t*>(malloc(physical_size));
    if (data_ == nullptr) {
      throw util::IOException(
        "Failed to allocate BitmapBuilder::data_ of size %lld",
          physical_size);
    }

    for (int64_t i = 0; i < physical_size; i++) {
      data_[i] = 0xff;
    }
  }
};


class ArrayBuilder {
public:
  ArrayBuilder(int64_t size = 1024):
    offset_(0), validity_buffer_builder_(size) {}

  virtual void release(struct ArrowArray* array) {
    throw util::IOException("Not implemented");
  }

protected:
  int64_t offset_;
  BitmapBuilder validity_buffer_builder_;
};


class StringBuilder: public ArrayBuilder {
public:
  StringBuilder(struct ArrowSchema* schema, int64_t size = 1024,
                int64_t data_size_guess_ = 1024):
      ArrayBuilder(size),
      is_large_(false),
      item_offset_(0),
      offset_buffer_builder_(size),
      large_offset_buffer_builder_(size),
      data_buffer_builder_(data_size_guess_) {
    reserve_data(data_size_guess_);
  }

  void reserve_data(int64_t additional_data_size_guess) {
    if (needs_make_large(additional_data_size_guess)) {
      make_large();
    }
  }

  void write(const uint8_t* buffer, int64_t size) {
    if (needs_make_large(size)) {
      make_large();
    }

    data_buffer_builder_.write(buffer, size);
    item_offset_ += size;
  }

  void finish_element(bool not_null = true) {
    if (is_large_) {
      large_offset_buffer_builder_.write(item_offset_);
    } else {
      offset_buffer_builder_.write(item_offset_);
    }

    item_offset_ = 0;
    validity_buffer_builder_.write_bool(not_null);
  }

  void release(struct ArrowArray* array) {
    throw util::IOException("Not implemented");
  }

private:
  bool is_large_;
  int64_t item_offset_;
  BufferBuilder<int32_t> offset_buffer_builder_;
  BufferBuilder<int64_t> large_offset_buffer_builder_;
  BufferBuilder<uint8_t> data_buffer_builder_;

  bool needs_make_large(int64_t size) {
    return !is_large_ &&
      ((data_buffer_builder_.offset() + size) > std::numeric_limits<int32_t>::min());
  }

  void make_large() {
    large_offset_buffer_builder_ = BufferBuilder<int64_t>(offset_buffer_builder_.offset());
    for (int64_t i = 0; i < offset_buffer_builder_.offset(); i++) {
      large_offset_buffer_builder_.write(offset_buffer_builder_.data()[i]);
    }

    free(offset_buffer_builder_.release());
    is_large_ = true;
  }
};




}

}
