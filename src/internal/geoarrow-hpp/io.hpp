
#pragma once

#include <cstdint>
#include <algorithm>

namespace geoarrow {

namespace io {

class IOException: public std::exception {
public:
  IOException(const char* fmt, ...) {
    memset(error_, 0, sizeof(error_));
    va_list args;
    va_start(args, fmt);
    vsnprintf(error_, sizeof(error_) - 1, fmt, args);
    va_end(args);
  }

  const char* what() const _NOEXCEPT {
    return error_;
  }

private:
  char error_[8096];
};

// The SimpleBufferSource is a wrapper around an in-memory buffer of bytes used
// as a basis for input. Rather than a base class, readers template along a class
// with an identical `fill_buffer()` method.
class SimpleBufferSource {
public:
  SimpleBufferSource(): data_(nullptr), size_(0), offset_(0) {}

  void set_buffer(const uint8_t* str, int64_t size) {
    data_ = str;
    size_ = size;
    offset_ = 0;
  }

  int64_t fill_buffer(uint8_t* buffer, int64_t max_size) {
    int64_t copy_size = std::min<int64_t>(size_ - offset_, max_size);
    if (copy_size > 0) {
      memcpy(buffer, data_ + offset_, copy_size);
      offset_ += copy_size;
      return copy_size;
    } else {
      return 0;
    }
  }

private:
  const uint8_t* data_;
  int64_t size_;
  int64_t offset_;
};

// The SimpleBufferSink is a wrapper around an in-memory buffer of bytes used
// as a basis for input. Rather than a base class, readers template along a class
// with an identical `write()` method.
class SimpleBufferSink {
public:
  SimpleBufferSink(int64_t size): data_(nullptr), size_(size), offset_(0) {
    data_ = reinterpret_cast<uint8_t*>(malloc(size));
    if (data_ == nullptr) {
      throw std::runtime_error("Failed to allocate SimpleBufferSink::data_");
    }
  }

  ~SimpleBufferSink() {
    if (data_ != nullptr) {
      free(data_);
    }
  }

  void write(uint8_t* buffer, int64_t size) {
    while ((offset_ + size) >= size_) {
      int64_t new_size = (size_ + 1) * 1.5;
      uint8_t* new_str = reinterpret_cast<uint8_t*>(realloc(data_, new_size));
      if (new_str == nullptr) {
        throw std::runtime_error("Failed to reallocate SimpleBufferSink::data_");
      }

      data_ = new_str;
      size_ = new_size;
    }

    memcpy(data_ + offset_, buffer, size);
    offset_ += size;
  }

  const uint8_t* data() {
    return data_;
  }

  uint8_t* release() {
    uint8_t* out = data_;
    data_ = nullptr;
    return out;
  }

private:
  uint8_t* data_;
  int64_t size_;
  int64_t offset_;
};

}

}
