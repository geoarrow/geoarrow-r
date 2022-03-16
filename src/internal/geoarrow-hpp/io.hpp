
#pragma once

#include <cstdint>
#include <algorithm>


namespace geoarrow {

namespace io {

template<typename T>
class SimpleBufferSink {
public:
  SimpleBufferSink(int64_t size): data_(nullptr), size_(size), offset_(0) {
    data_ = reinterpret_cast<T*>(malloc(size));
    if (data_ == nullptr) {
      throw std::runtime_error("Failed to allocate SimpleBufferSink::data_");
    }
  }

  ~SimpleBufferSink() {
    if (data_ != nullptr) {
      free(data_);
    }
  }

  void write(T* buffer, int64_t size) {
    while ((offset_ + size) >= size_) {
      int64_t new_size = (size_ + 1) * 1.5;
      T* new_str = reinterpret_cast<T*>(realloc(data_, new_size));
      if (new_str == nullptr) {
        throw std::runtime_error("Failed to reallocate SimpleBufferSink::data_");
      }

      data_ = new_str;
      size_ = new_size;
    }

    memcpy(data_ + offset_, buffer, size);
    offset_ += size;
  }

  const T* data() {
    return data_;
  }

  T* release() {
    T* out = data_;
    data_ = nullptr;
    return out;
  }

private:
  T* data_;
  int64_t size_;
  int64_t offset_;
};

}

}
