
#pragma once

#include <cstdlib>
#include <limits>
#include <cstring>
#include <algorithm>

#include "handler.hpp"
#include "common.hpp"

// The classes and functions in this file are all about building
// struct ArrowArray and struct ArrowSchema objects. All memory
// is allocated using malloc() or realloc() and freed using free().
// The general pattern is to use create an ArrayBuilder, write to it,
// and call release() to transfer ownership of the buffers to the
// struct ArrowArray/struct ArrowSchema.

// These two release callbacks can't be header only (I think);
// nor can the be with in the namespaces (I think)
void geoarrow_builder_release_schema_internal(struct ArrowSchema* schema);
void geoarrow_builder_release_array_data_internal(struct ArrowArray* array_data);

#ifdef GEOARROW_NO_HEADER_ONLY

// The .release() callback for all struct ArrowSchemas populated here
void geoarrow_builder_release_schema_internal(struct ArrowSchema* schema) {
  if (schema != nullptr && schema->release != nullptr) {
    // schema->name and/or schema->format must be kept alive via
    // private_data if dynamically allocated

    // metadata must be allocated with malloc()
    if (schema->metadata != nullptr) free((void*) schema->metadata);

    // this object owns the memory for all the children, but those
    // children may have been generated elsewhere and might have
    // their own release() callback.
    if (schema->children != nullptr) {
      for (int64_t i = 0; i < schema->n_children; i++) {
        if (schema->children[i] != nullptr) {
          if(schema->children[i]->release != nullptr) {
            schema->children[i]->release(schema->children[i]);
          }

          free(schema->children[i]);
        }
      }

      free(schema->children);
    }

    // this object owns the memory for the dictionary but it
    // may have been generated somewhere else and have its own
    // release() callback.
    if (schema->dictionary != nullptr) {
      if (schema->dictionary->release != nullptr) {
        schema->dictionary->release(schema->dictionary);
      }

      free(schema->dictionary);
    }

    // private data must be allocated with malloc() if needed
    if (schema->private_data != nullptr) {
      free(schema->private_data);
    }

    schema->release = nullptr;
  }
}

// The .release() callback for all struct ArrowArrays populated here
void geoarrow_builder_release_array_data_internal(struct ArrowArray* array_data) {
  if (array_data != nullptr && array_data->release != nullptr) {

    // buffers must be allocated with malloc()
    if (array_data->buffers != nullptr) {
      for (int64_t i = 0; i < array_data->n_buffers; i++) {
        if (array_data->buffers[i] != nullptr) {
          free((void*) array_data->buffers[i]);
        }
      }

      free(array_data->buffers);
    }

    // This object owns the memory for its children, but those children
    // might have their own release() callbacks if they were generated
    // elsewhere.
    if (array_data->children != nullptr) {
      for (int64_t i = 0; i < array_data->n_children; i++) {
        if (array_data->children[i] != nullptr) {
          if (array_data->children[i]->release != nullptr) {
            array_data->children[i]->release(array_data->children[i]);
          }

          free(array_data->children[i]);
        }
      }

      free(array_data->children);
    }

    // This object owns the memory for the dictionary, but it
    // might have its own release() callback if generated elsewhere
    if (array_data->dictionary != nullptr) {
      if (array_data->dictionary->release != nullptr) {
        array_data->dictionary->release(array_data->dictionary);
      }

      free(array_data->dictionary);
    }

    // private data must be allocated with malloc() if needed
    if (array_data->private_data != nullptr) {
      free(array_data->private_data);
    }

    array_data->release = nullptr;
  }
}

#endif

namespace geoarrow {

namespace builder {

// Allocates a struct ArrowSchema whose members can be further
// populated by the caller. This ArrowSchema owns the memory of its children
// and its dictionary (i.e., the parent release() callback will call the
// release() method of each child and then free() it).
inline void allocate_schema(struct ArrowSchema* schema, int64_t n_children = 0) {
  // schema->name and/or schema->format must be kept alive via
  // private_data if dynamically allocated
  schema->format = "";
  schema->name = "";
  schema->metadata = nullptr;
  schema->flags = ARROW_FLAG_NULLABLE;
  schema->n_children = n_children;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->private_data = nullptr;
  schema->release = &geoarrow_builder_release_schema_internal;

  if (n_children > 0) {
    schema->children = reinterpret_cast<struct ArrowSchema**>(
      malloc(n_children * sizeof(struct ArrowSchema*)));

    if (schema->children == nullptr) {
      geoarrow_builder_release_schema_internal(schema);
      throw util::IOException(
        "Failed to allocate schema->children of size %lld", n_children);
    }

    memset(schema->children, 0, n_children * sizeof(struct ArrowSchema*));

    for (int64_t i = 0; i < n_children; i++) {
      schema->children[i] = reinterpret_cast<struct ArrowSchema*>(
        malloc(sizeof(struct ArrowSchema)));

      if (schema->children[i] == nullptr) {
        geoarrow_builder_release_schema_internal(schema);
        throw util::IOException("Failed to allocate schema->children[%lld]", i);
      }

      schema->children[i]->release = nullptr;
    }
  }

  // we don't allocate the dictionary because it has to be nullptr
  // for non-dictionary-encoded arrays
}


// Allocates a struct ArrowArray whose members can be further
// populated by the caller. This ArrowArray owns the memory of its children
// and dictionary (i.e., the parent release() callback will call the release()
// method of each child and then free() it).
inline void allocate_array_data(struct ArrowArray* array_data, int64_t n_buffers,
                                int64_t n_children) {
  array_data->length = 0;
  array_data->null_count = -1;
  array_data->offset = 0;
  array_data->n_buffers = n_buffers;
  array_data->n_children = n_children;
  array_data->buffers = nullptr;
  array_data->children = nullptr;
  array_data->dictionary = nullptr;
  array_data->private_data = nullptr;
  array_data->release = &geoarrow_builder_release_array_data_internal;

  if (n_buffers > 0) {
    array_data->buffers = reinterpret_cast<const void**>(
      malloc(n_buffers * sizeof(const void*)));

    if (array_data->buffers == nullptr) {
      geoarrow_builder_release_array_data_internal(array_data);
      throw util::IOException(
        "Failed to allocate array_data->buffers of size %lld", n_buffers);
    }

    memset(array_data->buffers, 0, n_buffers * sizeof(const void*));
  }

  if (n_children > 0) {
    array_data->children = reinterpret_cast<struct ArrowArray**>(
      malloc(n_children * sizeof(struct ArrowArray*)));

    if (array_data->children == nullptr) {
      geoarrow_builder_release_array_data_internal(array_data);
      throw util::IOException(
        "Failed to allocate array_data->children of size %lld", n_children);
    }

    memset(array_data->children, 0, n_children * sizeof(struct ArrowArray*));

    for (int64_t i = 0; i < n_children; i++) {
      array_data->children[i] = reinterpret_cast<struct ArrowArray*>(
        malloc(sizeof(struct ArrowArray)));

      if (array_data->children[i] == nullptr) {
        geoarrow_builder_release_array_data_internal(array_data);
        throw util::IOException("Failed to allocate array_data->children[%lld]", i);
      }

      array_data->children[i]->release = nullptr;
    }
  }

  // we don't allocate the dictionary because it has to be nullptr
  // for non-dictionary-encoded arrays
}

// A construct needed to make sure that anything that gets allocated
// as part of the array construction process is cleaned up should
// an exception be thrown as part of that process. The intended pattern
// is to declare a CArrayFinalizer at the beginning of the ArrayBuilder's
// release() method, and call release() before returning.
class CArrayFinalizer {
public:
  struct ArrowArray array_data;
  struct ArrowSchema schema;

  CArrayFinalizer() {
    array_data.release = nullptr;
    schema.release = nullptr;
  }

  void allocate(int64_t n_buffers, int64_t n_children = 0) {
    allocate_array_data(&array_data, n_buffers, n_children);
    allocate_schema(&schema, n_children);
  }

  void release(struct ArrowArray* array_data_out, struct ArrowSchema* schema_out) {
    // The output pointers must be non-null but must be released before they
    // get here (or else they will leak).
    if (array_data_out == nullptr) {
      throw util::IOException("output array_data is nullptr");
    }

    if (schema_out == nullptr) {
      throw util::IOException("output schema is nullptr");
    }

    if (array_data_out->release != nullptr) {
      throw util::IOException("output array_data is not released");
    }

    if (schema_out->release != nullptr) {
      throw util::IOException("output schema is not released");
    }

    memcpy(array_data_out, &array_data, sizeof(struct ArrowArray));
    array_data.release = nullptr;

    memcpy(schema_out, &schema, sizeof(struct ArrowSchema));
    schema.release = nullptr;
  }

  ~CArrayFinalizer() {
    geoarrow_builder_release_array_data_internal(&array_data);
    geoarrow_builder_release_schema_internal(&schema);
  }
};

template<typename BufferT, typename T = BufferT, int bitwidth = 8 * sizeof(BufferT)>
class BufferBuilder {
public:
  BufferBuilder(int64_t capacity = 1024): data_(nullptr), capacity_(-1),
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
    if (capacity == capacity_) {
      return;
    }

    int64_t n_bytes;
    if (bitwidth == 1) {
      n_bytes = ((capacity * bitwidth + 8) / 8);
    } else {
      n_bytes = ((capacity * bitwidth) / 8);
    }

    BufferT* new_data = reinterpret_cast<BufferT*>(realloc(data_, n_bytes));
    if (new_data == nullptr) {
      throw util::IOException(
        "Failed to allocate BufferBuilder::data_ of capacity %lld", capacity);
    }

    data_ = new_data;
    capacity_ = n_bytes * 8 / bitwidth;
  }

  void write_buffer(const BufferT* buffer, int64_t size) {
    reserve(size);
    memcpy(data_ + size_, buffer, size);
    size_ += size;
  }

  const BufferT* data() {
    return data_;
  }

  BufferT* data_at_cursor() {
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

    if (null_count_ == 0) {
      return nullptr;
    } else {
      return BufferBuilder<uint8_t, bool, 1>::release();
    }
  }

  int64_t null_count() { return null_count_; }

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

  virtual void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
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

  void reserve_data(int64_t additional_data_size_guess) {
    if (needs_make_large(additional_data_size_guess)) {
      make_large();
    }
  }

  int64_t remaining_data_capacity() {
    return data_buffer_builder_.remaining_capacity();
  }

  uint8_t* data_at_cursor() {
    return data_buffer_builder_.data_at_cursor();
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

    finalizer.array_data.buffers[0] = validity_buffer_builder_.release();
    finalizer.array_data.buffers[2] = data_buffer_builder_.release();
    if (is_large_) {
      finalizer.schema.format = "U";
      finalizer.array_data.buffers[1] = large_offset_buffer_builder_.release();
    } else {
      finalizer.schema.format = "u";
      finalizer.array_data.buffers[1] = offset_buffer_builder_.release();
    }

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
