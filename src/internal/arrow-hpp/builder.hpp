
#pragma once

#include <cstdlib>
#include <limits>
#include <cstring>
#include <algorithm>
#include <string>

#include "common.hpp"

// The classes and functions in this file are all about building
// struct ArrowArray and struct ArrowSchema objects. All memory
// is allocated using malloc() or realloc() and freed using free().
// The general pattern is to use create an ArrayBuilder, write to it,
// and call release() to transfer ownership of the buffers to the
// struct ArrowArray/struct ArrowSchema.

// These two release callbacks can't be header only (I think);
// nor can the be with in the namespaces (I think)
void arrow_hpp_builder_release_schema_internal(struct ArrowSchema* schema);
void arrow_hpp_builder_release_array_data_internal(struct ArrowArray* array_data);

#ifdef ARROW_HPP_NO_HEADER_ONLY

// The .release() callback for all struct ArrowSchemas populated here
void arrow_hpp_builder_release_schema_internal(struct ArrowSchema* schema) {
  if (schema != nullptr && schema->release != nullptr) {
    // format, name, and metadata must be nullptr or allocated with malloc()
    if (schema->format != nullptr) free((void*) schema->format);
    if (schema->name != nullptr) free((void*) schema->name);
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
void arrow_hpp_builder_release_array_data_internal(struct ArrowArray* array_data) {
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

namespace arrow {
namespace hpp {

namespace builder {

// Allocates a struct ArrowSchema whose members can be further
// populated by the caller. This ArrowSchema owns the memory of its children
// and its dictionary (i.e., the parent release() callback will call the
// release() method of each child and then free() it).
inline void allocate_schema(struct ArrowSchema* schema, int64_t n_children = 0) {
  // schema->name and/or schema->format must be allocated via malloc()
  schema->format = nullptr;
  schema->name = nullptr;
  schema->metadata = nullptr;
  schema->flags = ARROW_FLAG_NULLABLE;
  schema->n_children = n_children;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->private_data = nullptr;
  schema->release = &arrow_hpp_builder_release_schema_internal;

  if (n_children > 0) {
    schema->children = reinterpret_cast<struct ArrowSchema**>(
      malloc(n_children * sizeof(struct ArrowSchema*)));

    if (schema->children == nullptr) {
      arrow_hpp_builder_release_schema_internal(schema);
      throw util::Exception(
        "Failed to allocate schema->children of size %lld", n_children);
    }

    memset(schema->children, 0, n_children * sizeof(struct ArrowSchema*));

    for (int64_t i = 0; i < n_children; i++) {
      schema->children[i] = reinterpret_cast<struct ArrowSchema*>(
        malloc(sizeof(struct ArrowSchema)));

      if (schema->children[i] == nullptr) {
        arrow_hpp_builder_release_schema_internal(schema);
        throw util::Exception("Failed to allocate schema->children[%lld]", i);
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
  array_data->release = &arrow_hpp_builder_release_array_data_internal;

  if (n_buffers > 0) {
    array_data->buffers = reinterpret_cast<const void**>(
      malloc(n_buffers * sizeof(const void*)));

    if (array_data->buffers == nullptr) {
      arrow_hpp_builder_release_array_data_internal(array_data);
      throw util::Exception(
        "Failed to allocate array_data->buffers of size %lld", n_buffers);
    }

    memset(array_data->buffers, 0, n_buffers * sizeof(const void*));
  }

  if (n_children > 0) {
    array_data->children = reinterpret_cast<struct ArrowArray**>(
      malloc(n_children * sizeof(struct ArrowArray*)));

    if (array_data->children == nullptr) {
      arrow_hpp_builder_release_array_data_internal(array_data);
      throw util::Exception(
        "Failed to allocate array_data->children of size %lld", n_children);
    }

    memset(array_data->children, 0, n_children * sizeof(struct ArrowArray*));

    for (int64_t i = 0; i < n_children; i++) {
      array_data->children[i] = reinterpret_cast<struct ArrowArray*>(
        malloc(sizeof(struct ArrowArray)));

      if (array_data->children[i] == nullptr) {
        arrow_hpp_builder_release_array_data_internal(array_data);
        throw util::Exception("Failed to allocate array_data->children[%lld]", i);
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
    set_schema_format("");
    set_schema_name("");
  }

  void set_schema_format(const char* format) {
    if (schema.format != nullptr) {
      free((void*) schema.format);
    }

    size_t len = strlen(format);
    char* format_owned = reinterpret_cast<char*>(malloc(len + 1));
    if (format_owned == nullptr) {
      throw util::Exception("Failed to allocate schema format");
    }
    memcpy(format_owned, format, len);
    format_owned[len] = '\0';

    schema.format = format_owned;
  }

  void set_schema_name(const char* name) {
    if (schema.name != nullptr) {
      free((void*) schema.name);
    }

    size_t len = strlen(name);
    char* name_owned = reinterpret_cast<char*>(malloc(len + 1));
    if (name_owned == nullptr) {
      throw util::Exception("Failed to allocate schema name");
    }
    memcpy(name_owned, name, len);
    name_owned[len] = '\0';

    schema.name = name_owned;
  }

  void release(struct ArrowArray* array_data_out, struct ArrowSchema* schema_out) {
    // The output pointers must be non-null but must be released before they
    // get here (or else they will leak).
    if (array_data_out == nullptr) {
      throw util::Exception("output array_data is nullptr");
    }

    if (schema_out == nullptr) {
      throw util::Exception("output schema is nullptr");
    }

    if (array_data_out->release != nullptr) {
      array_data_out->release(array_data_out);
    }

    if (schema_out->release != nullptr) {
      schema_out->release(schema_out);
    }

    memcpy(array_data_out, &array_data, sizeof(struct ArrowArray));
    array_data.release = nullptr;

    memcpy(schema_out, &schema, sizeof(struct ArrowSchema));
    schema.release = nullptr;
  }

  ~CArrayFinalizer() {
    if (array_data.release != nullptr) {
      array_data.release(&array_data);
    }

    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }
};

template<typename BufferT>
class BufferBuilder {
public:
  BufferBuilder(int64_t capacity = 1024): data_(nullptr), capacity_(-1),
    size_(0), growth_factor_(2) {
    reallocate(capacity);
  }

  virtual ~BufferBuilder() {
    if (data_ != nullptr) {
      free(data_);
    }
  }

  void write_element(BufferT item) {
    write_buffer(&item, 1);
  }

  void shrink() { reallocate(size_); }

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

    int64_t n_bytes = capacity * sizeof(BufferT);
    if (n_bytes <= 0) {
      n_bytes = 1;
    }

    BufferT* new_data = reinterpret_cast<BufferT*>(realloc(data_, n_bytes));
    if (new_data == nullptr) {
      throw util::Exception(
        "Failed to allocate BufferBuilder::data_ of capacity %lld", capacity);
    }

    data_ = new_data;
    capacity_ = n_bytes / sizeof(BufferT);
  }

  void write_buffer(const BufferT* buffer, int64_t size) {
    reserve(size);
    memcpy(data_ + size_, buffer, size * sizeof(BufferT));
    advance(size);
  }

  const BufferT* data() { return data_; }
  BufferT* mutable_data() { return data_; }
  BufferT* data_at_cursor() { return data_ + size_; }
  void advance(int64_t n) { size_ += n; }
  int64_t capacity() { return capacity_; }
  int64_t size() { return size_; }
  int64_t remaining_capacity() { return capacity_ - size_; }

protected:
  BufferT* data_;
  int64_t capacity_;
  int64_t size_;
  double growth_factor_;
};

class BitmapBuilder {
public:
  BitmapBuilder():
    buffer_builder_(0), null_count_(0), buffer_(0), buffer_size_(0), size_(0),
    allocated_(false) {}

  virtual ~BitmapBuilder() {}

  int64_t capacity() { return buffer_builder_.capacity() * 8; }
  int64_t size() { return size_; }
  bool is_allocated() { return allocated_; }

  void reallocate(int64_t capacity) {
    if (capacity % 8 == 0) {
      buffer_builder_.reallocate(capacity / 8);
    } else {
      buffer_builder_.reallocate(capacity / 8 + 1);
    }
  }

  void reserve(int64_t additional_capacity) {
    if (!allocated_) {
      return;
    }

    buffer_builder_.reserve(additional_capacity / 8 + 1);
  }

  void shrink() {
    if (allocated_) {
      buffer_builder_.shrink();
    }
  }

  void write_elements(int64_t n, bool value) {
    // Could be more efficient!
    for (int64_t i = 0; i < n; i++) {
      write_element(value);
    }
  }

  void write_element(bool value) {
    size_++;
    null_count_ += !value;
    buffer_ = buffer_ | (((uint8_t) value) << buffer_size_);
    buffer_size_++;
    if (buffer_size_ == 8) {
      if (buffer_ == 0xff && !allocated_) {
        buffer_size_ = 0;
        buffer_ = 0;
        return;
      }

      if (buffer_ != 0xff && !allocated_) {
        trigger_alloc(size_);
      }

      buffer_builder_.write_element(buffer_);
      buffer_size_ = 0;
      buffer_ = 0;
    }
  }

  uint8_t* release() {
    if (allocated_ || null_count_ > 0) {
      // when we call write_element() we might end up with extra
      // nulls in the null_count that shouldn't be there
      int64_t actual_null_count_ = null_count_;

      int remaining_bits = 8 - buffer_size_;
      for (int i = 0; i < remaining_bits; i++) {
        write_element(false);
      }

      null_count_ = actual_null_count_;
    }

    if (allocated_) {
      return buffer_builder_.release();
    } else {
      return nullptr;
    }
  }

  int64_t null_count() { return null_count_; }

private:
  BufferBuilder<uint8_t> buffer_builder_;
  int64_t null_count_;
  uint8_t buffer_;
  int buffer_size_;
  int64_t size_;
  bool allocated_;

  void trigger_alloc(int64_t capacity) {
    reallocate(capacity);
    memset(
      buffer_builder_.mutable_data() + buffer_builder_.size(),
      0xff,
      buffer_builder_.capacity());
    allocated_ = true;
    if (size_ > 0) {
      buffer_builder_.advance((size_ - 1) / 8);
    }
  }
};


class ArrayBuilder {
public:
  ArrayBuilder(): name_(""), size_(0) {}

  virtual ~ArrayBuilder() {}

  int64_t size() const { return size_; }
  const std::string& name() const { return name_; }
  void set_name(const std::string& name) { name_ = name; }

  virtual void reserve(int64_t additional_capacity) {
    validity_buffer_builder_.reserve(additional_capacity);
  }

  virtual void shrink() {
    validity_buffer_builder_.shrink();
  }

  virtual void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    throw util::Exception("Not implemented");
  }

  virtual const char* get_format() { return ""; }

protected:
  std::string name_;
  int64_t size_;
  builder::BitmapBuilder validity_buffer_builder_;
};


template <typename BufferT>
class FixedSizeLayoutArrayBuilder: public ArrayBuilder {
public:
  FixedSizeLayoutArrayBuilder() {}

  void reserve(int64_t additional_capacity) {
    ArrayBuilder::reserve(additional_capacity);
    buffer_builder_.reserve(additional_capacity);
  }

  void shrink() {
    ArrayBuilder::shrink();
    buffer_builder_.shrink();
  }

  void write_element(BufferT value) {
    buffer_builder_.write_element(value);
    size_++;
  }

  void write_buffer(const double* buffer, int64_t n) {
    buffer_builder_.write_buffer(buffer, n);
    size_+= n;
  }

  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    CArrayFinalizer finalizer;
    finalizer.allocate(2);
    finalizer.set_schema_format(get_format());
    finalizer.set_schema_name(name().c_str());

    finalizer.array_data.length = buffer_builder_.size();
    finalizer.array_data.null_count = validity_buffer_builder_.null_count();

    finalizer.array_data.buffers[0] = validity_buffer_builder_.release();
    finalizer.array_data.buffers[1] = buffer_builder_.release();

    finalizer.release(array_data, schema);
  }

private:
  BufferBuilder<BufferT> buffer_builder_;

};

class Float64ArrayBuilder: public FixedSizeLayoutArrayBuilder<double> {
public:
  virtual const char* get_format() { return "g"; }
};

}

}

}
