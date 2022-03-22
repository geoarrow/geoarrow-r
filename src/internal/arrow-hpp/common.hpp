
#pragma once

#include <cstdint>
#include <stdexcept>
#include <cstdarg>

#ifndef ARROW_FLAG_DICTIONARY_ORDERED
extern "C" {

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
  const char* (*get_last_error)(struct ArrowArrayStream*);
  void (*release)(struct ArrowArrayStream*);
  void* private_data;
};

}
#endif

namespace arrow {
namespace hpp {
namespace util {

class Exception: public std::exception {
public:
  Exception(const char* fmt, ...) {
    memset(error_, 0, sizeof(error_));
    va_list args;
    va_start(args, fmt);
    vsnprintf(error_, sizeof(error_) - 1, fmt, args);
    va_end(args);
  }

  const char* what() const noexcept {
    return error_;
  }

private:
  char error_[8096];
};

}
}
}
