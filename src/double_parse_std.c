
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "geoarrow_type.h"

#if !defined(GEOARROW_USE_FAST_FLOAT) || !GEOARROW_USE_FAST_FLOAT

GeoArrowErrorCode GeoArrowFromChars(const char* first, const char* last, double* out) {
  if (first == last) {
    return EINVAL;
  }

  int64_t size_bytes = last - first;

  // There is no guarantee that src.data is null-terminated. The maximum size of
  // a double is 24 characters, but if we can't fit all of src for some reason, error.
  char src_copy[64];
  if (size_bytes >= ((int64_t)sizeof(src_copy))) {
    return EINVAL;
  }

  memcpy(src_copy, first, size_bytes);
  char* last_copy = src_copy + size_bytes;
  *last_copy = '\0';

  char* end_ptr;
  double result = strtod(src_copy, &end_ptr);
  if (end_ptr != last_copy) {
    return EINVAL;
  } else {
    *out = result;
    return GEOARROW_OK;
  }
}

#endif
