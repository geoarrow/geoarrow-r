
#include "geoarrow.h"

#if defined(GEOARROW_USE_FAST_FLOAT) && GEOARROW_USE_FAST_FLOAT

#include "fast_float.h"

extern "C" GeoArrowErrorCode GeoArrowFromChars(const char* first, const char* last,
                                               double* out) {
  auto answer = fast_float::from_chars(first, last, *out);
  if (answer.ec != std::errc()) {
    return EINVAL;
  } else {
    return GEOARROW_OK;
  }
}

#endif
