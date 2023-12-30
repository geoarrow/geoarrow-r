
#include "geoarrow_type.h"

#if defined(GEOARROW_USE_RYU) && GEOARROW_USE_RYU

#include "ryu/ryu.h"

int64_t GeoArrowPrintDouble(double f, uint32_t precision, char* result) {
  return GeoArrowd2sfixed_buffered_n(f, precision, result);
}

#else

#include <stdio.h>

int64_t GeoArrowPrintDouble(double f, uint32_t precision, char* result) {
  int64_t n_chars = snprintf(result, 128, "%0.*f", precision, f);

  // Strip trailing zeroes + decimal
  for (int64_t i = n_chars - 1; i >= 0; i--) {
    if (result[i] == '0') {
      n_chars--;
    } else if (result[i] == '.') {
      n_chars--;
      break;
    } else {
      break;
    }
  }

  return n_chars;
}

#endif
