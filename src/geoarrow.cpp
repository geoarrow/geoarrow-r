
#include "port.h"
#ifdef IS_LITTLE_ENDIAN
#define GEOARROW_ENDIAN 0x01
#else
#define GEOARROW_ENDIAN 0x00
#endif

#define FASTFLOAT_ASSERT(x)
#include "internal/fast_float/fast_float.h"

// For implementations of release callbacks
#define GEOARROW_NO_HEADER_ONLY

#include "internal/geoarrow-hpp/factory.hpp"
#include "internal/geoarrow-hpp/builder-factory.hpp"
