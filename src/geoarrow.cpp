
#include "port.h"
#ifdef IS_LITTLE_ENDIAN
#define GEOARROW_ENDIAN 0x01
#else
#define GEOARROW_ENDIAN 0x00
#endif

#define FASTFLOAT_ASSERT(x)
#include "internal/fast_float/fast_float.h"

#include "ryu/ryu.h"
#define geoarrow_d2s_fixed_n geoarrow_d2sfixed_buffered_n

// For implementations of release callbacks
#define ARROW_HPP_IMPL

#include "geoarrow.h"
