
#include <errno.h>
#include <stdint.h>

#include "geoarrow.h"

#include "nanoarrow.h"

#define EWKB_Z_BIT 0x80000000
#define EWKB_M_BIT 0x40000000
#define EWKB_SRID_BIT 0x20000000

#ifndef GEOARROW_NATIVE_ENDIAN
#define GEOARROW_NATIVE_ENDIAN 0x01
#endif

#ifndef GEOARROW_BSWAP32
static inline uint32_t bswap_32(uint32_t x) {
  return (((x & 0xFF) << 24) | ((x & 0xFF00) << 8) | ((x & 0xFF0000) >> 8) |
          ((x & 0xFF000000) >> 24));
}
#define GEOARROW_BSWAP32(x) bswap_32(x)
#endif

#ifndef GEOARROW_BSWAP64
static inline uint64_t bswap_64(uint64_t x) {
  return (((x & 0xFFULL) << 56) | ((x & 0xFF00ULL) << 40) | ((x & 0xFF0000ULL) << 24) |
          ((x & 0xFF000000ULL) << 8) | ((x & 0xFF00000000ULL) >> 8) |
          ((x & 0xFF0000000000ULL) >> 24) | ((x & 0xFF000000000000ULL) >> 40) |
          ((x & 0xFF00000000000000ULL) >> 56));
}
#define GEOARROW_BSWAP64(x) bswap_64(x)
#endif

// This must be divisible by 2, 3, and 4
#define COORD_CACHE_SIZE_ELEMENTS 3072

struct WKBReaderPrivate {
  const uint8_t* data;
  int64_t size_bytes;
  const uint8_t* data0;
  int need_swapping;
  double coords[COORD_CACHE_SIZE_ELEMENTS];
  struct GeoArrowCoordView coord_view;
};

static inline int WKBReaderReadEndian(struct WKBReaderPrivate* s,
                                      struct GeoArrowError* error) {
  if (s->size_bytes > 0) {
    s->need_swapping = s->data[0] != GEOARROW_NATIVE_ENDIAN;
    s->data++;
    s->size_bytes--;
    return GEOARROW_OK;
  } else {
    GeoArrowErrorSet(error, "Expected endian byte but found end of buffer at byte %ld",
                     (long)(s->data - s->data0));
    return EINVAL;
  }
}

static inline int WKBReaderReadUInt32(struct WKBReaderPrivate* s, uint32_t* out,
                                      struct GeoArrowError* error) {
  if (s->size_bytes >= 4) {
    memcpy(out, s->data, sizeof(uint32_t));
    s->data += sizeof(uint32_t);
    s->size_bytes -= sizeof(uint32_t);
    if (s->need_swapping) {
      *out = GEOARROW_BSWAP32(*out);
    }
    return GEOARROW_OK;
  } else {
    GeoArrowErrorSet(error, "Expected uint32 but found end of buffer at byte %ld",
                     (long)(s->data - s->data0));
    return EINVAL;
  }
}

static inline void WKBReaderMaybeBswapCoords(struct WKBReaderPrivate* s, int64_t n) {
  if (s->need_swapping) {
    uint64_t* data64 = (uint64_t*)s->coords;
    for (int i = 0; i < n; i++) {
      data64[i] = GEOARROW_BSWAP64(data64[i]);
    }
  }
}

static int WKBReaderReadCoordinates(struct WKBReaderPrivate* s, int64_t n_coords,
                                    struct GeoArrowVisitor* v) {
  int64_t bytes_needed = n_coords * s->coord_view.n_values * sizeof(double);
  if (s->size_bytes < bytes_needed) {
    ArrowErrorSet(
        (struct ArrowError*)v->error,
        "Expected coordinate sequence of %ld coords (%ld bytes) but found %ld bytes "
        "remaining at byte %ld",
        (long)n_coords, (long)bytes_needed, (long)s->size_bytes,
        (long)(s->data - s->data0));
    return EINVAL;
  }

  int32_t chunk_size = COORD_CACHE_SIZE_ELEMENTS / s->coord_view.n_values;
  s->coord_view.n_coords = chunk_size;

  // Process full chunks
  while (n_coords > chunk_size) {
    memcpy(s->coords, s->data, COORD_CACHE_SIZE_ELEMENTS * sizeof(double));
    WKBReaderMaybeBswapCoords(s, COORD_CACHE_SIZE_ELEMENTS);
    NANOARROW_RETURN_NOT_OK(v->coords(v, &s->coord_view));
    s->data += COORD_CACHE_SIZE_ELEMENTS * sizeof(double);
    s->size_bytes -= COORD_CACHE_SIZE_ELEMENTS * sizeof(double);
    n_coords -= chunk_size;
  }

  // Process the last chunk
  int64_t remaining_bytes = n_coords * s->coord_view.n_values * sizeof(double);
  memcpy(s->coords, s->data, remaining_bytes);
  s->data += remaining_bytes;
  s->size_bytes -= remaining_bytes;
  s->coord_view.n_coords = n_coords;
  WKBReaderMaybeBswapCoords(s, n_coords * s->coord_view.n_values);
  return v->coords(v, &s->coord_view);
}

static int WKBReaderReadGeometry(struct WKBReaderPrivate* s, struct GeoArrowVisitor* v) {
  NANOARROW_RETURN_NOT_OK(WKBReaderReadEndian(s, v->error));
  uint32_t geometry_type;
  const uint8_t* data_at_geom_type = s->data;
  NANOARROW_RETURN_NOT_OK(WKBReaderReadUInt32(s, &geometry_type, v->error));

  int has_z = 0;
  int has_m = 0;

  // Handle EWKB high bits
  if (geometry_type & EWKB_Z_BIT) {
    has_z = 1;
  }

  if (geometry_type & EWKB_M_BIT) {
    has_m = 1;
  }

  if (geometry_type & EWKB_SRID_BIT) {
    // We ignore this because it's hard to work around if a user somehow
    // has embedded srid but still wants the data and doesn't have another way
    // to convert
    uint32_t embedded_srid;
    NANOARROW_RETURN_NOT_OK(WKBReaderReadUInt32(s, &embedded_srid, v->error));
  }

  geometry_type = geometry_type & 0x0000ffff;

  // Handle ISO X000 geometry types
  if (geometry_type >= 3000) {
    geometry_type = geometry_type - 3000;
    has_z = 1;
    has_m = 1;
  } else if (geometry_type >= 2000) {
    geometry_type = geometry_type - 2000;
    has_m = 1;
  } else if (geometry_type >= 1000) {
    geometry_type = geometry_type - 1000;
    has_z = 1;
  }

  // Read the number of coordinates/rings/parts
  uint32_t size;
  if (geometry_type != GEOARROW_GEOMETRY_TYPE_POINT) {
    NANOARROW_RETURN_NOT_OK(WKBReaderReadUInt32(s, &size, v->error));
  } else {
    size = 1;
  }

  // Set coord size
  s->coord_view.n_values = 2 + has_z + has_m;
  s->coord_view.coords_stride = s->coord_view.n_values;

  // Resolve dimensions
  enum GeoArrowDimensions dimensions;
  if (has_z && has_m) {
    dimensions = GEOARROW_DIMENSIONS_XYZM;
  } else if (has_z) {
    dimensions = GEOARROW_DIMENSIONS_XYZ;
  } else if (has_m) {
    dimensions = GEOARROW_DIMENSIONS_XYM;
  } else {
    dimensions = GEOARROW_DIMENSIONS_XY;
  }

  NANOARROW_RETURN_NOT_OK(v->geom_start(v, geometry_type, dimensions));

  switch (geometry_type) {
    case GEOARROW_GEOMETRY_TYPE_POINT:
    case GEOARROW_GEOMETRY_TYPE_LINESTRING:
      NANOARROW_RETURN_NOT_OK(WKBReaderReadCoordinates(s, size, v));
      break;
    case GEOARROW_GEOMETRY_TYPE_POLYGON:
      for (uint32_t i = 0; i < size; i++) {
        uint32_t ring_size;
        NANOARROW_RETURN_NOT_OK(WKBReaderReadUInt32(s, &ring_size, v->error));
        NANOARROW_RETURN_NOT_OK(v->ring_start(v));
        NANOARROW_RETURN_NOT_OK(WKBReaderReadCoordinates(s, ring_size, v));
        NANOARROW_RETURN_NOT_OK(v->ring_end(v));
      }
      break;
    case GEOARROW_GEOMETRY_TYPE_MULTIPOINT:
    case GEOARROW_GEOMETRY_TYPE_MULTILINESTRING:
    case GEOARROW_GEOMETRY_TYPE_MULTIPOLYGON:
    case GEOARROW_GEOMETRY_TYPE_GEOMETRYCOLLECTION:
      for (uint32_t i = 0; i < size; i++) {
        NANOARROW_RETURN_NOT_OK(WKBReaderReadGeometry(s, v));
      }
      break;
    default:
      GeoArrowErrorSet(v->error,
                       "Expected valid geometry type code but found %u at byte %ld",
                       (unsigned int)geometry_type, (long)(data_at_geom_type - s->data0));
      return EINVAL;
  }

  return v->geom_end(v);
}

GeoArrowErrorCode GeoArrowWKBReaderInit(struct GeoArrowWKBReader* reader) {
  struct WKBReaderPrivate* s =
      (struct WKBReaderPrivate*)ArrowMalloc(sizeof(struct WKBReaderPrivate));

  if (s == NULL) {
    return ENOMEM;
  }

  s->data0 = NULL;
  s->data = NULL;
  s->size_bytes = 0;
  s->need_swapping = 0;

  s->coord_view.coords_stride = 2;
  s->coord_view.n_values = 2;
  s->coord_view.n_coords = 0;
  s->coord_view.values[0] = s->coords + 0;
  s->coord_view.values[1] = s->coords + 1;
  s->coord_view.values[2] = s->coords + 2;
  s->coord_view.values[3] = s->coords + 3;

  reader->private_data = s;
  return GEOARROW_OK;
}

void GeoArrowWKBReaderReset(struct GeoArrowWKBReader* reader) {
  ArrowFree(reader->private_data);
}

GeoArrowErrorCode GeoArrowWKBReaderVisit(struct GeoArrowWKBReader* reader,
                                         struct GeoArrowBufferView src,
                                         struct GeoArrowVisitor* v) {
  struct WKBReaderPrivate* s = (struct WKBReaderPrivate*)reader->private_data;
  s->data0 = src.data;
  s->data = src.data;
  s->size_bytes = src.size_bytes;

  NANOARROW_RETURN_NOT_OK(v->feat_start(v));
  NANOARROW_RETURN_NOT_OK(WKBReaderReadGeometry(s, v));
  NANOARROW_RETURN_NOT_OK(v->feat_end(v));

  return GEOARROW_OK;
}
