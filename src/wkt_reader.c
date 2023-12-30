#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "nanoarrow.h"

#include "geoarrow.h"

#define COORD_CACHE_SIZE_COORDS 64

struct WKTReaderPrivate {
  const char* data;
  int64_t size_bytes;
  const char* data0;
  double coords[4 * COORD_CACHE_SIZE_COORDS];
  struct GeoArrowCoordView coord_view;
};

static inline void AdvanceUnsafe(struct WKTReaderPrivate* s, int64_t n) {
  s->data += n;
  s->size_bytes -= n;
}

static inline void SkipWhitespace(struct WKTReaderPrivate* s) {
  while (s->size_bytes > 0) {
    char c = *(s->data);
    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
      s->size_bytes--;
      s->data++;
    } else {
      break;
    }
  }
}

static inline int SkipUntil(struct WKTReaderPrivate* s, const char* items) {
  int64_t n_items = strlen(items);
  while (s->size_bytes > 0) {
    char c = *(s->data);
    if (c == '\0') {
      return 0;
    }

    for (int64_t i = 0; i < n_items; i++) {
      if (c == items[i]) {
        return 1;
      }
    }

    s->size_bytes--;
    s->data++;
  }

  return 0;
}

static inline void SkipUntilSep(struct WKTReaderPrivate* s) {
  SkipUntil(s, " \n\t\r,()");
}

static inline char PeekChar(struct WKTReaderPrivate* s) {
  if (s->size_bytes > 0) {
    return s->data[0];
  } else {
    return '\0';
  }
}

static inline struct ArrowStringView PeekUntilSep(struct WKTReaderPrivate* s,
                                                  int max_chars) {
  struct WKTReaderPrivate tmp = *s;
  if (tmp.size_bytes > max_chars) {
    tmp.size_bytes = max_chars;
  }

  SkipUntilSep(&tmp);
  struct ArrowStringView out = {s->data, tmp.data - s->data};
  return out;
}

static inline void SetParseErrorAuto(const char* expected, struct WKTReaderPrivate* s,
                                     struct GeoArrowError* error) {
  long pos = s->data - s->data0;
  // TODO: "but found ..." from s
  GeoArrowErrorSet(error, "Expected %s at byte %ld", expected, pos);
}

static inline int AssertChar(struct WKTReaderPrivate* s, char c,
                             struct GeoArrowError* error) {
  if (s->size_bytes > 0 && s->data[0] == c) {
    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  } else {
    char expected[4] = {'\'', c, '\'', '\0'};
    SetParseErrorAuto(expected, s, error);
    return EINVAL;
  }
}

static inline int AssertWhitespace(struct WKTReaderPrivate* s,
                                   struct GeoArrowError* error) {
  if (s->size_bytes > 0 && (s->data[0] == ' ' || s->data[0] == '\t' ||
                            s->data[0] == '\r' || s->data[0] == '\n')) {
    SkipWhitespace(s);
    return GEOARROW_OK;
  } else {
    SetParseErrorAuto("whitespace", s, error);
    return EINVAL;
  }
}

static inline int AssertWordEmpty(struct WKTReaderPrivate* s,
                                  struct GeoArrowError* error) {
  struct ArrowStringView word = PeekUntilSep(s, 6);
  if (word.size_bytes == 5 && strncmp(word.data, "EMPTY", 5) == 0) {
    AdvanceUnsafe(s, 5);
    return GEOARROW_OK;
  }

  SetParseErrorAuto("'(' or 'EMPTY'", s, error);
  return EINVAL;
}

static inline int ReadOrdinate(struct WKTReaderPrivate* s, double* out,
                               struct GeoArrowError* error) {
  const char* start = s->data;
  SkipUntilSep(s);
  int result = GeoArrowFromChars(start, s->data, out);
  if (result != GEOARROW_OK) {
    s->size_bytes += s->data - start;
    s->data = start;
    SetParseErrorAuto("number", s, error);
  }

  return result;
}

static inline void ResetCoordCache(struct WKTReaderPrivate* s) {
  s->coord_view.n_coords = 0;
}

static inline int FlushCoordCache(struct WKTReaderPrivate* s, struct GeoArrowVisitor* v) {
  if (s->coord_view.n_coords > 0) {
    int result = v->coords(v, &s->coord_view);
    s->coord_view.n_coords = 0;
    return result;
  } else {
    return GEOARROW_OK;
  }
}

static inline int ReadCoordinate(struct WKTReaderPrivate* s, struct GeoArrowVisitor* v) {
  if (s->coord_view.n_coords == COORD_CACHE_SIZE_COORDS) {
    NANOARROW_RETURN_NOT_OK(FlushCoordCache(s, v));
  }

  NANOARROW_RETURN_NOT_OK(ReadOrdinate(
      s, (double*)s->coord_view.values[0] + s->coord_view.n_coords, v->error));
  for (int i = 1; i < s->coord_view.n_values; i++) {
    NANOARROW_RETURN_NOT_OK(AssertWhitespace(s, v->error));
    NANOARROW_RETURN_NOT_OK(ReadOrdinate(
        s, (double*)s->coord_view.values[i] + s->coord_view.n_coords, v->error));
  }

  s->coord_view.n_coords++;
  return NANOARROW_OK;
}

static inline int ReadEmptyOrCoordinates(struct WKTReaderPrivate* s,
                                         struct GeoArrowVisitor* v) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);
    SkipWhitespace(s);

    ResetCoordCache(s);

    // Read the first coordinate (there must always be one)
    NANOARROW_RETURN_NOT_OK(ReadCoordinate(s, v));
    SkipWhitespace(s);

    // Read the rest of the coordinates
    while (PeekChar(s) != ')') {
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(ReadCoordinate(s, v));
      SkipWhitespace(s);
    }

    NANOARROW_RETURN_NOT_OK(FlushCoordCache(s, v));

    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadMultipointFlat(struct WKTReaderPrivate* s,
                                     struct GeoArrowVisitor* v,
                                     enum GeoArrowDimensions dimensions) {
  NANOARROW_RETURN_NOT_OK(AssertChar(s, '(', v->error));

  // Read the first coordinate (there must always be one)
  NANOARROW_RETURN_NOT_OK(v->geom_start(v, GEOARROW_GEOMETRY_TYPE_POINT, dimensions));
  ResetCoordCache(s);
  NANOARROW_RETURN_NOT_OK(ReadCoordinate(s, v));
  NANOARROW_RETURN_NOT_OK(FlushCoordCache(s, v));
  NANOARROW_RETURN_NOT_OK(v->geom_end(v));
  SkipWhitespace(s);

  // Read the rest of the coordinates
  while (PeekChar(s) != ')') {
    SkipWhitespace(s);
    NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
    SkipWhitespace(s);
    NANOARROW_RETURN_NOT_OK(v->geom_start(v, GEOARROW_GEOMETRY_TYPE_POINT, dimensions));
    ResetCoordCache(s);
    NANOARROW_RETURN_NOT_OK(ReadCoordinate(s, v));
    NANOARROW_RETURN_NOT_OK(FlushCoordCache(s, v));
    NANOARROW_RETURN_NOT_OK(v->geom_end(v));
    SkipWhitespace(s);
  }

  AdvanceUnsafe(s, 1);
  return GEOARROW_OK;
}

static inline int ReadEmptyOrPointCoordinate(struct WKTReaderPrivate* s,
                                             struct GeoArrowVisitor* v) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);

    SkipWhitespace(s);
    ResetCoordCache(s);
    NANOARROW_RETURN_NOT_OK(ReadCoordinate(s, v));
    NANOARROW_RETURN_NOT_OK(FlushCoordCache(s, v));
    SkipWhitespace(s);
    NANOARROW_RETURN_NOT_OK(AssertChar(s, ')', v->error));
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadPolygon(struct WKTReaderPrivate* s, struct GeoArrowVisitor* v) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);
    SkipWhitespace(s);

    // Read the first ring (there must always be one)
    NANOARROW_RETURN_NOT_OK(v->ring_start(v));
    NANOARROW_RETURN_NOT_OK(ReadEmptyOrCoordinates(s, v));
    NANOARROW_RETURN_NOT_OK(v->ring_end(v));
    SkipWhitespace(s);

    // Read the rest of the rings
    while (PeekChar(s) != ')') {
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(v->ring_start(v));
      NANOARROW_RETURN_NOT_OK(ReadEmptyOrCoordinates(s, v));
      NANOARROW_RETURN_NOT_OK(v->ring_end(v));
      SkipWhitespace(s);
    }

    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadMultipoint(struct WKTReaderPrivate* s, struct GeoArrowVisitor* v,
                                 enum GeoArrowDimensions dimensions) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);
    SkipWhitespace(s);

    // Both MULTIPOINT (1 2, 2 3) and MULTIPOINT ((1 2), (2 3)) have to parse here
    // if it doesn't look like the verbose version, try the flat version
    if (PeekChar(s) != '(' && PeekChar(s) != 'E') {
      s->data--;
      s->size_bytes++;
      return ReadMultipointFlat(s, v, dimensions);
    }

    // Read the first geometry (there must always be one)
    NANOARROW_RETURN_NOT_OK(v->geom_start(v, GEOARROW_GEOMETRY_TYPE_POINT, dimensions));
    NANOARROW_RETURN_NOT_OK(ReadEmptyOrPointCoordinate(s, v));
    NANOARROW_RETURN_NOT_OK(v->geom_end(v));
    SkipWhitespace(s);

    // Read the rest of the geometries
    while (PeekChar(s) != ')') {
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(v->geom_start(v, GEOARROW_GEOMETRY_TYPE_POINT, dimensions));
      NANOARROW_RETURN_NOT_OK(ReadEmptyOrPointCoordinate(s, v));
      NANOARROW_RETURN_NOT_OK(v->geom_end(v));
      SkipWhitespace(s);
    }

    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadMultilinestring(struct WKTReaderPrivate* s,
                                      struct GeoArrowVisitor* v,
                                      enum GeoArrowDimensions dimensions) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);
    SkipWhitespace(s);

    // Read the first geometry (there must always be one)
    NANOARROW_RETURN_NOT_OK(
        v->geom_start(v, GEOARROW_GEOMETRY_TYPE_LINESTRING, dimensions));
    NANOARROW_RETURN_NOT_OK(ReadEmptyOrCoordinates(s, v));
    NANOARROW_RETURN_NOT_OK(v->geom_end(v));
    SkipWhitespace(s);

    // Read the rest of the geometries
    while (PeekChar(s) != ')') {
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(
          v->geom_start(v, GEOARROW_GEOMETRY_TYPE_LINESTRING, dimensions));
      NANOARROW_RETURN_NOT_OK(ReadEmptyOrCoordinates(s, v));
      NANOARROW_RETURN_NOT_OK(v->geom_end(v));
      SkipWhitespace(s);
    }

    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadMultipolygon(struct WKTReaderPrivate* s, struct GeoArrowVisitor* v,
                                   enum GeoArrowDimensions dimensions) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);
    SkipWhitespace(s);

    // Read the first geometry (there must always be one)
    NANOARROW_RETURN_NOT_OK(v->geom_start(v, GEOARROW_GEOMETRY_TYPE_POLYGON, dimensions));
    NANOARROW_RETURN_NOT_OK(ReadPolygon(s, v));
    NANOARROW_RETURN_NOT_OK(v->geom_end(v));
    SkipWhitespace(s);

    // Read the rest of the geometries
    while (PeekChar(s) != ')') {
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(
          v->geom_start(v, GEOARROW_GEOMETRY_TYPE_POLYGON, dimensions));
      NANOARROW_RETURN_NOT_OK(ReadPolygon(s, v));
      NANOARROW_RETURN_NOT_OK(v->geom_end(v));
      SkipWhitespace(s);
    }

    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadTaggedGeometry(struct WKTReaderPrivate* s,
                                     struct GeoArrowVisitor* v);

static inline int ReadGeometryCollection(struct WKTReaderPrivate* s,
                                         struct GeoArrowVisitor* v) {
  SkipWhitespace(s);
  if (PeekChar(s) == '(') {
    AdvanceUnsafe(s, 1);
    SkipWhitespace(s);

    // Read the first geometry (there must always be one)
    NANOARROW_RETURN_NOT_OK(ReadTaggedGeometry(s, v));
    SkipWhitespace(s);

    // Read the rest of the geometries
    while (PeekChar(s) != ')') {
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(AssertChar(s, ',', v->error));
      SkipWhitespace(s);
      NANOARROW_RETURN_NOT_OK(ReadTaggedGeometry(s, v));
      SkipWhitespace(s);
    }

    AdvanceUnsafe(s, 1);
    return GEOARROW_OK;
  }

  return AssertWordEmpty(s, v->error);
}

static inline int ReadTaggedGeometry(struct WKTReaderPrivate* s,
                                     struct GeoArrowVisitor* v) {
  SkipWhitespace(s);

  struct ArrowStringView word = PeekUntilSep(s, 19);
  enum GeoArrowGeometryType geometry_type;
  if (word.size_bytes == 5 && strncmp(word.data, "POINT", 5) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_POINT;
  } else if (word.size_bytes == 10 && strncmp(word.data, "LINESTRING", 10) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_LINESTRING;
  } else if (word.size_bytes == 7 && strncmp(word.data, "POLYGON", 7) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_POLYGON;
  } else if (word.size_bytes == 10 && strncmp(word.data, "MULTIPOINT", 10) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_MULTIPOINT;
  } else if (word.size_bytes == 15 && strncmp(word.data, "MULTILINESTRING", 15) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_MULTILINESTRING;
  } else if (word.size_bytes == 12 && strncmp(word.data, "MULTIPOLYGON", 12) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_MULTIPOLYGON;
  } else if (word.size_bytes == 18 && strncmp(word.data, "GEOMETRYCOLLECTION", 18) == 0) {
    geometry_type = GEOARROW_GEOMETRY_TYPE_GEOMETRYCOLLECTION;
  } else {
    SetParseErrorAuto("geometry type", s, v->error);
    return EINVAL;
  }

  AdvanceUnsafe(s, word.size_bytes);
  SkipWhitespace(s);

  enum GeoArrowDimensions dimensions = GEOARROW_DIMENSIONS_XY;
  s->coord_view.n_values = 2;
  word = PeekUntilSep(s, 3);
  if (word.size_bytes == 1 && strncmp(word.data, "Z", 1) == 0) {
    dimensions = GEOARROW_DIMENSIONS_XYZ;
    s->coord_view.n_values = 3;
    AdvanceUnsafe(s, 1);
  } else if (word.size_bytes == 1 && strncmp(word.data, "M", 1) == 0) {
    dimensions = GEOARROW_DIMENSIONS_XYM;
    s->coord_view.n_values = 3;
    AdvanceUnsafe(s, 1);
  } else if (word.size_bytes == 2 && strncmp(word.data, "ZM", 2) == 0) {
    dimensions = GEOARROW_DIMENSIONS_XYZM;
    s->coord_view.n_values = 4;
    AdvanceUnsafe(s, 2);
  }

  NANOARROW_RETURN_NOT_OK(v->geom_start(v, geometry_type, dimensions));

  switch (geometry_type) {
    case GEOARROW_GEOMETRY_TYPE_POINT:
      NANOARROW_RETURN_NOT_OK(ReadEmptyOrPointCoordinate(s, v));
      break;
    case GEOARROW_GEOMETRY_TYPE_LINESTRING:
      NANOARROW_RETURN_NOT_OK(ReadEmptyOrCoordinates(s, v));
      break;
    case GEOARROW_GEOMETRY_TYPE_POLYGON:
      NANOARROW_RETURN_NOT_OK(ReadPolygon(s, v));
      break;
    case GEOARROW_GEOMETRY_TYPE_MULTIPOINT:
      NANOARROW_RETURN_NOT_OK(ReadMultipoint(s, v, dimensions));
      break;
    case GEOARROW_GEOMETRY_TYPE_MULTILINESTRING:
      NANOARROW_RETURN_NOT_OK(ReadMultilinestring(s, v, dimensions));
      break;
    case GEOARROW_GEOMETRY_TYPE_MULTIPOLYGON:
      NANOARROW_RETURN_NOT_OK(ReadMultipolygon(s, v, dimensions));
      break;
    case GEOARROW_GEOMETRY_TYPE_GEOMETRYCOLLECTION:
      NANOARROW_RETURN_NOT_OK(ReadGeometryCollection(s, v));
      break;
    default:
      GeoArrowErrorSet(v->error, "Internal error: unrecognized geometry type id");
      return EINVAL;
  }

  return v->geom_end(v);
}

GeoArrowErrorCode GeoArrowWKTReaderInit(struct GeoArrowWKTReader* reader) {
  struct WKTReaderPrivate* s =
      (struct WKTReaderPrivate*)ArrowMalloc(sizeof(struct WKTReaderPrivate));

  if (s == NULL) {
    return ENOMEM;
  }

  s->data0 = NULL;
  s->data = NULL;
  s->size_bytes = 0;

  s->coord_view.coords_stride = 1;
  s->coord_view.values[0] = s->coords;
  for (int i = 1; i < 4; i++) {
    s->coord_view.values[i] = s->coord_view.values[i - 1] + COORD_CACHE_SIZE_COORDS;
  }

  reader->private_data = s;
  return GEOARROW_OK;
}

void GeoArrowWKTReaderReset(struct GeoArrowWKTReader* reader) {
  ArrowFree(reader->private_data);
}

GeoArrowErrorCode GeoArrowWKTReaderVisit(struct GeoArrowWKTReader* reader,
                                         struct GeoArrowStringView src,
                                         struct GeoArrowVisitor* v) {
  struct WKTReaderPrivate* s = (struct WKTReaderPrivate*)reader->private_data;
  s->data0 = src.data;
  s->data = src.data;
  s->size_bytes = src.size_bytes;

  NANOARROW_RETURN_NOT_OK(v->feat_start(v));
  NANOARROW_RETURN_NOT_OK(ReadTaggedGeometry(s, v));
  NANOARROW_RETURN_NOT_OK(v->feat_end(v));
  SkipWhitespace(s);
  if (PeekChar(s) != '\0') {
    SetParseErrorAuto("end of input", s, v->error);
    return EINVAL;
  }

  return GEOARROW_OK;
}
