
#pragma once

#include <cstdint>
#include <cstdarg>
#include <stdexcept>

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

namespace geoarrow {

namespace util {

enum Extension {
    Geometry,
    Point,
    Linestring,
    Polygon,
    MultiPoint,
    MultiLinestring,
    MultiPolygon,
    GeometryCollection,
    WKB,
    WKT,
    ExtensionOther,
    ExtensionNone
};

enum StorageType {
    Float32,
    Float64,
    String,
    LargeString,
    FixedWidthBinary,
    Binary,
    LargeBinary,
    FixedSizeList,
    Struct,
    List,
    LargeList,
    StorageTypeOther,
    StorageTypeNone
};

enum GeometryType {
    GEOMETRY_TYPE_UNKNOWN = 0,
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
    MULTIPOINT = 4,
    MULTILINESTRING = 5,
    MULTIPOLYGON = 6,
    GEOMETRYCOLLECTION = 7
};

enum Dimensions {DIMENSIONS_UNKNOWN = 0, XY = 1, XYZ = 2, XYM = 3, XYZM = 4};

enum Edges {Planar, Spherical, Ellipsoidal, EdgesUnknown};

class IOException: public std::exception {
public:
  IOException(const char* fmt, ...) {
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
