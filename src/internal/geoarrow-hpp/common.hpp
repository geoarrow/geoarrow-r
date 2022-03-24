
#pragma once

#include "../arrow-hpp/common.hpp"

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
    Null,
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

enum Dimensions {DIMENSIONS_UNKNOWN = 10000, XY = 0, XYZ = 1000, XYM = 2000, XYZM = 3000};

enum Edges {Planar, Spherical, Ellipsoidal, EdgesUnknown};

class IOException: public arrow::hpp::util::Exception {
public:
  IOException(const char* fmt, ...): arrow::hpp::util::Exception() {
    va_list args;
    va_start(args, fmt);
    vsnprintf(error_, sizeof(error_) - 1, fmt, args);
    va_end(args);
  }

  const char* what() const noexcept {
    return error_;
  }
};

}

}
