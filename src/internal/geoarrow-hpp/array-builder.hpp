
#pragma once

#include <cstdlib>
#include <limits>
#include <cstring>
#include <algorithm>

#include "handler.hpp"
#include "common.hpp"
#include "../arrow-hpp/builder.hpp"

namespace geoarrow {

class GeoArrayBuilder: public arrow::hpp::builder::ArrayBuilder, public Handler {
public:
  GeoArrayBuilder(int64_t capacity = 1024): ArrayBuilder(capacity) {}
};

class NullBuilder: public GeoArrayBuilder {
public:
  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    arrow::hpp::builder::CArrayFinalizer finalizer;
    finalizer.allocate(0);
    finalizer.schema.format = "n";
    finalizer.release(array_data, schema);
  }
};

}
