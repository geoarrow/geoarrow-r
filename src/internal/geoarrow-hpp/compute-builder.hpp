
#pragma once

#include <cstdlib>
#include <limits>
#include <cstring>
#include <algorithm>

#include "handler.hpp"
#include "common.hpp"
#include "../arrow-hpp/builder.hpp"

namespace geoarrow {

class ComputeBuilder: public arrow::hpp::builder::ArrayBuilder, public Handler {
public:
  ComputeBuilder(int64_t capacity = 1024): ArrayBuilder(capacity) {}
};

class NullBuilder: public ComputeBuilder {
public:
  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    arrow::hpp::builder::CArrayFinalizer finalizer;
    finalizer.allocate(0);
    finalizer.set_schema_format("n");
    finalizer.array_data.null_count = 0;
    finalizer.release(array_data, schema);
  }
};

}
