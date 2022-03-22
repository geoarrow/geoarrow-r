
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

}
