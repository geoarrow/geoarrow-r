
#pragma once

#include "internal/geoarrow-cpp/meta.hpp"
#include "internal/geoarrow-cpp/handler.hpp"
#include "internal/geoarrow-cpp/array-view-base.hpp"

namespace geoarrow {

ArrayView* create_view(struct ArrowSchema* schema);

}
